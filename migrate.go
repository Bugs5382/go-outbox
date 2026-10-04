package outbox

/*
MIT License

Copyright (c) 2026 Shane

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"text/template"

	log "github.com/Bugs5382/go-log"
	postgres "github.com/Bugs5382/go-postgres"
	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type migration struct {
	version int
	name    string
	sql     string
}

// Migrate creates the outbox and inbox tables, or brings them up to date. It
// is safe to run on every start and from several processes at once: the
// steps run in one transaction under an advisory lock, and each applied
// version is recorded in the <table>_migrations table, so a step never runs
// twice. A schema-qualified table needs its schema to exist already.
//
// To run the schema through a migration tool of your own instead, take the
// statements from SQL.
func (o *Outbox) Migrate(ctx context.Context, db *postgres.DB) error {
	steps, err := o.migrations()
	if err != nil {
		return err
	}
	applied := 0
	err = db.RunInTx(ctx, func(tx pgx.Tx) error {
		applied = 0
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "go-outbox:"+o.t.full); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+o.t.migrations+
			` (version integer PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
			return err
		}
		var current int
		if err := tx.QueryRow(ctx, `SELECT COALESCE(max(version), 0) FROM `+o.t.migrations).Scan(&current); err != nil {
			return err
		}
		for _, m := range steps {
			if m.version <= current {
				continue
			}
			if _, err := tx.Exec(ctx, m.sql); err != nil {
				return fmt.Errorf("migration %d %s: %w", m.version, m.name, err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO `+o.t.migrations+` (version, name) VALUES ($1, $2)`, m.version, m.name); err != nil {
				return err
			}
			applied++
		}
		return nil
	})
	if err != nil {
		o.logger.Error(err, "migrate failed")
		return fmt.Errorf("outbox: migrate: %w", err)
	}
	o.logger.Info("migrated", log.F("applied", applied), log.F("latest", steps[len(steps)-1].version))
	return nil
}

// SQL returns the schema statements Migrate runs, rendered for this outbox's
// table names, one string per migration in version order. Use it to feed the
// schema to your own migration tool instead of calling Migrate.
func (o *Outbox) SQL() ([]string, error) {
	steps, err := o.migrations()
	if err != nil {
		return nil, err
	}
	out := make([]string, len(steps))
	for i, m := range steps {
		out[i] = m.sql
	}
	return out, nil
}

func (o *Outbox) migrations() ([]migration, error) {
	names, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		return nil, err
	}
	data := struct{ Table, Inbox, Base string }{o.t.table, o.t.inbox, o.t.base}
	var steps []migration
	for _, path := range names {
		file := strings.TrimPrefix(path, "migrations/")
		version, err := strconv.Atoi(strings.SplitN(file, "_", 2)[0])
		if err != nil {
			return nil, fmt.Errorf("outbox: migration %s has no version prefix", file)
		}
		raw, err := migrationFiles.ReadFile(path)
		if err != nil {
			return nil, err
		}
		tmpl, err := template.New(file).Parse(string(raw))
		if err != nil {
			return nil, err
		}
		var b strings.Builder
		if err := tmpl.Execute(&b, data); err != nil {
			return nil, err
		}
		steps = append(steps, migration{version: version, name: strings.TrimSuffix(file, ".sql"), sql: b.String()})
	}
	sort.Slice(steps, func(i, j int) bool { return steps[i].version < steps[j].version })
	return steps, nil
}
