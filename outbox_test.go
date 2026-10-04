package outbox_test

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
	"errors"
	"strings"
	"testing"

	outbox "github.com/Bugs5382/go-outbox"
)

func TestNewDefaultsTable(t *testing.T) {
	ob, err := outbox.New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if ob.Table() != outbox.DefaultTable {
		t.Errorf("Table() = %q, want %q", ob.Table(), outbox.DefaultTable)
	}
}

func TestNewRejectsInvalidTableNames(t *testing.T) {
	for _, name := range []string{
		"",
		"Events",
		"9events",
		"events; drop table users",
		`ev"ents`,
		"a.b.c",
		".events",
		strings.Repeat("e", 51),
	} {
		if _, err := outbox.New(outbox.WithTable(name)); !errors.Is(err, outbox.ErrInvalidTable) {
			t.Errorf("New(WithTable(%q)) = %v, want ErrInvalidTable", name, err)
		}
	}
}

func TestNewAcceptsSchemaQualifiedTable(t *testing.T) {
	ob, err := outbox.New(outbox.WithTable("audit.events_v2"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if ob.Table() != "audit.events_v2" {
		t.Errorf("Table() = %q", ob.Table())
	}
}

func TestPermanentWrapsAndIsDetected(t *testing.T) {
	cause := errors.New("rejected")
	err := outbox.Permanent(cause)
	if !outbox.IsPermanent(err) {
		t.Error("IsPermanent(Permanent(err)) = false")
	}
	if !errors.Is(err, cause) {
		t.Error("Permanent must keep the cause for errors.Is")
	}
	if outbox.IsPermanent(cause) {
		t.Error("IsPermanent(plain error) = true")
	}
	if outbox.Permanent(nil) != nil {
		t.Error("Permanent(nil) must be nil")
	}
}
