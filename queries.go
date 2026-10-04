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

import "strings"

type queries struct {
	enqueue     string
	claim       string
	markSent    string
	retry       string
	dead        string
	release     string
	dedupe      string
	stats       string
	redriveAll  string
	redriveKeys string
	notify      string
	purge       string
}

func buildQueries(t tableNames) queries {
	r := strings.NewReplacer("{T}", t.table, "{I}", t.inbox)
	return queries{
		// pg_notify is transactional: idle relays hear about the row only
		// once the caller's transaction commits.
		enqueue: r.Replace(`
WITH ins AS (
    INSERT INTO {T} (idempotency_key, topic, aggregate_key, payload, content_type, headers, available_at)
    VALUES (COALESCE(NULLIF($1, ''), gen_random_uuid()::text), $2, NULLIF($3, ''), $4, $5, $6,
            now() + $7 * interval '1 millisecond')
    ON CONFLICT (idempotency_key) DO NOTHING
    RETURNING idempotency_key
)
SELECT ins.idempotency_key FROM ins, pg_notify($8, '') AS n`),

		// A row is due when it is pending, past its backoff and not under a
		// live lease. A row with an aggregate key is due only while no older
		// row of the same aggregate is still pending, which keeps each
		// aggregate in enqueue order. Claiming bumps attempts, and that value
		// fences every later update to the row.
		claim: r.Replace(`
WITH due AS (
    SELECT o.id FROM {T} o
    WHERE o.status = 'pending'
      AND o.available_at <= now()
      AND (o.lease_until IS NULL OR o.lease_until <= now())
      AND (o.aggregate_key IS NULL OR NOT EXISTS (
            SELECT 1 FROM {T} p
            WHERE p.aggregate_key = o.aggregate_key AND p.status = 'pending' AND p.id < o.id))
    ORDER BY o.id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE {T} t
SET lease_until = now() + $2 * interval '1 millisecond', attempts = t.attempts + 1
FROM due
WHERE t.id = due.id
RETURNING t.id, t.idempotency_key, t.topic, COALESCE(t.aggregate_key, ''), t.payload,
          t.content_type, t.headers, t.attempts, t.created_at`),

		markSent: r.Replace(`
UPDATE {T} SET status = 'sent', sent_at = now(), lease_until = NULL, last_error = NULL
WHERE id = $1 AND status = 'pending'`),

		retry: r.Replace(`
UPDATE {T} SET available_at = now() + $3 * interval '1 millisecond', lease_until = NULL, last_error = $4
WHERE id = $1 AND attempts = $2 AND status = 'pending'`),

		dead: r.Replace(`
UPDATE {T} SET status = 'dead', dead_at = now(), lease_until = NULL, last_error = $3
WHERE id = $1 AND attempts = $2 AND status = 'pending'`),

		release: r.Replace(`
UPDATE {T} t SET lease_until = NULL, attempts = t.attempts - 1
FROM unnest($1::bigint[], $2::integer[]) AS c(id, attempts)
WHERE t.id = c.id AND t.attempts = c.attempts AND t.status = 'pending'`),

		dedupe: r.Replace(`
INSERT INTO {I} (consumer, idempotency_key) VALUES ($1, $2)
ON CONFLICT DO NOTHING`),

		stats: r.Replace(`
SELECT
    count(*) FILTER (WHERE status = 'pending' AND (lease_until IS NULL OR lease_until <= now())),
    count(*) FILTER (WHERE status = 'pending' AND lease_until > now()),
    count(*) FILTER (WHERE status = 'sent'),
    count(*) FILTER (WHERE status = 'dead'),
    COALESCE(EXTRACT(EPOCH FROM now() - min(created_at) FILTER (WHERE status = 'pending')), 0)::float8
FROM {T}`),

		redriveAll: r.Replace(`
UPDATE {T} SET status = 'pending', attempts = 0, available_at = now(), dead_at = NULL, lease_until = NULL
WHERE status = 'dead'`),

		redriveKeys: r.Replace(`
UPDATE {T} SET status = 'pending', attempts = 0, available_at = now(), dead_at = NULL, lease_until = NULL
WHERE status = 'dead' AND idempotency_key = ANY($1)`),

		notify: `SELECT pg_notify($1, '')`,

		purge: r.Replace(`
DELETE FROM {T} WHERE status = 'sent' AND sent_at < now() - $1 * interval '1 millisecond'`),
	}
}
