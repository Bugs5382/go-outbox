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

import "errors"

var (
	// ErrInvalidTable is returned by New for a table name that is not a
	// plain, optionally schema-qualified, lowercase identifier.
	ErrInvalidTable = errors.New("outbox: invalid table name")
	// ErrInvalidMessage is returned by Enqueue and Dedupe for input that is
	// missing a required field.
	ErrInvalidMessage = errors.New("outbox: invalid message")
	// ErrDuplicateKey is returned by Enqueue when the idempotency key is
	// already in the outbox. The caller's transaction is still usable.
	ErrDuplicateKey = errors.New("outbox: duplicate idempotency key")
)

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent marks err as not worth retrying. A Publisher returns it for a
// message the broker will never accept (a malformed payload, a rejected
// destination), and the relay dead-letters the row at once instead of
// spending its remaining attempts. Permanent(nil) is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// IsPermanent reports whether err, or any error it wraps, was marked with
// Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}
