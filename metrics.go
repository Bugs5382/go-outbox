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

import "time"

// Metrics receives relay events.
type Metrics interface {
	Claimed(n int)
	Published(topic string, attempt int, took time.Duration)
	Failed(topic string, attempt int, err error)
	DeadLettered(topic string, attempts int)
	Released(n int)
}

// NopMetrics discards every event.
type NopMetrics struct{}

func (NopMetrics) Claimed(int)                          {}
func (NopMetrics) Published(string, int, time.Duration) {}
func (NopMetrics) Failed(string, int, error)            {}
func (NopMetrics) DeadLettered(string, int)             {}
func (NopMetrics) Released(int)                         {}
