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
	"testing"
	"time"
)

func TestBackoffGrowsAndCaps(t *testing.T) {
	minDelay, maxDelay := 100*time.Millisecond, time.Second
	for attempt := 1; attempt <= 10; attempt++ {
		ceiling := minDelay << (attempt - 1)
		if ceiling > maxDelay || ceiling <= 0 {
			ceiling = maxDelay
		}
		for i := 0; i < 50; i++ {
			d := backoff(attempt, minDelay, maxDelay)
			if d < ceiling/2 || d > ceiling {
				t.Fatalf("backoff(%d) = %s, want within [%s, %s]", attempt, d, ceiling/2, ceiling)
			}
		}
	}
}

func TestBackoffSurvivesHugeAttempts(t *testing.T) {
	if d := backoff(1000, time.Second, time.Minute); d < 30*time.Second || d > time.Minute {
		t.Errorf("backoff(1000) = %s, want capped near a minute", d)
	}
}
