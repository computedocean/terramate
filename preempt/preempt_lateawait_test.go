// Copyright 2026 Terramate GmbH
// SPDX-License-Identifier: MPL-2.0

package preempt_test

import (
	"context"
	"testing"
	"time"

	"github.com/terramate-io/terramate/errors"
	"github.com/terramate-io/terramate/preempt"
)

// TestRunSingleAwaitErrUnresolvable verifies the baseline behaviour.
func TestRunSingleAwaitErrUnresolvable(t *testing.T) {
	t.Parallel()
	done := make(chan error, 1)
	go func() {
		done <- preempt.Run(context.Background(), func(yield func(preempt.Preemptable) bool) {
			yield(func(ctx context.Context) ([]string, error) {
				return nil, preempt.Await(ctx, "missing-only")
			})
		})
	}()

	select {
	case err := <-done:
		if err == nil || !errors.IsKind(err, preempt.ErrUnresolvable) {
			t.Fatalf("expected ErrUnresolvable, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("preempt.Run deadlocked on a single Await")
	}
}

// TestRunHandlesLateAwaitAfterUnresolvable verifies that a goroutine which
// calls Await, receives ErrUnresolvable, and then calls Await does not deadlock the cleanup loop.
func TestRunHandlesLateAwaitAfterUnresolvable(t *testing.T) {
	t.Parallel()
	done := make(chan error, 1)
	go func() {
		done <- preempt.Run(context.Background(), func(yield func(preempt.Preemptable) bool) {
			yield(func(ctx context.Context) ([]string, error) {
				// First await — unblocked with ErrUnresolvable when the
				// iterator exhausts.
				_ = preempt.Await(ctx, "missing-1")
				// Late await — must not deadlock.
				_ = preempt.Await(ctx, "missing-2")
				return nil, nil
			})
		})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("preempt.Run deadlocked on a late Await after ErrUnresolvable")
	}
}

// TestRunMultipleLateAwaits stresses the fix with many late Awaits in a row.
func TestRunMultipleLateAwaits(t *testing.T) {
	t.Parallel()
	done := make(chan error, 1)
	go func() {
		done <- preempt.Run(context.Background(), func(yield func(preempt.Preemptable) bool) {
			yield(func(ctx context.Context) ([]string, error) {
				for range 5 {
					_ = preempt.Await(ctx, "missing")
				}
				return nil, nil
			})
		})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("preempt.Run deadlocked on repeated late Awaits")
	}
}
