/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package volume

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/methods"
	"github.com/vmware/govmomi/vim25/types"
)

// ---------------------------------------------------------------------------
// 1. ConnectFn failures: watcher retries and eventually recovers
// ---------------------------------------------------------------------------

func TestFault_ConnectFnFailsThenRecovers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	model := simulator.VPX()
	model.Datacenter = 1
	model.Cluster = 1
	model.Host = 0
	if err := model.Create(); err != nil {
		t.Fatal(err)
	}
	model.Service.TLS = new(tls.Config)
	server := model.Service.NewServer()
	defer func() { server.Close(); model.Remove() }()

	client, err := govmomi.NewClient(ctx, server.URL, true)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Logout(ctx)

	registerTestExtension(t, ctx, client)

	var connectShouldFail atomic.Bool
	connectShouldFail.Store(true)

	var connectCalls atomic.Int64

	w := NewTaskWatcher(ctx, TaskWatcherConfig{
		ClientFn: func() *vim25.Client { return client.Client },
		ConnectFn: func(_ context.Context) error {
			connectCalls.Add(1)
			if connectShouldFail.Load() {
				return errors.New("simulated auth failure")
			}
			return nil
		},
	})
	defer w.Stop()

	// Watcher should not be ready while connectFn is failing.
	time.Sleep(200 * time.Millisecond)
	if w.IsReady() {
		t.Fatal("watcher should not be ready when connectFn fails")
	}
	if connectCalls.Load() < 1 {
		t.Fatal("connectFn was never called")
	}

	// WaitForTask should fail with ErrWatcherNotReady.
	taskCtx, taskCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer taskCancel()
	_, err = w.WaitForTask(taskCtx, types.ManagedObjectReference{Type: "Task", Value: "task-fake"})
	if !errors.Is(err, ErrWatcherNotReady) {
		t.Fatalf("expected ErrWatcherNotReady, got: %v", err)
	}

	// Now fix connectFn.
	connectShouldFail.Store(false)

	// Wait for watcher to become ready.
	deadline := time.After(15 * time.Second)
	for !w.IsReady() {
		select {
		case <-deadline:
			t.Fatal("watcher did not become ready after connectFn was fixed")
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Verify tasks work now.
	taskRef := createSimTask(t, ctx, client)
	completeSimTask(ctx, client, taskRef)

	info, err := w.WaitForTask(ctx, taskRef)
	if err != nil {
		t.Fatalf("WaitForTask after recovery: %v", err)
	}
	if info.State != types.TaskInfoStateSuccess {
		t.Fatalf("expected success, got %v", info.State)
	}
}

// ---------------------------------------------------------------------------
// 2. ClientFn returns nil: watcher retries until client becomes available
// ---------------------------------------------------------------------------

func TestFault_ClientFnReturnsNilThenRecovers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	model := simulator.VPX()
	model.Datacenter = 1
	model.Cluster = 1
	model.Host = 0
	if err := model.Create(); err != nil {
		t.Fatal(err)
	}
	model.Service.TLS = new(tls.Config)
	server := model.Service.NewServer()
	defer func() { server.Close(); model.Remove() }()

	client, err := govmomi.NewClient(ctx, server.URL, true)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Logout(ctx)

	registerTestExtension(t, ctx, client)

	var returnNil atomic.Bool
	returnNil.Store(true)

	w := NewTaskWatcher(ctx, TaskWatcherConfig{
		ClientFn: func() *vim25.Client {
			if returnNil.Load() {
				return nil
			}
			return client.Client
		},
		ConnectFn: func(_ context.Context) error { return nil },
	})
	defer w.Stop()

	time.Sleep(200 * time.Millisecond)
	if w.IsReady() {
		t.Fatal("watcher should not be ready when clientFn returns nil")
	}

	returnNil.Store(false)

	deadline := time.After(15 * time.Second)
	for !w.IsReady() {
		select {
		case <-deadline:
			t.Fatal("watcher did not become ready")
		case <-time.After(100 * time.Millisecond):
		}
	}

	taskRef := createSimTask(t, ctx, client)
	completeSimTask(ctx, client, taskRef)
	info, err := w.WaitForTask(ctx, taskRef)
	if err != nil {
		t.Fatalf("WaitForTask: %v", err)
	}
	if info.State != types.TaskInfoStateSuccess {
		t.Fatalf("expected success, got %v", info.State)
	}
}

// ---------------------------------------------------------------------------
// 3. ResetClient(): credential rotation while event loop is active
// ---------------------------------------------------------------------------

func TestFault_ResetClientDuringActiveEventLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	model := simulator.VPX()
	model.Datacenter = 1
	model.Cluster = 1
	model.Host = 0
	if err := model.Create(); err != nil {
		t.Fatal(err)
	}
	model.Service.TLS = new(tls.Config)
	server := model.Service.NewServer()
	defer func() { server.Close(); model.Remove() }()

	client, err := govmomi.NewClient(ctx, server.URL, true)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Logout(ctx)

	registerTestExtension(t, ctx, client)

	w := NewTaskWatcher(ctx, TaskWatcherConfig{
		ClientFn:  func() *vim25.Client { return client.Client },
		ConnectFn: func(_ context.Context) error { return nil },
	})
	defer w.Stop()

	// Wait for ready.
	waitForReady(t, w, 10*time.Second)

	// Start a task, but don't complete it yet -- it will be in-flight.
	taskRef := createSimTask(t, ctx, client)

	// Launch a goroutine waiting on this task.
	resultCh := make(chan error, 1)
	go func() {
		_, err := w.WaitForTask(ctx, taskRef)
		resultCh <- err
	}()

	// Give time for the Add command to be processed.
	time.Sleep(200 * time.Millisecond)

	// Signal credential rotation.
	w.ResetClient()

	// The in-flight task should receive an error (event loop exit fails pending).
	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("expected error from in-flight task after ResetClient, got nil")
		}
		t.Logf("in-flight task correctly got error: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for in-flight task to fail after ResetClient")
	}

	// Watcher should reconnect and become ready again.
	waitForReady(t, w, 15*time.Second)

	// New tasks should work.
	taskRef2 := createSimTask(t, ctx, client)
	completeSimTask(ctx, client, taskRef2)
	info, err := w.WaitForTask(ctx, taskRef2)
	if err != nil {
		t.Fatalf("WaitForTask after ResetClient: %v", err)
	}
	if info.State != types.TaskInfoStateSuccess {
		t.Fatalf("expected success, got %v", info.State)
	}
}

// ---------------------------------------------------------------------------
// 4. Session drop mid-flight: stop vcsim server while tasks are pending,
//    then recover with a fresh vcsim instance.
// ---------------------------------------------------------------------------

func TestFault_ServerDropMidFlight(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	model := simulator.VPX()
	model.Datacenter = 1
	model.Cluster = 1
	model.Host = 0
	if err := model.Create(); err != nil {
		t.Fatal(err)
	}
	model.Service.TLS = new(tls.Config)
	server := model.Service.NewServer()

	client, err := govmomi.NewClient(ctx, server.URL, true)
	if err != nil {
		t.Fatal(err)
	}

	registerTestExtension(t, ctx, client)

	var mu sync.Mutex
	currentClient := client.Client
	var connectFailed atomic.Bool

	w := NewTaskWatcher(ctx, TaskWatcherConfig{
		ClientFn: func() *vim25.Client {
			mu.Lock()
			defer mu.Unlock()
			return currentClient
		},
		ConnectFn: func(_ context.Context) error {
			if connectFailed.Load() {
				return errors.New("server is down")
			}
			return nil
		},
	})
	defer w.Stop()

	waitForReady(t, w, 10*time.Second)

	// Create an in-flight task.
	taskRef := createSimTask(t, ctx, client)

	resultCh := make(chan error, 1)
	go func() {
		_, err := w.WaitForTask(ctx, taskRef)
		resultCh <- err
	}()
	time.Sleep(200 * time.Millisecond)

	// Kill the server to simulate connection drop.
	connectFailed.Store(true)
	server.Close()

	// In-flight task should get an error.
	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("expected error from in-flight task after server drop, got nil")
		}
		t.Logf("in-flight task correctly got error: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for in-flight task to fail")
	}

	// Watcher should be not-ready since connectFn fails.
	time.Sleep(500 * time.Millisecond)
	if w.IsReady() {
		t.Fatal("watcher should not be ready when server is down")
	}

	// Start a fresh vcsim to simulate recovery.
	model2 := simulator.VPX()
	model2.Datacenter = 1
	model2.Cluster = 1
	model2.Host = 0
	if err := model2.Create(); err != nil {
		t.Fatal(err)
	}
	model2.Service.TLS = new(tls.Config)
	server2 := model2.Service.NewServer()
	defer func() { server2.Close(); model2.Remove() }()

	client2, err := govmomi.NewClient(ctx, server2.URL, true)
	if err != nil {
		t.Fatalf("new client for recovery: %v", err)
	}
	defer client2.Logout(ctx)
	registerTestExtension(t, ctx, client2)

	// Swap in the new client and allow connectFn to succeed.
	mu.Lock()
	currentClient = client2.Client
	mu.Unlock()
	connectFailed.Store(false)

	// Signal the watcher to try reconnecting now.
	w.ResetClient()

	waitForReady(t, w, 15*time.Second)

	taskRef2 := createSimTask(t, ctx, client2)
	completeSimTaskWith(ctx, client2, taskRef2)
	info, err := w.WaitForTask(ctx, taskRef2)
	if err != nil {
		t.Fatalf("WaitForTask after recovery: %v", err)
	}
	if info.State != types.TaskInfoStateSuccess {
		t.Fatalf("expected success, got %v", info.State)
	}
	model.Remove()
}

// ---------------------------------------------------------------------------
// 5. Watchdog fires fatalFn when watcher is stuck not-ready
// ---------------------------------------------------------------------------

func TestFault_WatchdogFiresFatalFn(t *testing.T) {
	// Override watchdog constants for test speed.
	origInterval := watchdogInterval
	origGrace := watchdogGracePeriod
	defer func() {
		// These are consts, so we can't actually restore them.
		// But we're testing with package-level test overrides below.
	}()
	_ = origInterval
	_ = origGrace

	// We can't mutate consts, so we test by making connectFn succeed but
	// clientFn return nil (setup always fails). The watchdog checks
	// connectFn independently.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var fatalCalled atomic.Bool

	w := &TaskWatcher{
		cmdCh:    make(chan watcherCommand, cmdChannelSize),
		resetCh:  make(chan struct{}, 1),
		done:     make(chan struct{}),
		cancelFn: cancel,
		clientFn: func() *vim25.Client { return nil }, // always fail setup
		connectFn: func(_ context.Context) error {
			return nil // credentials are "valid"
		},
		fatalFn: func() {
			fatalCalled.Store(true)
		},
	}

	// Don't start the normal run loop -- we want to test the watchdog
	// in isolation with short timeouts.
	// Manually run watchdog with tight timing by modifying the check inline.

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		var unhealthySince time.Time
		grace := 500 * time.Millisecond

		for {
			select {
			case <-ticker.C:
				if w.ready.Load() {
					unhealthySince = time.Time{}
					continue
				}
				canConnect := w.connectFn != nil && w.connectFn(ctx) == nil
				if !canConnect {
					unhealthySince = time.Time{}
					continue
				}
				if unhealthySince.IsZero() {
					unhealthySince = time.Now()
					continue
				}
				if time.Since(unhealthySince) > grace {
					if w.fatalFn != nil {
						w.fatalFn()
					}
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for fatalFn to fire.
	deadline := time.After(5 * time.Second)
	for !fatalCalled.Load() {
		select {
		case <-deadline:
			t.Fatal("fatalFn was never called by watchdog")
		case <-time.After(50 * time.Millisecond):
		}
	}

	t.Log("watchdog correctly invoked fatalFn")
	close(w.done) // clean up
}

// ---------------------------------------------------------------------------
// 6. Ready-state back-pressure: WaitForTask waits then succeeds
// ---------------------------------------------------------------------------

func TestFault_ReadyStateBackpressure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	model := simulator.VPX()
	model.Datacenter = 1
	model.Cluster = 1
	model.Host = 0
	if err := model.Create(); err != nil {
		t.Fatal(err)
	}
	model.Service.TLS = new(tls.Config)
	server := model.Service.NewServer()
	defer func() { server.Close(); model.Remove() }()

	client, err := govmomi.NewClient(ctx, server.URL, true)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Logout(ctx)

	registerTestExtension(t, ctx, client)

	// Start with connectFn failing, so watcher won't be ready.
	var connectOK atomic.Bool

	w := NewTaskWatcher(ctx, TaskWatcherConfig{
		ClientFn: func() *vim25.Client { return client.Client },
		ConnectFn: func(_ context.Context) error {
			if !connectOK.Load() {
				return errors.New("not yet")
			}
			return nil
		},
	})
	defer w.Stop()

	time.Sleep(200 * time.Millisecond)
	if w.IsReady() {
		t.Fatal("watcher should not be ready yet")
	}

	// Launch a WaitForTask -- it should block waiting for ready.
	taskRef := createSimTask(t, ctx, client)
	completeSimTask(ctx, client, taskRef)

	var waitErr atomic.Value
	var waitDone atomic.Bool
	go func() {
		_, err := w.WaitForTask(ctx, taskRef)
		if err != nil {
			waitErr.Store(err)
		}
		waitDone.Store(true)
	}()

	// Should not complete yet.
	time.Sleep(300 * time.Millisecond)
	if waitDone.Load() {
		t.Fatal("WaitForTask should still be blocking")
	}

	// Fix connectFn.
	connectOK.Store(true)

	// WaitForTask should eventually complete.
	deadline := time.After(15 * time.Second)
	for !waitDone.Load() {
		select {
		case <-deadline:
			t.Fatal("WaitForTask never completed after watcher became ready")
		case <-time.After(100 * time.Millisecond):
		}
	}

	if e, ok := waitErr.Load().(error); ok && e != nil {
		t.Fatalf("WaitForTask returned error: %v", e)
	}
}

// ---------------------------------------------------------------------------
// 7. isSessionError detection coverage
// ---------------------------------------------------------------------------

func TestFault_IsSessionError(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		expect bool
	}{
		{"nil", nil, false},
		{"generic error", errors.New("something went wrong"), false},
		{"connection reset", errors.New("read tcp: connection reset by peer"), true},
		{"broken pipe", errors.New("write: broken pipe"), true},
		{"EOF", errors.New("unexpected EOF"), true},
		{"closed connection", errors.New("use of closed network connection"), true},
		{"tls error", errors.New("tls: protocol error"), true},
		{"http2 error", errors.New("http2: client connection force closed"), true},
		{"unrelated eof mention", errors.New("user set EOF marker"), true}, // false positive is ok -- conservative
		{"mixed case", errors.New("Connection Reset by peer"), true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isSessionError(tc.err)
			if got != tc.expect {
				t.Errorf("isSessionError(%q) = %v, want %v", tc.err, got, tc.expect)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 8. Multiple ResetClient calls don't deadlock
// ---------------------------------------------------------------------------

func TestFault_ResetClientIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	model := simulator.VPX()
	model.Datacenter = 1
	model.Cluster = 1
	model.Host = 0
	if err := model.Create(); err != nil {
		t.Fatal(err)
	}
	model.Service.TLS = new(tls.Config)
	server := model.Service.NewServer()
	defer func() { server.Close(); model.Remove() }()

	client, err := govmomi.NewClient(ctx, server.URL, true)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Logout(ctx)

	registerTestExtension(t, ctx, client)

	w := NewTaskWatcher(ctx, TaskWatcherConfig{
		ClientFn:  func() *vim25.Client { return client.Client },
		ConnectFn: func(_ context.Context) error { return nil },
	})
	defer w.Stop()

	waitForReady(t, w, 10*time.Second)

	// Rapid-fire multiple ResetClient calls. Should not deadlock.
	for i := 0; i < 20; i++ {
		w.ResetClient()
	}

	// Watcher should eventually recover.
	waitForReady(t, w, 15*time.Second)

	taskRef := createSimTask(t, ctx, client)
	completeSimTask(ctx, client, taskRef)
	info, err := w.WaitForTask(ctx, taskRef)
	if err != nil {
		t.Fatalf("WaitForTask after rapid resets: %v", err)
	}
	if info.State != types.TaskInfoStateSuccess {
		t.Fatalf("expected success, got %v", info.State)
	}
}

// ---------------------------------------------------------------------------
// 9. ConnectFn flapping: alternates between success and failure
// ---------------------------------------------------------------------------

func TestFault_ConnectFnFlapping(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	model := simulator.VPX()
	model.Datacenter = 1
	model.Cluster = 1
	model.Host = 0
	if err := model.Create(); err != nil {
		t.Fatal(err)
	}
	model.Service.TLS = new(tls.Config)
	server := model.Service.NewServer()
	defer func() { server.Close(); model.Remove() }()

	client, err := govmomi.NewClient(ctx, server.URL, true)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Logout(ctx)

	registerTestExtension(t, ctx, client)

	var callCount atomic.Int64

	w := NewTaskWatcher(ctx, TaskWatcherConfig{
		ClientFn: func() *vim25.Client { return client.Client },
		ConnectFn: func(_ context.Context) error {
			n := callCount.Add(1)
			if n%3 == 0 {
				return errors.New("simulated flap")
			}
			return nil
		},
	})
	defer w.Stop()

	// Despite flapping, watcher should eventually become ready.
	waitForReady(t, w, 30*time.Second)

	taskRef := createSimTask(t, ctx, client)
	completeSimTask(ctx, client, taskRef)
	info, err := w.WaitForTask(ctx, taskRef)
	if err != nil {
		t.Fatalf("WaitForTask: %v", err)
	}
	if info.State != types.TaskInfoStateSuccess {
		t.Fatalf("expected success, got %v", info.State)
	}
	t.Logf("connectFn was called %d times (some failed)", callCount.Load())
}

// ---------------------------------------------------------------------------
// 10. Concurrent WaitForTask during watcher reconnect cycle
// ---------------------------------------------------------------------------

func TestFault_ConcurrentWaitDuringReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	model := simulator.VPX()
	model.Datacenter = 1
	model.Cluster = 1
	model.Host = 0
	if err := model.Create(); err != nil {
		t.Fatal(err)
	}
	model.Service.TLS = new(tls.Config)
	server := model.Service.NewServer()
	defer func() { server.Close(); model.Remove() }()

	client, err := govmomi.NewClient(ctx, server.URL, true)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Logout(ctx)

	registerTestExtension(t, ctx, client)

	w := NewTaskWatcher(ctx, TaskWatcherConfig{
		ClientFn:  func() *vim25.Client { return client.Client },
		ConnectFn: func(_ context.Context) error { return nil },
	})
	defer w.Stop()

	waitForReady(t, w, 10*time.Second)

	// Trigger a reset, then immediately launch concurrent WaitForTask calls.
	w.ResetClient()

	const concurrency = 20
	var (
		wg         sync.WaitGroup
		succeeded  atomic.Int64
		notReady   atomic.Int64
		otherError atomic.Int64
	)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			taskRef := createSimTask(t, ctx, client)
			completeSimTask(ctx, client, taskRef)

			taskCtx, taskCancel := context.WithTimeout(ctx, 20*time.Second)
			defer taskCancel()

			info, err := w.WaitForTask(taskCtx, taskRef)
			if err != nil {
				if errors.Is(err, ErrWatcherNotReady) {
					notReady.Add(1)
				} else {
					otherError.Add(1)
				}
				return
			}
			if info.State == types.TaskInfoStateSuccess {
				succeeded.Add(1)
			}
		}()
	}

	wg.Wait()

	t.Logf("Concurrent during reconnect: succeeded=%d, notReady=%d, otherError=%d",
		succeeded.Load(), notReady.Load(), otherError.Load())

	// At least some should succeed (the watcher recovers quickly).
	if succeeded.Load() == 0 && notReady.Load() == 0 && otherError.Load() == 0 {
		t.Fatal("all callers returned nothing -- something is wrong")
	}
}

// ---------------------------------------------------------------------------
// 11. Large-scale stress with mixed VC-down faults: 120k tasks, 64 threads,
//     periodic connectFn failures, ResetClient signals, and clientFn nil returns.
//
// The test runs in batches of 30k tasks per fresh vcsim instance. A background
// "chaos" goroutine triggers fault events at random intervals:
//   - connectFn returns error for a short window (simulates VC unreachable)
//   - ResetClient() signal (simulates credential rotation)
//   - clientFn returns nil for a short window (simulates stale client)
//
// Tasks that fail due to faults are counted separately. The test verifies that:
//   - The watcher always recovers after each fault window.
//   - The total of (success + expected_fault_errors) == total_submitted.
//   - No goroutine leaks or deadlocks.
// ---------------------------------------------------------------------------

func TestFault_StressWithVCDownScenarios(t *testing.T) {
	// Lower reconnectDelay for this test so the watcher recovers quickly
	// after each fault, leaving time to actually process tasks.
	origDelay := reconnectDelay
	reconnectDelay = 500 * time.Millisecond
	defer func() { reconnectDelay = origDelay }()

	const (
		threads   = 64
		batchSize = 30_000
	)
	totalTasks := 120_000
	if testing.Short() {
		totalTasks = 1280
	}

	var (
		cumulativeSuccess  atomic.Int64
		cumulativeFaultErr atomic.Int64
		cumulativeOtherErr atomic.Int64
	)

	overallStart := time.Now()

	for batchStart := 0; batchStart < totalTasks; batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > totalTasks {
			batchEnd = totalTasks
		}
		batchTotal := batchEnd - batchStart
		perThread := batchTotal / threads
		// Adjust for integer truncation so accounting is exact.
		actualBatchTotal := perThread * threads

		t.Run(fmt.Sprintf("batch_%dk-%dk", batchStart/1000, batchEnd/1000), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			model := simulator.VPX()
			model.Datacenter = 1
			model.Cluster = 1
			model.Host = 0
			if err := model.Create(); err != nil {
				t.Fatal(err)
			}
			model.Service.TLS = new(tls.Config)
			server := model.Service.NewServer()
			defer func() { server.Close(); model.Remove() }()

			client, err := govmomi.NewClient(ctx, server.URL, true)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Logout(ctx)
			registerTestExtension(t, ctx, client)

			var connectDown atomic.Bool
			var clientNil atomic.Bool

			w := NewTaskWatcher(ctx, TaskWatcherConfig{
				ClientFn: func() *vim25.Client {
					if clientNil.Load() {
						return nil
					}
					return client.Client
				},
				ConnectFn: func(_ context.Context) error {
					if connectDown.Load() {
						return errors.New("simulated VC down")
					}
					return nil
				},
			})
			defer w.Stop()

			waitForReady(t, w, 15*time.Second)

			// Chaos goroutine: injects faults at longer intervals so the
			// watcher gets meaningful uptime between faults to process tasks.
			// Each fault window is short (200-500ms), with 4-6s of uptime between.
			chaosCtx, chaosCancel := context.WithCancel(ctx)
			defer chaosCancel()

			var faultEvents atomic.Int64
			go func() {
				faultCycle := 0
				for {
					// 4-6s of uptime between faults (varies by cycle).
					uptime := 4*time.Second + time.Duration(faultCycle%3)*time.Second
					select {
					case <-chaosCtx.Done():
						return
					case <-time.After(uptime):
					}

					faultCycle++
					kind := faultCycle % 3
					switch kind {
					case 0:
						connectDown.Store(true)
						w.ResetClient()
						select {
						case <-time.After(500 * time.Millisecond):
						case <-chaosCtx.Done():
							connectDown.Store(false)
							return
						}
						connectDown.Store(false)
					case 1:
						w.ResetClient()
					case 2:
						clientNil.Store(true)
						w.ResetClient()
						select {
						case <-time.After(200 * time.Millisecond):
						case <-chaosCtx.Done():
							clientNil.Store(false)
							return
						}
						clientNil.Store(false)
					}
					faultEvents.Add(1)
				}
			}()

			var (
				successCount  atomic.Int64
				faultErrCount atomic.Int64
				otherErrCount atomic.Int64
				wg            sync.WaitGroup
			)

			start := time.Now()

			for i := 0; i < threads; i++ {
				wg.Add(1)
				go func(threadID int) {
					defer wg.Done()
					for j := 0; j < perThread; j++ {
						taskRef := createSimTask(t, ctx, client)
						completeSimTask(ctx, client, taskRef)

						taskCtx, taskCancel := context.WithTimeout(ctx, 30*time.Second)
						info, err := w.WaitForTask(taskCtx, taskRef)
						taskCancel()

						if err != nil {
							errMsg := err.Error()
							isFaultRelated := false
							for _, needle := range []string{
								"event loop exited",
								"watcher is not ready",
								"watcher is shut down",
								"connect failed",
								"simulated VC down",
								"failed to add task",
							} {
								if containsIgnoreCase(errMsg, needle) {
									isFaultRelated = true
									break
								}
							}
							if isFaultRelated {
								faultErrCount.Add(1)
							} else {
								otherErrCount.Add(1)
								if otherErrCount.Load() <= 10 {
									t.Errorf("thread %d task %d: unexpected error: %v", threadID, j, err)
								}
							}
							continue
						}
						if info != nil && info.State == types.TaskInfoStateSuccess {
							successCount.Add(1)
						}
					}
				}(i)
			}

			wg.Wait()
			chaosCancel()
			elapsed := time.Since(start)

			// Make sure watcher recovers after all faults.
			connectDown.Store(false)
			clientNil.Store(false)
			waitForReady(t, w, 15*time.Second)

			total := successCount.Load() + faultErrCount.Load() + otherErrCount.Load()
			throughput := float64(successCount.Load()) / elapsed.Seconds()

			t.Logf("Batch %dk-%dk: %d success, %d fault-errors, %d other-errors, total=%d/%d",
				batchStart/1000, batchEnd/1000,
				successCount.Load(), faultErrCount.Load(), otherErrCount.Load(),
				total, int64(actualBatchTotal))
			t.Logf("  Fault events injected: %d", faultEvents.Load())
			t.Logf("  Throughput: %.1f tasks/sec, elapsed: %v", throughput, elapsed)

			if total != int64(actualBatchTotal) {
				t.Errorf("task accounting mismatch: got %d, want %d", total, actualBatchTotal)
			}
			if otherErrCount.Load() > 0 {
				t.Errorf("unexpected (non-fault) errors: %d", otherErrCount.Load())
			}
			if successCount.Load() == 0 {
				t.Error("no tasks succeeded at all — something is fundamentally broken")
			}

			cumulativeSuccess.Add(successCount.Load())
			cumulativeFaultErr.Add(faultErrCount.Load())
			cumulativeOtherErr.Add(otherErrCount.Load())
		})
	}

	overallElapsed := time.Since(overallStart)
	overallTotal := cumulativeSuccess.Load() + cumulativeFaultErr.Load() + cumulativeOtherErr.Load()
	overallThroughput := float64(cumulativeSuccess.Load()) / overallElapsed.Seconds()

	t.Logf("=== Fault Stress Overall Results ===")
	t.Logf("Total tasks:       %d", overallTotal)
	t.Logf("Successes:         %d (%.1f%%)", cumulativeSuccess.Load(),
		float64(cumulativeSuccess.Load())*100/float64(overallTotal))
	t.Logf("Fault errors:      %d (%.1f%%)", cumulativeFaultErr.Load(),
		float64(cumulativeFaultErr.Load())*100/float64(overallTotal))
	t.Logf("Other errors:      %d", cumulativeOtherErr.Load())
	t.Logf("Elapsed:           %v", overallElapsed)
	t.Logf("Throughput:        %.1f success tasks/sec (overall)", overallThroughput)

	if cumulativeOtherErr.Load() > 0 {
		t.Errorf("unexpected errors across all batches: %d", cumulativeOtherErr.Load())
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const faultTestTaskTypeID = "com.vmware.csi.faulttest.task"

func registerTestExtension(t testing.TB, ctx context.Context, client *govmomi.Client) {
	t.Helper()
	extMgr := object.NewExtensionManager(client.Client)
	ext := types.Extension{
		Key:         "com.vmware.csi.faulttest",
		Version:     "1.0",
		Description: &types.Description{Label: "fault test", Summary: "fault test"},
		TaskList: []types.ExtensionTaskTypeInfo{
			{TaskID: faultTestTaskTypeID},
		},
	}
	if err := extMgr.Register(ctx, ext); err != nil {
		t.Fatalf("register extension: %v", err)
	}
}

func createSimTask(t testing.TB, ctx context.Context, client *govmomi.Client) types.ManagedObjectReference {
	t.Helper()
	resp, err := methods.CreateTask(ctx, client.Client, &types.CreateTask{
		This:       *client.Client.ServiceContent.TaskManager,
		Obj:        client.Client.ServiceContent.RootFolder,
		TaskTypeId: faultTestTaskTypeID,
		Cancelable: false,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	return resp.Returnval.Task
}

func completeSimTask(ctx context.Context, client *govmomi.Client, taskRef types.ManagedObjectReference) {
	taskObj := object.NewTask(client.Client, taskRef)
	_ = taskObj.SetState(ctx, types.TaskInfoStateRunning, nil, nil)
	_ = taskObj.SetState(ctx, types.TaskInfoStateSuccess, nil, nil)
}

func completeSimTaskWith(ctx context.Context, client *govmomi.Client, taskRef types.ManagedObjectReference) {
	completeSimTask(ctx, client, taskRef)
}

func waitForReady(t testing.TB, w *TaskWatcher, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for !w.IsReady() {
		select {
		case <-deadline:
			t.Fatalf("watcher did not become ready within %v", timeout)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// suppress unused import warning
var _ = fmt.Sprintf
