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
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vim25/methods"
	"github.com/vmware/govmomi/vim25/types"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
)

const stressTestTaskTypeID = "com.vmware.csi.stresstest.createVolume"

// testVCSimEnv encapsulates a vcsim environment and a defaultManager wired to it.
type testVCSimEnv struct {
	model  *simulator.Model
	server *simulator.Server
	client *govmomi.Client
	mgr    *defaultManager
	ctx    context.Context
	taskMgr types.ManagedObjectReference
}

func newTestVCSimEnv(t *testing.T) *testVCSimEnv {
	t.Helper()
	ctx := context.Background()

	model := simulator.VPX()
	model.Datacenter = 1
	model.Cluster = 1
	model.Host = 0
	if err := model.Create(); err != nil {
		t.Fatalf("simulator model create: %v", err)
	}

	model.Service.TLS = new(tls.Config)
	server := model.Service.NewServer()

	client, err := govmomi.NewClient(ctx, server.URL, true)
	if err != nil {
		t.Fatalf("govmomi client: %v", err)
	}

	vc := &cnsvsphere.VirtualCenter{
		Client:      client,
		ClientMutex: &sync.Mutex{},
	}

	mgr := &defaultManager{
		virtualCenter: vc,
	}

	extMgr := object.NewExtensionManager(client.Client)
	ext := types.Extension{
		Key:         "com.vmware.csi.stresstest",
		Version:     "1.0",
		Description: &types.Description{Label: "CSI stress test", Summary: "CSI stress test extension"},
		TaskList: []types.ExtensionTaskTypeInfo{
			{TaskID: stressTestTaskTypeID},
		},
	}
	if err := extMgr.Register(ctx, ext); err != nil {
		t.Fatalf("register extension: %v", err)
	}

	return &testVCSimEnv{
		model:   model,
		server:  server,
		client:  client,
		mgr:     mgr,
		ctx:     ctx,
		taskMgr: *client.Client.ServiceContent.TaskManager,
	}
}

func (e *testVCSimEnv) close() {
	e.client.Logout(e.ctx)
	e.server.Close()
	e.model.Remove()
}

// createTask creates a task in queued state via the simulator TaskManager API.
func (e *testVCSimEnv) createTask(t testing.TB) types.ManagedObjectReference {
	t.Helper()
	resp, err := methods.CreateTask(e.ctx, e.client.Client, &types.CreateTask{
		This:       e.taskMgr,
		Obj:        e.client.Client.ServiceContent.RootFolder,
		TaskTypeId: stressTestTaskTypeID,
		Cancelable: false,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	return resp.Returnval.Task
}

// completeTaskSuccess transitions a task to running then success synchronously.
func (e *testVCSimEnv) completeTaskSuccess(taskRef types.ManagedObjectReference) {
	taskObj := object.NewTask(e.client.Client, taskRef)
	_ = taskObj.SetState(e.ctx, types.TaskInfoStateRunning, nil, nil)
	_ = taskObj.SetState(e.ctx, types.TaskInfoStateSuccess, nil, nil)
}

// completeTaskError transitions a task to running then error synchronously.
func (e *testVCSimEnv) completeTaskError(taskRef types.ManagedObjectReference, msg string) {
	taskObj := object.NewTask(e.client.Client, taskRef)
	_ = taskObj.SetState(e.ctx, types.TaskInfoStateRunning, nil, nil)
	_ = taskObj.SetState(e.ctx, types.TaskInfoStateError, nil, &types.LocalizedMethodFault{
		Fault:            &types.RuntimeFault{},
		LocalizedMessage: msg,
	})
}

// completeTaskAsync transitions a task in a separate goroutine after delay.
// Only use for low-concurrency tests where goroutine count stays bounded.
func (e *testVCSimEnv) completeTaskAsync(taskRef types.ManagedObjectReference, delay time.Duration) {
	go func() {
		if delay > 0 {
			time.Sleep(delay)
		}
		e.completeTaskSuccess(taskRef)
	}()
}

// ---------------------------------------------------------------------------
// Correctness Tests
// ---------------------------------------------------------------------------

// TestWaitOnTask_SingleTask verifies the basic happy-path.
func TestWaitOnTask_SingleTask(t *testing.T) {
	env := newTestVCSimEnv(t)
	defer env.close()

	taskRef := env.createTask(t)
	env.completeTaskAsync(taskRef, 10*time.Millisecond)

	info, err := env.mgr.waitOnTask(env.ctx, taskRef)
	if err != nil {
		t.Fatalf("waitOnTask: %v", err)
	}
	if info == nil {
		t.Fatal("waitOnTask returned nil TaskInfo")
	}
	if info.State != types.TaskInfoStateSuccess {
		t.Fatalf("expected success, got %v", info.State)
	}
}

// TestWaitOnTask_ContextTimeout verifies that waitOnTask respects context
// cancellation and returns promptly.
func TestWaitOnTask_ContextTimeout(t *testing.T) {
	env := newTestVCSimEnv(t)
	defer env.close()

	// Task will never complete — no goroutine sets its state.
	taskRef := env.createTask(t)

	ctx, cancel := context.WithTimeout(env.ctx, 200*time.Millisecond)
	defer cancel()

	_, err := env.mgr.waitOnTask(ctx, taskRef)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if ctx.Err() == nil {
		t.Fatal("expected context to be done")
	}
}

// TestWaitOnTask_TaskError verifies that waitOnTask propagates task failures.
func TestWaitOnTask_TaskError(t *testing.T) {
	env := newTestVCSimEnv(t)
	defer env.close()

	taskRef := env.createTask(t)

	go func() {
		time.Sleep(10 * time.Millisecond)
		env.completeTaskError(taskRef, "simulated CNS failure")
	}()

	_, err := env.mgr.waitOnTask(env.ctx, taskRef)
	if err == nil {
		t.Fatal("expected error from failed task, got nil")
	}
}

// TestWaitOnTask_AlreadyCompleted verifies waitOnTask works when the task
// reached a terminal state before the call is made.
func TestWaitOnTask_AlreadyCompleted(t *testing.T) {
	env := newTestVCSimEnv(t)
	defer env.close()

	taskRef := env.createTask(t)
	env.completeTaskSuccess(taskRef)

	info, err := env.mgr.waitOnTask(env.ctx, taskRef)
	if err != nil {
		t.Fatalf("waitOnTask: %v", err)
	}
	if info.State != types.TaskInfoStateSuccess {
		t.Fatalf("expected success, got %v", info.State)
	}
}

// ---------------------------------------------------------------------------
// Concurrency Tests
// ---------------------------------------------------------------------------

// TestWaitOnTask_ConcurrentBasic runs 65 concurrent goroutines each waiting
// on their own task with async completion to verify thread-safety.
func TestWaitOnTask_ConcurrentBasic(t *testing.T) {
	env := newTestVCSimEnv(t)
	defer env.close()

	const concurrency = 65
	var wg sync.WaitGroup
	errs := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			taskRef := env.createTask(t)
			delay := time.Duration(rand.Intn(50)+1) * time.Millisecond
			env.completeTaskAsync(taskRef, delay)

			info, err := env.mgr.waitOnTask(env.ctx, taskRef)
			if err != nil {
				errs <- fmt.Errorf("goroutine %d: waitOnTask: %v", idx, err)
				return
			}
			if info.State != types.TaskInfoStateSuccess {
				errs <- fmt.Errorf("goroutine %d: expected success, got %v", idx, info.State)
			}
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// ---------------------------------------------------------------------------
// Stress Tests
// ---------------------------------------------------------------------------

// TestWaitOnTask_StressSequential65Threads is the full-scale stress test.
// 65 goroutines each process volumes sequentially, up to 250k total.
// Under `go test -short` this runs a reduced count (650 total).
//
// The test is split into batches of 50k volumes, each with a fresh vcsim
// instance. This avoids the O(n) overhead in vcsim's TaskManager which
// keeps all tasks in memory and iterates them on every state change.
//
// The test measures correctness, throughput, memory stability, and goroutine leaks.
func TestWaitOnTask_StressSequential65Threads(t *testing.T) {
	const threads = 65
	totalVolumes := 250_000
	batchSize := 50_000
	if testing.Short() {
		totalVolumes = 650
		batchSize = 650
	}

	var (
		cumulativeSuccess atomic.Int64
		cumulativeErrors  atomic.Int64
	)

	overallStart := time.Now()
	goroutinesBefore := runtime.NumGoroutine()

	for batchStart := 0; batchStart < totalVolumes; batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > totalVolumes {
			batchEnd = totalVolumes
		}
		batchVolumes := batchEnd - batchStart
		volumesPerThread := batchVolumes / threads

		t.Run(fmt.Sprintf("batch_%dk-%dk", batchStart/1000, batchEnd/1000), func(t *testing.T) {
			env := newTestVCSimEnv(t)
			defer env.close()

			t.Logf("batch: %d threads x %d volumes/thread = %d volumes (cumulative: %d-%d)",
				threads, volumesPerThread, threads*volumesPerThread, batchStart, batchEnd)

			var (
				successCount atomic.Int64
				errorCount   atomic.Int64
				wg           sync.WaitGroup
			)

			start := time.Now()

			for i := 0; i < threads; i++ {
				wg.Add(1)
				go func(threadID int) {
					defer wg.Done()
					for j := 0; j < volumesPerThread; j++ {
						taskRef := env.createTask(t)
						env.completeTaskSuccess(taskRef)

						info, err := env.mgr.waitOnTask(env.ctx, taskRef)
						if err != nil {
							errorCount.Add(1)
							if errorCount.Load() <= 10 {
								t.Errorf("thread %d vol %d: %v", threadID, j, err)
							}
							continue
						}
						if info.State != types.TaskInfoStateSuccess {
							errorCount.Add(1)
							continue
						}
						successCount.Add(1)
					}
				}(i)
			}

			wg.Wait()
			elapsed := time.Since(start)
			throughput := float64(successCount.Load()) / elapsed.Seconds()

			t.Logf("Batch results: %d success, %d errors, %.1f tasks/sec, %v",
				successCount.Load(), errorCount.Load(), throughput, elapsed)

			cumulativeSuccess.Add(successCount.Load())
			cumulativeErrors.Add(errorCount.Load())

			if errorCount.Load() > 0 {
				t.Errorf("expected 0 errors, got %d", errorCount.Load())
			}
		})
	}

	overallElapsed := time.Since(overallStart)
	goroutinesAfter := runtime.NumGoroutine()
	goroutineDelta := goroutinesAfter - goroutinesBefore
	overallThroughput := float64(cumulativeSuccess.Load()) / overallElapsed.Seconds()

	t.Logf("--- Overall Stress Test Results ---")
	t.Logf("Total volumes:     %d", cumulativeSuccess.Load()+cumulativeErrors.Load())
	t.Logf("Successes:         %d", cumulativeSuccess.Load())
	t.Logf("Errors:            %d", cumulativeErrors.Load())
	t.Logf("Elapsed:           %v", overallElapsed)
	t.Logf("Throughput:        %.1f tasks/sec (overall)", overallThroughput)
	t.Logf("Goroutines before: %d", goroutinesBefore)
	t.Logf("Goroutines after:  %d (delta: %d)", goroutinesAfter, goroutineDelta)

	if cumulativeErrors.Load() > 0 {
		t.Errorf("expected 0 total errors, got %d", cumulativeErrors.Load())
	}
	if goroutineDelta > 20 {
		t.Errorf("potential goroutine leak: delta=%d", goroutineDelta)
	}
}

// TestWaitOnTask_StressMixedSuccessAndFailure runs 65 threads with a mix of
// succeeding and failing tasks (~10% failure rate).
func TestWaitOnTask_StressMixedSuccessAndFailure(t *testing.T) {
	env := newTestVCSimEnv(t)
	defer env.close()

	const threads = 65
	totalVolumes := 6500
	if testing.Short() {
		totalVolumes = 650
	}
	volumesPerThread := totalVolumes / threads
	failEveryN := 10

	var (
		successCount atomic.Int64
		errorCount   atomic.Int64
		wg           sync.WaitGroup
	)

	start := time.Now()

	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()
			for j := 0; j < volumesPerThread; j++ {
				shouldFail := (j % failEveryN) == (failEveryN - 1)
				taskRef := env.createTask(t)

				if shouldFail {
					env.completeTaskError(taskRef, "simulated storage full")
				} else {
					env.completeTaskSuccess(taskRef)
				}

				_, err := env.mgr.waitOnTask(env.ctx, taskRef)
				if shouldFail {
					if err != nil {
						errorCount.Add(1)
					} else {
						t.Errorf("thread %d vol %d: expected error for failing task", threadID, j)
					}
				} else {
					if err != nil {
						errorCount.Add(1)
						t.Errorf("thread %d vol %d: unexpected error: %v", threadID, j, err)
					} else {
						successCount.Add(1)
					}
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	expectedSuccess := int64(totalVolumes - totalVolumes/failEveryN)
	expectedErrors := int64(totalVolumes / failEveryN)

	t.Logf("--- Mixed Stress Test Results ---")
	t.Logf("Total:    %d (success=%d, errors=%d)", totalVolumes, successCount.Load(), errorCount.Load())
	t.Logf("Expected: success=%d, errors=%d", expectedSuccess, expectedErrors)
	t.Logf("Elapsed:  %v", elapsed)

	if successCount.Load() != expectedSuccess {
		t.Errorf("success count: got %d, want %d", successCount.Load(), expectedSuccess)
	}
	if errorCount.Load() != expectedErrors {
		t.Errorf("error count: got %d, want %d", errorCount.Load(), expectedErrors)
	}
}

// TestWaitOnTask_StressBurstConcurrency creates all tasks first, completes
// them, then waits on them all concurrently to maximise PropertyCollector
// pressure on the vcsim server.
func TestWaitOnTask_StressBurstConcurrency(t *testing.T) {
	env := newTestVCSimEnv(t)
	defer env.close()

	burstSize := 500
	if testing.Short() {
		burstSize = 65
	}

	// Phase 1: create and complete all tasks.
	taskRefs := make([]types.ManagedObjectReference, burstSize)
	for i := 0; i < burstSize; i++ {
		taskRefs[i] = env.createTask(t)
		env.completeTaskSuccess(taskRefs[i])
	}

	// Phase 2: wait on all tasks concurrently.
	var (
		wg           sync.WaitGroup
		successCount atomic.Int64
	)
	errs := make(chan error, burstSize)

	start := time.Now()
	for i, ref := range taskRefs {
		wg.Add(1)
		go func(idx int, ref types.ManagedObjectReference) {
			defer wg.Done()
			info, err := env.mgr.waitOnTask(env.ctx, ref)
			if err != nil {
				errs <- fmt.Errorf("task %d: %v", idx, err)
				return
			}
			if info.State != types.TaskInfoStateSuccess {
				errs <- fmt.Errorf("task %d: got %v", idx, info.State)
				return
			}
			successCount.Add(1)
		}(i, ref)
	}

	wg.Wait()
	close(errs)
	elapsed := time.Since(start)

	for err := range errs {
		t.Error(err)
	}

	t.Logf("--- Burst Concurrency Results ---")
	t.Logf("Burst size: %d", burstSize)
	t.Logf("Successes:  %d", successCount.Load())
	t.Logf("Elapsed:    %v", elapsed)

	if successCount.Load() != int64(burstSize) {
		t.Errorf("expected %d successes, got %d", burstSize, successCount.Load())
	}
}

// TestWaitOnTask_StressContextCancellationUnderLoad verifies that context
// cancellation is handled cleanly under heavy load without goroutine leaks.
func TestWaitOnTask_StressContextCancellationUnderLoad(t *testing.T) {
	env := newTestVCSimEnv(t)
	defer env.close()

	const threads = 65
	volumesPerThread := 20
	if testing.Short() {
		volumesPerThread = 5
	}

	var (
		cancelledCount atomic.Int64
		completedCount atomic.Int64
		wg             sync.WaitGroup
	)

	goroutinesBefore := runtime.NumGoroutine()

	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()
			for j := 0; j < volumesPerThread; j++ {
				taskRef := env.createTask(t)
				shouldCancel := j%3 == 0

				if shouldCancel {
					// Don't complete the task so the wait will time out.
					ctx, cancel := context.WithTimeout(env.ctx, 5*time.Millisecond)
					_, err := env.mgr.waitOnTask(ctx, taskRef)
					cancel()
					if err != nil {
						cancelledCount.Add(1)
					}
					// Clean up: complete it so vcsim state doesn't accumulate.
					env.completeTaskSuccess(taskRef)
				} else {
					// Complete synchronously, then wait.
					env.completeTaskSuccess(taskRef)
					info, err := env.mgr.waitOnTask(env.ctx, taskRef)
					if err == nil && info.State == types.TaskInfoStateSuccess {
						completedCount.Add(1)
					}
				}
			}
		}(i)
	}

	wg.Wait()

	time.Sleep(200 * time.Millisecond)
	goroutinesAfter := runtime.NumGoroutine()

	t.Logf("--- Context Cancellation Under Load ---")
	t.Logf("Completed: %d, Cancelled: %d", completedCount.Load(), cancelledCount.Load())
	t.Logf("Goroutines before: %d, after: %d", goroutinesBefore, goroutinesAfter)

	if cancelledCount.Load() == 0 {
		t.Error("expected some cancelled tasks")
	}
	if completedCount.Load() == 0 {
		t.Error("expected some completed tasks")
	}

	goroutineDelta := goroutinesAfter - goroutinesBefore
	if goroutineDelta > 30 {
		t.Errorf("potential goroutine leak: delta=%d (before=%d, after=%d)",
			goroutineDelta, goroutinesBefore, goroutinesAfter)
	}
}

// TestWaitOnTask_StressRapidFireNoDelay tests zero-delay task completion at max
// speed to stress the PropertyCollector create/destroy cycle.
func TestWaitOnTask_StressRapidFireNoDelay(t *testing.T) {
	env := newTestVCSimEnv(t)
	defer env.close()

	const threads = 65
	totalVolumes := 13_000
	if testing.Short() {
		totalVolumes = 650
	}
	volumesPerThread := totalVolumes / threads

	var (
		successCount atomic.Int64
		wg           sync.WaitGroup
	)

	start := time.Now()

	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()
			for j := 0; j < volumesPerThread; j++ {
				taskRef := env.createTask(t)
				env.completeTaskSuccess(taskRef)

				info, err := env.mgr.waitOnTask(env.ctx, taskRef)
				if err != nil {
					t.Errorf("thread %d vol %d: waitOnTask: %v", threadID, j, err)
					continue
				}
				if info.State != types.TaskInfoStateSuccess {
					t.Errorf("thread %d vol %d: got %v", threadID, j, info.State)
					continue
				}
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	throughput := float64(successCount.Load()) / elapsed.Seconds()
	t.Logf("--- Rapid Fire Results ---")
	t.Logf("Successes:  %d / %d", successCount.Load(), totalVolumes)
	t.Logf("Elapsed:    %v", elapsed)
	t.Logf("Throughput: %.1f tasks/sec", throughput)

	if successCount.Load() != int64(totalVolumes) {
		t.Errorf("expected %d successes, got %d", totalVolumes, successCount.Load())
	}
}

// TestWaitOnTask_MemoryStability runs a sustained load and checks that memory
// usage doesn't grow unboundedly, verifying no PropertyCollector leaks.
func TestWaitOnTask_MemoryStability(t *testing.T) {
	env := newTestVCSimEnv(t)
	defer env.close()

	iterations := 5000
	if testing.Short() {
		iterations = 200
	}

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	for i := 0; i < iterations; i++ {
		taskRef := env.createTask(t)
		env.completeTaskSuccess(taskRef)
		_, err := env.mgr.waitOnTask(env.ctx, taskRef)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	allocDelta := int64(memAfter.TotalAlloc) - int64(memBefore.TotalAlloc)
	perTask := allocDelta / int64(iterations)
	heapDelta := int64(memAfter.HeapInuse) - int64(memBefore.HeapInuse)

	t.Logf("--- Memory Stability ---")
	t.Logf("Iterations:      %d", iterations)
	t.Logf("Total alloc:     %d MB (delta)", allocDelta/(1024*1024))
	t.Logf("Per-task alloc:  %d bytes", perTask)
	t.Logf("Heap in-use:     %d MB (delta)", heapDelta/(1024*1024))

	if heapDelta > 100*1024*1024 {
		t.Errorf("heap grew by %d MB — possible PropertyCollector leak", heapDelta/(1024*1024))
	}
}
