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
	"runtime"
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

const watcherTestTaskTypeID = "com.vmware.csi.watchertest.task"

type watcherTestEnv struct {
	model   *simulator.Model
	server  *simulator.Server
	client  *govmomi.Client
	watcher *TaskWatcher
	ctx     context.Context
	cancel  context.CancelFunc
	taskMgr types.ManagedObjectReference
}

func newWatcherTestEnv(t *testing.T) *watcherTestEnv {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	model := simulator.VPX()
	model.Datacenter = 1
	model.Cluster = 1
	model.Host = 0
	if err := model.Create(); err != nil {
		cancel()
		t.Fatalf("simulator model create: %v", err)
	}

	model.Service.TLS = new(tls.Config)
	server := model.Service.NewServer()

	client, err := govmomi.NewClient(ctx, server.URL, true)
	if err != nil {
		cancel()
		t.Fatalf("govmomi client: %v", err)
	}

	extMgr := object.NewExtensionManager(client.Client)
	ext := types.Extension{
		Key:         "com.vmware.csi.watchertest",
		Version:     "1.0",
		Description: &types.Description{Label: "watcher test", Summary: "watcher test extension"},
		TaskList: []types.ExtensionTaskTypeInfo{
			{TaskID: watcherTestTaskTypeID},
		},
	}
	if err := extMgr.Register(ctx, ext); err != nil {
		cancel()
		t.Fatalf("register extension: %v", err)
	}

	vimClient := client.Client
	watcher := NewTaskWatcher(ctx, TaskWatcherConfig{
		ClientFn: func() *vim25.Client {
			return vimClient
		},
		ConnectFn: func(_ context.Context) error {
			return nil // vcsim sessions don't expire
		},
	})

	return &watcherTestEnv{
		model:   model,
		server:  server,
		client:  client,
		watcher: watcher,
		ctx:     ctx,
		cancel:  cancel,
		taskMgr: *client.Client.ServiceContent.TaskManager,
	}
}

func (e *watcherTestEnv) close() {
	e.watcher.Stop()
	e.client.Logout(e.ctx)
	e.cancel()
	e.server.Close()
	e.model.Remove()
}

func (e *watcherTestEnv) createTask(t testing.TB) types.ManagedObjectReference {
	t.Helper()
	resp, err := methods.CreateTask(e.ctx, e.client.Client, &types.CreateTask{
		This:       e.taskMgr,
		Obj:        e.client.Client.ServiceContent.RootFolder,
		TaskTypeId: watcherTestTaskTypeID,
		Cancelable: false,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	return resp.Returnval.Task
}

func (e *watcherTestEnv) completeTaskSuccess(taskRef types.ManagedObjectReference) {
	taskObj := object.NewTask(e.client.Client, taskRef)
	_ = taskObj.SetState(e.ctx, types.TaskInfoStateRunning, nil, nil)
	_ = taskObj.SetState(e.ctx, types.TaskInfoStateSuccess, nil, nil)
}

func (e *watcherTestEnv) completeTaskError(taskRef types.ManagedObjectReference, msg string) {
	taskObj := object.NewTask(e.client.Client, taskRef)
	_ = taskObj.SetState(e.ctx, types.TaskInfoStateRunning, nil, nil)
	_ = taskObj.SetState(e.ctx, types.TaskInfoStateError, nil, &types.LocalizedMethodFault{
		Fault:            &types.RuntimeFault{},
		LocalizedMessage: msg,
	})
}

func (e *watcherTestEnv) completeTaskAsync(taskRef types.ManagedObjectReference, delay time.Duration) {
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

func TestTaskWatcher_SingleTask(t *testing.T) {
	env := newWatcherTestEnv(t)
	defer env.close()

	taskRef := env.createTask(t)
	env.completeTaskAsync(taskRef, 50*time.Millisecond)

	info, err := env.watcher.WaitForTask(env.ctx, taskRef)
	if err != nil {
		t.Fatalf("WaitForTask: %v", err)
	}
	if info == nil {
		t.Fatal("WaitForTask returned nil TaskInfo")
	}
	if info.State != types.TaskInfoStateSuccess {
		t.Fatalf("expected success, got %v", info.State)
	}
}

func TestTaskWatcher_TaskError(t *testing.T) {
	env := newWatcherTestEnv(t)
	defer env.close()

	taskRef := env.createTask(t)
	go func() {
		time.Sleep(50 * time.Millisecond)
		env.completeTaskError(taskRef, "simulated CNS failure")
	}()

	_, err := env.watcher.WaitForTask(env.ctx, taskRef)
	if err == nil {
		t.Fatal("expected error from failed task, got nil")
	}
}

func TestTaskWatcher_ContextTimeout(t *testing.T) {
	env := newWatcherTestEnv(t)
	defer env.close()

	taskRef := env.createTask(t)
	// Task will never complete.

	ctx, cancel := context.WithTimeout(env.ctx, 200*time.Millisecond)
	defer cancel()

	_, err := env.watcher.WaitForTask(ctx, taskRef)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestTaskWatcher_AlreadyCompleted(t *testing.T) {
	env := newWatcherTestEnv(t)
	defer env.close()

	taskRef := env.createTask(t)
	env.completeTaskSuccess(taskRef)

	info, err := env.watcher.WaitForTask(env.ctx, taskRef)
	if err != nil {
		t.Fatalf("WaitForTask: %v", err)
	}
	if info.State != types.TaskInfoStateSuccess {
		t.Fatalf("expected success, got %v", info.State)
	}
}

func TestTaskWatcher_ConcurrentBasic(t *testing.T) {
	env := newWatcherTestEnv(t)
	defer env.close()

	const concurrency = 65
	var wg sync.WaitGroup
	errs := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			taskRef := env.createTask(t)
			env.completeTaskAsync(taskRef, 20*time.Millisecond)

			info, err := env.watcher.WaitForTask(env.ctx, taskRef)
			if err != nil {
				errs <- fmt.Errorf("goroutine %d: %v", idx, err)
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

func TestTaskWatcher_StopCleanup(t *testing.T) {
	env := newWatcherTestEnv(t)

	goroutinesBefore := runtime.NumGoroutine()
	env.close()
	time.Sleep(100 * time.Millisecond)
	goroutinesAfter := runtime.NumGoroutine()

	delta := goroutinesAfter - goroutinesBefore
	if delta > 5 {
		t.Errorf("potential goroutine leak after Stop: delta=%d", delta)
	}
}

// ---------------------------------------------------------------------------
// Stress Tests
// ---------------------------------------------------------------------------

// TestTaskWatcher_Stress runs 64 threads creating volumes sequentially through
// the watcher, in batches of 50k with fresh vcsim per batch.
func TestTaskWatcher_Stress(t *testing.T) {
	const threads = 64
	totalVolumes := 250_000
	batchSize := 50_000
	if testing.Short() {
		totalVolumes = 640
		batchSize = 640
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
			env := newWatcherTestEnv(t)
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

						info, err := env.watcher.WaitForTask(env.ctx, taskRef)
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

	t.Logf("--- TaskWatcher Overall Stress Results ---")
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

// TestTaskWatcher_StressMixed runs a mix of success and error tasks.
func TestTaskWatcher_StressMixed(t *testing.T) {
	env := newWatcherTestEnv(t)
	defer env.close()

	const threads = 64
	totalVolumes := 6400
	if testing.Short() {
		totalVolumes = 640
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

				_, err := env.watcher.WaitForTask(env.ctx, taskRef)
				if shouldFail {
					if err != nil {
						errorCount.Add(1)
					} else {
						t.Errorf("thread %d vol %d: expected error", threadID, j)
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

	t.Logf("--- TaskWatcher Mixed Stress Results ---")
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

// TestTaskWatcher_MemoryStability checks for PropertyCollector leaks.
func TestTaskWatcher_MemoryStability(t *testing.T) {
	env := newWatcherTestEnv(t)
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
		_, err := env.watcher.WaitForTask(env.ctx, taskRef)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	heapDelta := int64(memAfter.HeapInuse) - int64(memBefore.HeapInuse)

	t.Logf("--- TaskWatcher Memory Stability ---")
	t.Logf("Iterations:  %d", iterations)
	t.Logf("Heap delta:  %d MB", heapDelta/(1024*1024))

	if heapDelta > 100*1024*1024 {
		t.Errorf("heap grew by %d MB — possible leak", heapDelta/(1024*1024))
	}
}

// TestTaskWatcher_ContextCancellationUnderLoad verifies clean cancellation.
func TestTaskWatcher_ContextCancellationUnderLoad(t *testing.T) {
	env := newWatcherTestEnv(t)
	defer env.close()

	const threads = 64
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
					ctx, cancel := context.WithTimeout(env.ctx, 5*time.Millisecond)
					_, err := env.watcher.WaitForTask(ctx, taskRef)
					cancel()
					if err != nil {
						cancelledCount.Add(1)
					}
					env.completeTaskSuccess(taskRef)
				} else {
					env.completeTaskSuccess(taskRef)
					info, err := env.watcher.WaitForTask(env.ctx, taskRef)
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

	t.Logf("--- TaskWatcher Context Cancellation ---")
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
		t.Errorf("potential goroutine leak: delta=%d", goroutineDelta)
	}
}
