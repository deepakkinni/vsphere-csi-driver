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
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/view"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/methods"
	"github.com/vmware/govmomi/vim25/types"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	cmdChannelSize      = 4096
	watchdogInterval    = 30 * time.Second
	watchdogGracePeriod = 2 * time.Minute
)

var reconnectDelay = 5 * time.Second

var pollMaxWaitSeconds int32 = 1

var (
	ErrWatcherStopped  = errors.New("task watcher is shut down")
	ErrWatcherNotReady = errors.New("task watcher is not ready")
)

type taskWatcherResult struct {
	Info *types.TaskInfo
	Err  error
}

type commandOp int

const (
	opAdd commandOp = iota
	opRemove
)

type watcherCommand struct {
	op       commandOp
	taskRef  types.ManagedObjectReference
	resultCh chan<- taskWatcherResult
	errCh    chan<- error
}

// TaskWatcher multiplexes task completion monitoring through a single
// PropertyCollector + ListView. All vSphere state is owned exclusively by a
// single goroutine (the actor), eliminating shared-state concurrency bugs.
//
// Session resilience:
//   - Any SOAP error from ListView.Add / WaitForUpdatesEx that indicates a
//     session failure (NotAuthenticated, connection reset, etc.) causes the
//     event loop to exit, fail all pending tasks, tear down vSphere objects,
//     call connectFn to re-establish the session, and rebuild from scratch.
//   - External code (e.g. ReloadConfiguration) can signal credential rotation
//     via ResetClient(), which immediately breaks the event loop.
//   - A watchdog goroutine monitors the ready state. If the watcher has valid
//     credentials (connectFn succeeds) but cannot reach ready state within
//     the grace period, it invokes the fatalFn callback (typically os.Exit).
type TaskWatcher struct {
	cmdCh     chan watcherCommand
	resetCh   chan struct{}       // external signal: credentials changed
	done      chan struct{}       // closed when the actor exits
	cancelFn  context.CancelFunc
	clientFn  func() *vim25.Client               // returns current vim25 client
	connectFn func(context.Context) error         // calls VirtualCenter.Connect
	fatalFn   func()                              // called by watchdog on irrecoverable failure
	ready     atomic.Bool                         // true when event loop is active
}

// TaskWatcherConfig holds the dependencies for creating a TaskWatcher.
type TaskWatcherConfig struct {
	// ClientFn returns the current vim25 client pointer. Called on each
	// reconnect to pick up refreshed sessions.
	ClientFn func() *vim25.Client

	// ConnectFn re-establishes the vCenter session. Typically wraps
	// VirtualCenter.Connect(ctx). Called before each setup attempt.
	ConnectFn func(context.Context) error

	// FatalFn is invoked by the watchdog when the watcher cannot recover
	// despite valid credentials. Typically os.Exit(1).
	// If nil, the watchdog logs but does not exit.
	FatalFn func()
}

// NewTaskWatcher creates and starts a TaskWatcher. Call Stop() to shut down.
func NewTaskWatcher(ctx context.Context, cfg TaskWatcherConfig) *TaskWatcher {
	ctx, cancel := context.WithCancel(ctx)
	w := &TaskWatcher{
		cmdCh:     make(chan watcherCommand, cmdChannelSize),
		resetCh:   make(chan struct{}, 1),
		done:      make(chan struct{}),
		cancelFn:  cancel,
		clientFn:  cfg.ClientFn,
		connectFn: cfg.ConnectFn,
		fatalFn:   cfg.FatalFn,
	}
	go w.run(ctx)
	if w.fatalFn != nil {
		go w.watchdog(ctx)
	}
	return w
}

// Stop shuts down the actor goroutine and waits for it to exit.
func (w *TaskWatcher) Stop() {
	w.cancelFn()
	<-w.done
}

// IsReady returns true when the event loop is active and accepting tasks.
func (w *TaskWatcher) IsReady() bool {
	return w.ready.Load()
}

// ResetClient signals the actor that credentials have changed and it should
// tear down the current session and reconnect. Non-blocking; safe to call
// from any goroutine.
func (w *TaskWatcher) ResetClient() {
	select {
	case w.resetCh <- struct{}{}:
	default:
	}
}

// WaitForTask blocks until the vCenter task reaches a terminal state or
// the caller's context expires. If the watcher is not yet ready (e.g.
// reconnecting), it waits up to the caller's context deadline for it to
// become ready before returning ErrWatcherNotReady.
func (w *TaskWatcher) WaitForTask(
	ctx context.Context,
	taskRef types.ManagedObjectReference,
) (*types.TaskInfo, error) {

	// Wait for the watcher to become ready, respecting caller context.
	if !w.ready.Load() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if w.ready.Load() {
					goto ready
				}
			case <-ctx.Done():
				return nil, ErrWatcherNotReady
			case <-w.done:
				return nil, ErrWatcherStopped
			}
		}
	}
ready:

	resultCh := make(chan taskWatcherResult, 1)
	errCh := make(chan error, 1)

	select {
	case w.cmdCh <- watcherCommand{op: opAdd, taskRef: taskRef, resultCh: resultCh, errCh: errCh}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-w.done:
		return nil, ErrWatcherStopped
	}

	select {
	case err := <-errCh:
		if err != nil {
			return nil, fmt.Errorf("failed to add task to watcher: %w", err)
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-w.done:
		return nil, ErrWatcherStopped
	}

	select {
	case result := <-resultCh:
		return result.Info, result.Err
	case <-ctx.Done():
		select {
		case w.cmdCh <- watcherCommand{op: opRemove, taskRef: taskRef, errCh: make(chan error, 1)}:
		default:
		}
		return nil, ctx.Err()
	case <-w.done:
		return nil, ErrWatcherStopped
	}
}

// ---------------------------------------------------------------------------
// Actor goroutine
// ---------------------------------------------------------------------------

func (w *TaskWatcher) run(ctx context.Context) {
	defer close(w.done)
	log := logger.GetLoggerWithNoContext()

	for {
		if ctx.Err() != nil {
			return
		}

		// Step 1: ensure we have a live vCenter session.
		if w.connectFn != nil {
			if err := w.connectFn(ctx); err != nil {
				log.Errorf("TaskWatcher: connect failed: %v", err)
				w.ready.Store(false)
				w.drainAndFailCommands(fmt.Errorf("connect failed: %w", err))
				select {
				case <-time.After(reconnectDelay):
					continue
				case <-ctx.Done():
					return
				}
			}
		}

		// Step 2: get the (now-refreshed) vim25 client.
		client := w.clientFn()
		if client == nil {
			log.Errorf("TaskWatcher: clientFn returned nil after successful connect")
			w.ready.Store(false)
			select {
			case <-time.After(reconnectDelay):
				continue
			case <-ctx.Done():
				return
			}
		}

		// Step 3: create ListView + PropertyCollector.
		lv, pc, err := w.setup(ctx, client)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Errorf("TaskWatcher: setup failed: %v", err)
			w.ready.Store(false)
			w.drainAndFailCommands(fmt.Errorf("setup failed: %w", err))
			select {
			case <-time.After(reconnectDelay):
				continue
			case <-ctx.Done():
				return
			}
		}

		// Step 4: run the event loop.
		w.ready.Store(true)
		log.Infof("TaskWatcher: ready, entering event loop")
		pending, loopErr := w.eventLoop(ctx, client, lv, pc)
		w.ready.Store(false)

		w.teardown(lv, pc)
		if pending != nil {
			w.failAllPending(pending, fmt.Errorf("event loop exited: %w", loopErr))
		}

		if ctx.Err() != nil {
			return
		}

		log.Warnf("TaskWatcher: event loop exited: %v, reconnecting in %v", loopErr, reconnectDelay)
		select {
		case <-time.After(reconnectDelay):
		case <-ctx.Done():
			return
		}
	}
}

// watchdog runs alongside the actor. If credentials are valid (connectFn
// succeeds) but the watcher hasn't reached ready state within the grace
// period, it invokes fatalFn as a last resort.
func (w *TaskWatcher) watchdog(ctx context.Context) {
	log := logger.GetLoggerWithNoContext()
	ticker := time.NewTicker(watchdogInterval)
	defer ticker.Stop()

	var unhealthySince time.Time

	for {
		select {
		case <-ticker.C:
			if w.ready.Load() {
				unhealthySince = time.Time{}
				continue
			}

			canConnect := w.connectFn != nil && w.connectFn(ctx) == nil
			if !canConnect {
				// Credentials are also bad; nothing we can do, reset timer.
				unhealthySince = time.Time{}
				continue
			}

			if unhealthySince.IsZero() {
				unhealthySince = time.Now()
				log.Warnf("TaskWatcher watchdog: credentials valid but watcher not ready, starting grace period")
				continue
			}

			if time.Since(unhealthySince) > watchdogGracePeriod {
				log.Errorf("TaskWatcher watchdog: watcher not ready for %v despite valid credentials, invoking fatal callback",
					time.Since(unhealthySince))
				if w.fatalFn != nil {
					w.fatalFn()
				}
				return
			}

		case <-ctx.Done():
			return
		case <-w.done:
			return
		}
	}
}

func (w *TaskWatcher) setup(
	ctx context.Context,
	client *vim25.Client,
) (*view.ListView, *property.Collector, error) {
	vm := view.NewManager(client)
	lv, err := vm.CreateListView(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("create list view: %w", err)
	}
	pc, err := property.DefaultCollector(client).Create(ctx)
	if err != nil {
		_ = lv.Destroy(context.Background())
		return nil, nil, fmt.Errorf("create property collector: %w", err)
	}
	return lv, pc, nil
}

func (w *TaskWatcher) teardown(lv *view.ListView, pc *property.Collector) {
	bg := context.Background()
	if pc != nil {
		_ = pc.Destroy(bg)
	}
	if lv != nil {
		_ = lv.Destroy(bg)
	}
}

// eventLoop is the core actor. It owns the pending map exclusively.
// It exits on: context cancellation, session errors, or external reset signal.
func (w *TaskWatcher) eventLoop(
	ctx context.Context,
	client *vim25.Client,
	lv *view.ListView,
	pc *property.Collector,
) (map[types.ManagedObjectReference]chan<- taskWatcherResult, error) {

	pending := make(map[types.ManagedObjectReference]chan<- taskWatcherResult)

	pf, err := pc.CreateFilter(ctx, listViewTaskFilter(lv))
	if err != nil {
		return pending, fmt.Errorf("create filter: %w", err)
	}
	defer func() { _ = pf.Destroy(context.Background()) }()

	version := ""
	var completedRefs []types.ManagedObjectReference

	for {
		// Check for external reset signal (credential rotation).
		select {
		case <-w.resetCh:
			return pending, errors.New("client reset requested")
		default:
		}

		// Step 1: batch-drain all queued commands.
		sessionErr := w.drainCommandsBatched(ctx, lv, pending)
		if sessionErr != nil {
			return pending, sessionErr
		}

		// Batch-remove completed tasks from ListView.
		if len(completedRefs) > 0 {
			_, removeErr := lv.Remove(ctx, completedRefs)
			completedRefs = completedRefs[:0]
			if removeErr != nil && isSessionError(removeErr) {
				return pending, fmt.Errorf("session error removing tasks: %w", removeErr)
			}
		}

		// Step 2: if no tasks pending, block waiting for the next command
		// or a reset signal.
		if len(pending) == 0 {
			select {
			case cmd := <-w.cmdCh:
				if sErr := w.handleSingleAdd(ctx, lv, pending, cmd); sErr != nil {
					return pending, sErr
				}
				continue
			case <-w.resetCh:
				return pending, errors.New("client reset requested")
			case <-ctx.Done():
				return pending, ctx.Err()
			}
		}

		// Step 3: poll vCenter for updates.
		newVersion, completed, pollErr := w.pollUpdates(ctx, client, pc, version, pending)
		if pollErr != nil {
			return pending, pollErr
		}
		version = newVersion
		completedRefs = append(completedRefs, completed...)
	}
}

// drainCommandsBatched collects all queued add commands and issues a single
// batched ListView.Add call. Returns a non-nil error if a session error is
// detected, which causes the event loop to exit and reconnect.
func (w *TaskWatcher) drainCommandsBatched(
	ctx context.Context,
	lv *view.ListView,
	pending map[types.ManagedObjectReference]chan<- taskWatcherResult,
) error {
	var addCmds []watcherCommand
	var removeRefs []types.ManagedObjectReference

	for {
		select {
		case cmd := <-w.cmdCh:
			switch cmd.op {
			case opAdd:
				addCmds = append(addCmds, cmd)
			case opRemove:
				if _, ok := pending[cmd.taskRef]; ok {
					removeRefs = append(removeRefs, cmd.taskRef)
					delete(pending, cmd.taskRef)
				}
				if cmd.errCh != nil {
					cmd.errCh <- nil
				}
			}
		case <-ctx.Done():
			for _, cmd := range addCmds {
				cmd.errCh <- ctx.Err()
			}
			return ctx.Err()
		default:
			goto done
		}
	}

done:
	if len(removeRefs) > 0 {
		_, removeErr := lv.Remove(ctx, removeRefs)
		if removeErr != nil && isSessionError(removeErr) {
			for _, cmd := range addCmds {
				cmd.errCh <- removeErr
			}
			return fmt.Errorf("session error removing tasks: %w", removeErr)
		}
	}

	if len(addCmds) == 0 {
		return nil
	}

	refs := make([]types.ManagedObjectReference, len(addCmds))
	for i, cmd := range addCmds {
		refs[i] = cmd.taskRef
	}

	unresolved, err := lv.Add(ctx, refs)
	if err != nil {
		for _, cmd := range addCmds {
			cmd.errCh <- err
		}
		if isSessionError(err) {
			return fmt.Errorf("session error adding tasks: %w", err)
		}
		return nil
	}

	unresolvedSet := make(map[types.ManagedObjectReference]struct{}, len(unresolved))
	for _, ref := range unresolved {
		unresolvedSet[ref] = struct{}{}
	}

	for _, cmd := range addCmds {
		if _, bad := unresolvedSet[cmd.taskRef]; bad {
			cmd.errCh <- fmt.Errorf("task %v not found in vCenter", cmd.taskRef)
		} else {
			pending[cmd.taskRef] = cmd.resultCh
			cmd.errCh <- nil
		}
	}

	return nil
}

// handleSingleAdd processes a single command when the pending map is empty.
// Returns a non-nil error if a session error occurs.
func (w *TaskWatcher) handleSingleAdd(
	ctx context.Context,
	lv *view.ListView,
	pending map[types.ManagedObjectReference]chan<- taskWatcherResult,
	cmd watcherCommand,
) error {
	if cmd.op == opRemove {
		if _, ok := pending[cmd.taskRef]; ok {
			_, err := lv.Remove(ctx, []types.ManagedObjectReference{cmd.taskRef})
			delete(pending, cmd.taskRef)
			if err != nil && isSessionError(err) {
				if cmd.errCh != nil {
					cmd.errCh <- nil
				}
				return fmt.Errorf("session error removing task: %w", err)
			}
		}
		if cmd.errCh != nil {
			cmd.errCh <- nil
		}
		return nil
	}

	unresolved, err := lv.Add(ctx, []types.ManagedObjectReference{cmd.taskRef})
	if err != nil {
		cmd.errCh <- err
		if isSessionError(err) {
			return fmt.Errorf("session error adding task: %w", err)
		}
		return nil
	}
	if len(unresolved) > 0 {
		cmd.errCh <- fmt.Errorf("task %v not found in vCenter", cmd.taskRef)
		return nil
	}
	pending[cmd.taskRef] = cmd.resultCh
	cmd.errCh <- nil
	return nil
}

func (w *TaskWatcher) pollUpdates(
	ctx context.Context,
	client *vim25.Client,
	pc *property.Collector,
	version string,
	pending map[types.ManagedObjectReference]chan<- taskWatcherResult,
) (string, []types.ManagedObjectReference, error) {

	req := types.WaitForUpdatesEx{
		This:    pc.Reference(),
		Version: version,
		Options: &types.WaitOptions{
			MaxWaitSeconds: &pollMaxWaitSeconds,
		},
	}

	res, err := methods.WaitForUpdatesEx(ctx, client, &req)
	if err != nil {
		return version, nil, err
	}

	if res.Returnval == nil {
		return version, nil, nil
	}

	newVersion := res.Returnval.Version
	var completed []types.ManagedObjectReference

	for _, fs := range res.Returnval.FilterSet {
		for _, obj := range fs.ObjectSet {
			completed = append(completed, w.dispatchUpdate(pending, obj)...)
		}
	}

	return newVersion, completed, nil
}

func (w *TaskWatcher) dispatchUpdate(
	pending map[types.ManagedObjectReference]chan<- taskWatcherResult,
	obj types.ObjectUpdate,
) []types.ManagedObjectReference {
	var completed []types.ManagedObjectReference
	for _, prop := range obj.ChangeSet {
		if prop.Name != "info" {
			continue
		}
		ti, ok := prop.Val.(types.TaskInfo)
		if !ok {
			continue
		}
		if ti.State != types.TaskInfoStateSuccess && ti.State != types.TaskInfoStateError {
			continue
		}
		ch, exists := pending[ti.Task]
		if !exists {
			continue
		}
		if ti.State == types.TaskInfoStateError {
			msg := "task failed"
			if ti.Error != nil {
				msg = ti.Error.LocalizedMessage
			}
			ch <- taskWatcherResult{Err: errors.New(msg)}
		} else {
			ch <- taskWatcherResult{Info: &ti}
		}
		delete(pending, ti.Task)
		completed = append(completed, ti.Task)
	}
	return completed
}

func (w *TaskWatcher) failAllPending(
	pending map[types.ManagedObjectReference]chan<- taskWatcherResult,
	err error,
) {
	for ref, ch := range pending {
		ch <- taskWatcherResult{Err: err}
		delete(pending, ref)
	}
}

func (w *TaskWatcher) drainAndFailCommands(err error) {
	for {
		select {
		case cmd := <-w.cmdCh:
			if cmd.op == opAdd {
				cmd.errCh <- err
			} else if cmd.errCh != nil {
				cmd.errCh <- nil
			}
		default:
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Session error detection
// ---------------------------------------------------------------------------

// isSessionError returns true if the error indicates the vCenter session is no
// longer valid: NotAuthenticated, connection reset, or similar transport errors.
func isSessionError(err error) bool {
	if err == nil {
		return false
	}
	if soap.IsSoapFault(err) {
		if _, ok := soap.ToSoapFault(err).VimFault().(types.NotAuthenticated); ok {
			return true
		}
	}
	if soap.IsVimFault(err) {
		if _, ok := soap.ToVimFault(err).(*types.NotAuthenticated); ok {
			return true
		}
	}
	// Catch transport-level failures (connection reset, EOF, etc.) that
	// indicate the session or TCP connection is gone.
	if isTransportError(err) {
		return true
	}
	return false
}

// isTransportError returns true for errors typically caused by a dropped
// connection: EOF, connection reset, broken pipe, TLS errors.
func isTransportError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, needle := range []string{
		"connection reset",
		"broken pipe",
		"EOF",
		"use of closed network connection",
		"tls:",
		"http2:",
	} {
		if containsIgnoreCase(msg, needle) {
			return true
		}
	}
	return false
}

func containsIgnoreCase(s, substr string) bool {
	// Simple ASCII-only case-insensitive check.
	if len(substr) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			a, b := s[i+j], substr[j]
			if a == b {
				continue
			}
			if a >= 'A' && a <= 'Z' {
				a += 'a' - 'A'
			}
			if b >= 'A' && b <= 'Z' {
				b += 'a' - 'A'
			}
			if a != b {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// PropertyFilter
// ---------------------------------------------------------------------------

func listViewTaskFilter(lv *view.ListView) types.CreateFilter {
	return types.CreateFilter{
		Spec: types.PropertyFilterSpec{
			ObjectSet: []types.ObjectSpec{
				{
					Obj:  lv.Reference(),
					Skip: types.NewBool(true),
					SelectSet: []types.BaseSelectionSpec{
						&types.TraversalSpec{
							Type: "ListView",
							Path: "view",
							Skip: types.NewBool(false),
						},
					},
				},
			},
			PropSet: []types.PropertySpec{
				{
					Type:    "Task",
					PathSet: []string{"info"},
				},
			},
		},
	}
}
