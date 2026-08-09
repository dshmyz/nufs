// Package testutil provides deterministic crash and fault injection for
// the V2 storage engine. The reference-model test (phase 1) and the
// crash matrix (phase 3) drive these to verify that no acknowledged
// write is lost and no corrupt data is returned across arbitrary
// interruption points.
package testutil

import (
	"errors"
	"fmt"
	"sync"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
)

// ErrSimulatedCrash is returned by fault injectors to abort an op at a
// crash point, simulating the process dying mid-transaction.
var ErrSimulatedCrash = errors.New("testutil: simulated crash")

// CrashPoint is re-exported from the storage package so test scripts
// can name stages without importing storage.
type CrashPoint = storage.CrashPoint

// FaultInjector is the interface the storage engine consults at each
// durable stage. Returning an error simulates the fault; setting
// Crash to true simulates a process crash (recovery must run next).
type FaultInjector interface {
	// OnStage is called before the named stage executes. A nil return
	// lets the stage proceed. Returning an error aborts the op with
	// that error (the engine must leave no partial durable state that
	// breaks the invariants).
	OnStage(point storage.CrashPoint) error
}

// ScriptedFaults is a deterministic fault injector driven by a fixed
// script of (stage, error, crash) triples. Once the script is
// exhausted it is a no-op, so tests can replay the same sequence.
type ScriptedFaults struct {
	mu      sync.Mutex
	steps   []Step
	next    int
	stopped bool
}

// Step is one scripted fault.
type Step struct {
	Point storage.CrashPoint
	// Err, if non-nil, is returned by OnStage.
	Err error
	// Crash, if true, records that a crash occurred at this point.
	Crash bool
}

// NewScriptedFaults returns an injector that fires the given steps in
// order.
func NewScriptedFaults(steps []Step) *ScriptedFaults {
	return &ScriptedFaults{steps: steps}
}

// OnStage consumes the next matching step.
func (s *ScriptedFaults) OnStage(point storage.CrashPoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next >= len(s.steps) {
		return nil
	}
	step := s.steps[s.next]
	if step.Point != point {
		// Not the scripted point: allow. (Strict tests should use a
		// point-specific injector; this loose matching keeps the
		// reference model simple.)
		return nil
	}
	s.next++
	if step.Crash {
		s.stopped = true
	}
	return step.Err
}

// Stopped reports whether the scripted sequence hit a crash.
func (s *ScriptedFaults) Stopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopped
}

var _ = fmt.Sprintf
