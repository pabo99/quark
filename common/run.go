package common

import (
	"math/rand"
	"sync/atomic"
	"time"
)

var (
	attemptID uint64
)

func init() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	attemptID = uint64(r.Int63())
}

// An omegaUp run.
type Run struct {
	AttemptID uint64  `json:"attempt_id"`
	Source    string  `json:"source"`
	Language  string  `json:"language"`
	InputHash string  `json:"input_hash"`
	MaxScore  float64 `json:"max_score"`
}

// NewAttemptID allocates a locally-unique AttemptID. A counter is initialized
// to a random 63-bit integer on startup and then atomically incremented eacn
// time a new ID is needed.
func NewAttemptID() uint64 {
	return atomic.AddUint64(&attemptID, 1)
}

// UpdateID assigns a new AttemptID to a run.
func (run *Run) UpdateAttemptID() uint64 {
	run.AttemptID = NewAttemptID()
	return run.AttemptID
}
