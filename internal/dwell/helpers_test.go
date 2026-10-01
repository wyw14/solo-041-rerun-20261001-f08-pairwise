package dwell

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

func filepathJoin(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "dwell-state.json")
}

func mustPolicy(t *testing.T, overrides map[model.LineID]map[model.ProcessStage]time.Duration) Policy {
	t.Helper()
	policy, err := NewPolicy(nil, overrides)
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	return policy
}

func batchID(seed byte) uuid.UUID {
	var id uuid.UUID
	for i := range id {
		id[i] = seed
	}
	return id
}
