package dwell

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/alarm"
	"github.com/wyw14/cry-97/internal/model"
)

// AlarmCode identifies a stage dwell timeout alarm.
const AlarmCode = "STAGE_DWELL_EXCEEDED"

// Record tracks how long a batch has been sitting in its current stage. The
// raised alarm is persisted alongside so a restart neither resets the timer
// nor raises a duplicate.
type Record struct {
	BatchID   uuid.UUID          `json:"batch_id"`
	LineID    model.LineID       `json:"line_id"`
	Stage     model.ProcessStage `json:"stage"`
	EnteredAt time.Time          `json:"entered_at"`
	Alarm     *model.Alarm       `json:"alarm,omitempty"`
}

// Status is the live view of a batch's dwell budget for the status page.
type Status struct {
	LineID    model.LineID       `json:"line_id"`
	BatchID   uuid.UUID          `json:"batch_id"`
	Stage     model.ProcessStage `json:"stage"`
	EnteredAt time.Time          `json:"entered_at"`
	HasLimit  bool               `json:"has_limit"`
	Limit     time.Duration      `json:"limit_ns"`
	Elapsed   time.Duration      `json:"elapsed_ns"`
	Remaining time.Duration      `json:"remaining_ns"`
	Exceeded  bool               `json:"exceeded"`
	AlarmID   *uuid.UUID         `json:"alarm_id,omitempty"`
}

// Tracker observes batch stage dwell times, raises a single confirmable
// alarm when a stage limit is exceeded, and clears it again when the batch
// advances or closes. State is persisted after every change so restarts
// continue from the original stage entry time.
type Tracker struct {
	mu      sync.Mutex
	limits  Limits
	alarms  *alarm.Service
	path    string
	records map[uuid.UUID]*Record
}

// NewTracker loads previously persisted records from path and re-registers
// any still-active dwell alarms with the alarm service.
func NewTracker(path string, limits Limits, alarms *alarm.Service) (*Tracker, error) {
	if path == "" || alarms == nil {
		return nil, errors.New("dwell tracker requires a state path and alarm service")
	}
	tracker := &Tracker{
		limits: limits, alarms: alarms, path: path,
		records: make(map[uuid.UUID]*Record),
	}
	if err := tracker.load(); err != nil {
		return nil, err
	}
	return tracker, nil
}

// Observe records the stage a batch currently occupies. A stage change
// recovers the previous dwell alarm and restarts the timer; a closed or
// isolated batch is dropped from tracking.
func (t *Tracker) Observe(batch model.Batch, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	record, tracked := t.records[batch.ID]
	if !batch.IsActive() {
		if tracked {
			t.recoverLocked(record, now)
			delete(t.records, batch.ID)
			t.persistLocked()
		}
		return
	}
	if tracked && record.Stage == batch.Stage {
		return
	}
	if tracked {
		t.recoverLocked(record, now)
	}
	t.records[batch.ID] = &Record{
		BatchID: batch.ID, LineID: batch.LineID, Stage: batch.Stage,
		EnteredAt: now.UTC(),
	}
	t.persistLocked()
}

// SuspendLine drops every tracked batch on a line and recovers its dwell
// alarms, e.g. when the line is isolated by an emergency stop.
func (t *Tracker) SuspendLine(lineID model.LineID, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	changed := false
	for id, record := range t.records {
		if record.LineID != lineID {
			continue
		}
		t.recoverLocked(record, now)
		delete(t.records, id)
		changed = true
	}
	if changed {
		t.persistLocked()
	}
}

// Evaluate raises one alarm per record whose stage dwell has exceeded the
// configured limit. Records that already raised an alarm are skipped.
func (t *Tracker) Evaluate(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	changed := false
	for _, record := range t.records {
		if record.Alarm != nil {
			continue
		}
		limit, ok := t.limits.LimitFor(record.LineID, record.Stage)
		if !ok || now.Sub(record.EnteredAt) <= limit {
			continue
		}
		raised := model.NewAlarm(record.LineID, AlarmCode, describe(record, limit, now), model.SeverityWarning, now)
		t.alarms.Raise(raised)
		record.Alarm = &raised
		changed = true
	}
	if changed {
		t.persistLocked()
	}
}

// Status returns the dwell status of the batch currently tracked on a line.
func (t *Tracker) Status(lineID model.LineID, now time.Time) (Status, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, record := range t.records {
		if record.LineID == lineID {
			return t.statusLocked(record, now), true
		}
	}
	return Status{}, false
}

// Statuses returns the dwell status of every tracked batch, ordered by line.
func (t *Tracker) Statuses(now time.Time) []Status {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]Status, 0, len(t.records))
	for _, record := range t.records {
		result = append(result, t.statusLocked(record, now))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].LineID < result[j].LineID })
	return result
}

func (t *Tracker) statusLocked(record *Record, now time.Time) Status {
	elapsed := now.UTC().Sub(record.EnteredAt)
	status := Status{
		LineID: record.LineID, BatchID: record.BatchID, Stage: record.Stage,
		EnteredAt: record.EnteredAt, Elapsed: elapsed,
	}
	if limit, ok := t.limits.LimitFor(record.LineID, record.Stage); ok {
		status.HasLimit = true
		status.Limit = limit
		status.Remaining = limit - elapsed
		status.Exceeded = status.Remaining < 0
	}
	if record.Alarm != nil && record.Alarm.Active() {
		id := record.Alarm.ID
		status.AlarmID = &id
	}
	return status
}

func (t *Tracker) recoverLocked(record *Record, now time.Time) {
	if record.Alarm == nil {
		return
	}
	if recovered, err := t.alarms.Recover(record.Alarm.ID, now); err == nil {
		record.Alarm = &recovered
	}
}

func (t *Tracker) load() error {
	data, err := os.ReadFile(t.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var records []Record
	if err := json.Unmarshal(data, &records); err != nil {
		return fmt.Errorf("decode dwell state: %w", err)
	}
	for index := range records {
		record := records[index]
		if record.BatchID == uuid.Nil || record.LineID == "" || record.Stage == "" {
			return fmt.Errorf("decode dwell state: record %d is incomplete", index)
		}
		stored := record
		if stored.Alarm != nil && stored.Alarm.Active() {
			t.alarms.Raise(*stored.Alarm)
		}
		t.records[stored.BatchID] = &stored
	}
	return nil
}

func (t *Tracker) persistLocked() {
	records := make([]Record, 0, len(t.records))
	for _, record := range t.records {
		records = append(records, *record)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].EnteredAt.Before(records[j].EnteredAt) })
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(t.path), 0o755); err != nil {
		return
	}
	temporary := t.path + ".next"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o640); err != nil {
		return
	}
	_ = os.Rename(temporary, t.path)
}

func describe(record *Record, limit time.Duration, now time.Time) string {
	return fmt.Sprintf("batch %s stalled in %s on %s: entered %s, limit %s, elapsed %s",
		record.BatchID, record.Stage, record.LineID,
		record.EnteredAt.Format(time.RFC3339), limit, now.Sub(record.EnteredAt).Round(time.Second))
}
