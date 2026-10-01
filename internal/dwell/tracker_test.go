package dwell_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/alarm"
	"github.com/wyw14/cry-97/internal/dwell"
	"github.com/wyw14/cry-97/internal/model"
)

var start = time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)

func newTracker(t *testing.T) (*dwell.Tracker, *alarm.Service, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dwell.json")
	service := alarm.NewService()
	tracker, err := dwell.NewTracker(path, dwell.DefaultLimits(), service)
	if err != nil {
		t.Fatalf("new tracker: %v", err)
	}
	return tracker, service, path
}

func activeDwellAlarms(service *alarm.Service, line model.LineID) int {
	count := 0
	for _, value := range service.Active(line) {
		if value.Code == dwell.AlarmCode {
			count++
		}
	}
	return count
}

func TestEvaluateRaisesSingleAlarmAfterLimit(t *testing.T) {
	tracker, service, _ := newTracker(t)
	batch := model.NewBatch("line-1", start)
	tracker.Observe(batch, start)

	tracker.Evaluate(start.Add(3 * time.Hour))
	if got := activeDwellAlarms(service, "line-1"); got != 0 {
		t.Fatalf("before limit: expected no alarm, got %d", got)
	}
	tracker.Evaluate(start.Add(5 * time.Hour))
	tracker.Evaluate(start.Add(6 * time.Hour))
	if got := activeDwellAlarms(service, "line-1"); got != 1 {
		t.Fatalf("after limit: expected exactly one alarm, got %d", got)
	}
	status, ok := tracker.Status("line-1", start.Add(6*time.Hour))
	if !ok || !status.Exceeded || status.AlarmID == nil {
		t.Fatalf("expected exceeded status with alarm, got %+v (ok=%v)", status, ok)
	}
}

func TestStageAdvanceRecoversAlarmAndRestartsTimer(t *testing.T) {
	tracker, service, _ := newTracker(t)
	batch := model.NewBatch("line-1", start)
	tracker.Observe(batch, start)
	tracker.Evaluate(start.Add(5 * time.Hour))
	if got := activeDwellAlarms(service, "line-1"); got != 1 {
		t.Fatalf("expected alarm before advance, got %d", got)
	}
	advanced, err := batch.Move(model.StageEqualizing, start.Add(5*time.Hour))
	if err != nil {
		t.Fatalf("move batch: %v", err)
	}
	tracker.Observe(advanced, start.Add(5*time.Hour))
	if got := activeDwellAlarms(service, "line-1"); got != 0 {
		t.Fatalf("expected alarm recovered after advance, got %d active", got)
	}
	status, ok := tracker.Status("line-1", start.Add(6*time.Hour))
	if !ok || status.Stage != model.StageEqualizing || status.Exceeded {
		t.Fatalf("expected fresh equalizing record, got %+v (ok=%v)", status, ok)
	}
}

func TestClosedBatchIsUntrackedAndRecovered(t *testing.T) {
	tracker, service, _ := newTracker(t)
	batch := model.NewBatch("line-1", start)
	tracker.Observe(batch, start)
	tracker.Evaluate(start.Add(5 * time.Hour))
	closed := batch
	closed.Stage = model.StageClosed
	tracker.Observe(closed, start.Add(6*time.Hour))
	if got := activeDwellAlarms(service, "line-1"); got != 0 {
		t.Fatalf("expected alarm recovered after close, got %d active", got)
	}
	if _, ok := tracker.Status("line-1", start.Add(6*time.Hour)); ok {
		t.Fatal("expected closed batch to be untracked")
	}
}

func TestSuspendLineDropsRecords(t *testing.T) {
	tracker, service, _ := newTracker(t)
	batch := model.NewBatch("line-1", start)
	tracker.Observe(batch, start)
	tracker.Evaluate(start.Add(5 * time.Hour))
	tracker.SuspendLine("line-1", start.Add(6*time.Hour))
	if got := activeDwellAlarms(service, "line-1"); got != 0 {
		t.Fatalf("expected alarm recovered after suspend, got %d active", got)
	}
	if _, ok := tracker.Status("line-1", start.Add(6*time.Hour)); ok {
		t.Fatal("expected suspended line to be untracked")
	}
}

func TestRestartKeepsOriginalEntryTimeAndAlarm(t *testing.T) {
	tracker, service, path := newTracker(t)
	batch := model.NewBatch("line-1", start)
	tracker.Observe(batch, start)
	tracker.Evaluate(start.Add(5 * time.Hour))
	if got := activeDwellAlarms(service, "line-1"); got != 1 {
		t.Fatalf("expected alarm before restart, got %d", got)
	}

	// Simulate a restart: a fresh alarm service and tracker over the same file.
	restartedService := alarm.NewService()
	restarted, err := dwell.NewTracker(path, dwell.DefaultLimits(), restartedService)
	if err != nil {
		t.Fatalf("reload tracker: %v", err)
	}
	// Downtime must not reset the timer: the original entry time still applies.
	status, ok := restarted.Status("line-1", start.Add(9*time.Hour))
	if !ok || !status.EnteredAt.Equal(start) || !status.Exceeded {
		t.Fatalf("expected original entry time preserved, got %+v (ok=%v)", status, ok)
	}
	// The previously raised alarm is restored, not duplicated.
	if got := activeDwellAlarms(restartedService, "line-1"); got != 1 {
		t.Fatalf("expected restored alarm after restart, got %d", got)
	}
	restarted.Evaluate(start.Add(9 * time.Hour))
	if got := activeDwellAlarms(restartedService, "line-1"); got != 1 {
		t.Fatalf("expected no duplicate alarm after restart, got %d", got)
	}
}

func TestLineOverrideTakesPrecedence(t *testing.T) {
	limits := dwell.DefaultLimits()
	limits.Lines["line-1"] = map[model.ProcessStage]time.Duration{model.StageReceiving: time.Minute}
	tracker, err := dwell.NewTracker(filepath.Join(t.TempDir(), "dwell.json"), limits, alarm.NewService())
	if err != nil {
		t.Fatalf("new tracker: %v", err)
	}
	batch := model.NewBatch("line-1", start)
	tracker.Observe(batch, start)
	status, ok := tracker.Status("line-1", start)
	if !ok || status.Limit != time.Minute {
		t.Fatalf("expected per-line override, got %+v (ok=%v)", status, ok)
	}
	if _, ok := limits.LimitFor("line-2", model.StageReceiving); !ok {
		t.Fatal("expected default limit to apply to other lines")
	}
}

func TestLoadLimitsFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "limits.json")
	content := []byte(`{"defaults":{"receiving":"90m"},"lines":{"line-3":{"biological":"8h"}}}`)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write limits: %v", err)
	}
	limits, err := dwell.LoadLimits(path)
	if err != nil {
		t.Fatalf("load limits: %v", err)
	}
	if limit, _ := limits.LimitFor("line-1", model.StageReceiving); limit != 90*time.Minute {
		t.Fatalf("expected default override 90m, got %s", limit)
	}
	if limit, _ := limits.LimitFor("line-3", model.StageBiological); limit != 8*time.Hour {
		t.Fatalf("expected line override 8h, got %s", limit)
	}
	if limit, _ := limits.LimitFor("line-3", model.StageSettling); limit != 8*time.Hour {
		t.Fatalf("expected built-in settling default 8h, got %s", limit)
	}
	if _, err := dwell.LoadLimits(filepath.Join(t.TempDir(), "missing.json")); err != nil {
		t.Fatalf("missing file should yield defaults: %v", err)
	}
}
