package dwell

import (
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/model"
)

func TestPolicyDefaultsAndOverrides(t *testing.T) {
	policy, err := NewPolicy(
		map[model.ProcessStage]time.Duration{model.StageSettling: 90 * time.Minute},
		map[model.LineID]map[model.ProcessStage]time.Duration{
			"line-1": {model.StageBiological: 8 * time.Hour},
		},
	)
	if err != nil {
		t.Fatalf("new policy: %v", err)
	}
	if limit, ok := policy.Limit("line-1", model.StageBiological); !ok || limit != 8*time.Hour {
		t.Fatalf("line override = %v, %v", limit, ok)
	}
	if policy.Source("line-1", model.StageBiological) != SourceLine {
		t.Fatalf("expected line override source")
	}
	if limit, ok := policy.Limit("line-2", model.StageBiological); !ok || limit != 12*time.Hour {
		t.Fatalf("default biological = %v, %v", limit, ok)
	}
	if limit, ok := policy.Limit("line-2", model.StageSettling); !ok || limit != 90*time.Minute {
		t.Fatalf("custom default settling = %v, %v", limit, ok)
	}
	if _, ok := policy.Limit("line-2", model.StageClosed); ok {
		t.Fatalf("closed stage must not be tracked")
	}
}

func TestParsePolicy(t *testing.T) {
	policy, err := ParsePolicy(`{"defaults":{"settling":"3h"},"lines":{"line-9":{"receiving":"45m","biological":"0s"}}}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if limit, ok := policy.Limit("line-x", model.StageSettling); !ok || limit != 3*time.Hour {
		t.Fatalf("parsed default = %v, %v", limit, ok)
	}
	if _, ok := policy.Limit("line-9", model.StageBiological); ok {
		t.Fatalf("zero duration disables tracking")
	}
	if _, err := ParsePolicy(`{"defaults":{"receiving":"nope"}}`); err == nil {
		t.Fatalf("expected invalid duration error")
	}
	if _, err := ParsePolicy(`{bad`); err == nil {
		t.Fatalf("expected malformed JSON error")
	}
}

func TestTrackerTickFiresOnceAndSurvivesReload(t *testing.T) {
	path := filepathJoin(t)
	policy := mustPolicy(t, map[model.LineID]map[model.ProcessStage]time.Duration{
		"line-1": {model.StageReceiving: time.Hour},
	})
	tracker, err := NewTracker(path, policy)
	if err != nil {
		t.Fatalf("new tracker: %v", err)
	}
	t0 := time.Unix(1_700_000_000, 0).UTC()
	entry, _, tracked, err := tracker.Enter("line-1", batchID(1), model.StageReceiving, t0)
	if err != nil || !tracked {
		t.Fatalf("enter: %v tracked=%v", err, tracked)
	}
	if !entry.Deadline.Equal(t0.Add(time.Hour)) {
		t.Fatalf("deadline = %v", entry.Deadline)
	}
	// 未到点不产生告警。
	if due, err := tracker.Tick(t0.Add(30 * time.Minute)); err != nil || len(due) != 0 {
		t.Fatalf("early tick = %v, %v", due, err)
	}
	// 到点产生一次指派。
	due, err := tracker.Tick(t0.Add(time.Hour + time.Second))
	if err != nil || len(due) != 1 {
		t.Fatalf("due tick = %d items, %v", len(due), err)
	}
	alarmID := due[0].AlarmID
	// 再次评估不会重复指派。
	if due, err := tracker.Tick(t0.Add(2 * time.Hour)); err != nil || len(due) != 0 {
		t.Fatalf("repeat tick = %d items, %v", len(due), err)
	}
	if err := tracker.MarkAcknowledged(alarmID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// 重新打开 Tracker（模拟进程重启）：进入时间与告警身份都沿用落盘值。
	reloaded, err := NewTracker(path, policy)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got, ok := reloaded.Get("line-1")
	if !ok {
		t.Fatalf("entry lost after reload")
	}
	if !got.EnteredAt.Equal(t0) {
		t.Fatalf("entered_at reset after reload: %v", got.EnteredAt)
	}
	if got.AlarmID == nil || *got.AlarmID != alarmID {
		t.Fatalf("alarm identity lost after reload: %+v", got.AlarmID)
	}
	if !got.Acknowledged {
		t.Fatalf("acknowledged state lost after reload")
	}
	if due, err := reloaded.Tick(t0.Add(3 * time.Hour)); err != nil || len(due) != 0 {
		t.Fatalf("tick after reload must not re-fire: %d, %v", len(due), err)
	}
	status, ok := reloaded.View("line-1", t0.Add(2*time.Hour))
	if !ok || !status.Overtime || status.RemainingMS >= 0 {
		t.Fatalf("overtime view wrong: %+v", status)
	}
}

func TestTrackerStageAdvanceAndClose(t *testing.T) {
	path := filepathJoin(t)
	policy := mustPolicy(t, map[model.LineID]map[model.ProcessStage]time.Duration{
		"line-1": {
			model.StageReceiving:  time.Hour,
			model.StageBiological: 2 * time.Hour,
		},
	})
	tracker, err := NewTracker(path, policy)
	if err != nil {
		t.Fatalf("new tracker: %v", err)
	}
	t0 := time.Unix(1_700_000_000, 0).UTC()
	if _, _, _, err := tracker.Enter("line-1", batchID(7), model.StageReceiving, t0); err != nil {
		t.Fatalf("enter receiving: %v", err)
	}
	due, err := tracker.Tick(t0.Add(2 * time.Hour))
	if err != nil || len(due) != 1 {
		t.Fatalf("receiving due = %d, %v", len(due), err)
	}
	// 推进到新阶段：返回旧驻留供调用方解除告警，并以新进入时间重新计时。
	_, previous, tracked, err := tracker.Enter("line-1", batchID(7), model.StageBiological, t0.Add(2*time.Hour))
	if err != nil || !tracked {
		t.Fatalf("enter biological: %v tracked=%v", err, tracked)
	}
	if previous.AlarmID == nil || *previous.AlarmID != due[0].AlarmID {
		t.Fatalf("previous entry must carry the raised alarm")
	}
	if got, ok := tracker.Get("line-1"); !ok || got.AlarmID != nil || !got.EnteredAt.Equal(t0.Add(2*time.Hour)) {
		t.Fatalf("new stage not reset cleanly: %+v ok=%v", got, ok)
	}
	if previous, tracked, err := tracker.Close("line-1"); err != nil || !tracked || previous.Stage != model.StageBiological {
		t.Fatalf("close = %+v tracked=%v err=%v", previous, tracked, err)
	}
	if _, ok := tracker.Get("line-1"); ok {
		t.Fatalf("entry must be gone after close")
	}
}

func TestTrackerRecordReplayReconciliation(t *testing.T) {
	path := filepathJoin(t)
	policy := mustPolicy(t, map[model.LineID]map[model.ProcessStage]time.Duration{
		"line-1": {model.StageBiological: 10 * time.Hour},
	})
	tracker, err := NewTracker(path, policy)
	if err != nil {
		t.Fatalf("new tracker: %v", err)
	}
	t0 := time.Unix(1_700_000_000, 0).UTC()
	if _, _, _, err := tracker.Enter("line-1", batchID(1), model.StageBiological, t0); err != nil {
		t.Fatal(err)
	}
	due, err := tracker.Tick(t0.Add(11 * time.Hour))
	if err != nil || len(due) != 1 {
		t.Fatalf("tick = %d, %v", len(due), err)
	}

	// journal 重放同一批次/阶段：以事件时间为准，但保留已指派的告警身份。
	journalTime := t0.Add(time.Minute)
	if _, kept, err := tracker.RecordReplay("line-1", batchID(1), model.StageBiological, journalTime, false); err != nil || !kept {
		t.Fatalf("record replay: kept=%v err=%v", kept, err)
	}
	got, _ := tracker.Get("line-1")
	if !got.EnteredAt.Equal(journalTime) {
		t.Fatalf("entered time should follow journal: %v", got.EnteredAt)
	}
	if got.AlarmID == nil || *got.AlarmID != due[0].AlarmID {
		t.Fatalf("alarm identity must survive reconciliation")
	}

	// journal 显示已推进到新批次：旧告警身份必须清除。
	if prior, kept, err := tracker.RecordReplay("line-1", batchID(2), model.StageBiological, t0.Add(12*time.Hour), false); err != nil || kept || prior != due[0].AlarmID {
		t.Fatalf("record replay new batch: prior=%v kept=%v err=%v", prior, kept, err)
	}
	got, _ = tracker.Get("line-1")
	if got.BatchID != batchID(2) || got.AlarmID != nil {
		t.Fatalf("new batch must reset alarm: %+v", got)
	}

	// journal 显示批次关闭：驻留被清除。
	if _, _, err := tracker.RecordReplay("line-1", batchID(2), model.StageClosed, t0.Add(20*time.Hour), true); err != nil {
		t.Fatalf("record replay close: %v", err)
	}
	if _, ok := tracker.Get("line-1"); ok {
		t.Fatalf("closed batch must clear dwell entry")
	}
}
