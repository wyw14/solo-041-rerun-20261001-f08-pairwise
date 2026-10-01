package process

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/dwell"
	"github.com/wyw14/cry-97/internal/model"
)

func shortDwellPolicy(t *testing.T) dwell.Policy {
	t.Helper()
	stages := map[model.ProcessStage]time.Duration{
		model.StageReceiving:   time.Hour,
		model.StageEqualizing:  time.Hour,
		model.StageBiological:  2 * time.Hour,
		model.StageSettling:    time.Hour,
		model.StageQualified:   time.Hour,
		model.StageDischarging: time.Hour,
	}
	policy, err := dwell.NewPolicy(nil, map[model.LineID]map[model.ProcessStage]time.Duration{
		"line-1": stages,
	})
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	return policy
}

func newTestPlant(t *testing.T, clock *time.Time) *Plant {
	t.Helper()
	plant, err := NewPlant(t.TempDir(), []model.LineID{"line-1"}, shortDwellPolicy(t), func() time.Time { return clock.UTC() })
	if err != nil {
		t.Fatalf("new plant: %v", err)
	}
	if _, err := plant.RecoverLine(context.Background(), "line-1"); err != nil {
		t.Fatalf("recover: %v", err)
	}
	return plant
}

func activeDwellAlarms(plant *Plant) []model.Alarm {
	result := make([]model.Alarm, 0)
	for _, value := range plant.alarms.Active("line-1") {
		if value.Code == dwellAlarmCode {
			result = append(result, value)
		}
	}
	return result
}

func TestDwellAlarmFiresOnceAndResolvesOnAdvance(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0).UTC()
	clock := t0
	plant := newTestPlant(t, &clock)
	ctx := context.Background()

	batch, err := plant.StartBatch(ctx, "line-1", 760, "north-intake")
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}
	clock = t0.Add(30 * time.Minute)
	if n, err := plant.EvaluateDwell(ctx); err != nil || n != 0 {
		t.Fatalf("early evaluate = %d, %v", n, err)
	}
	clock = t0.Add(time.Hour + time.Second)
	if n, err := plant.EvaluateDwell(ctx); err != nil || n != 1 {
		t.Fatalf("overdue evaluate = %d, %v", n, err)
	}
	if n, err := plant.EvaluateDwell(ctx); err != nil || n != 0 {
		t.Fatalf("duplicate evaluate = %d, %v", n, err)
	}
	alarms := activeDwellAlarms(plant)
	if len(alarms) != 1 {
		t.Fatalf("active dwell alarms = %d", len(alarms))
	}
	if alarms[0].Acknowledged {
		t.Fatalf("new alarm must need acknowledgement")
	}
	// 告警可确认，确认态回写驻留。
	if _, err := plant.AcknowledgeAlarm(alarms[0].ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// 推进到 equalizing：receiving 的超时告警自动解除，新阶段重新计时。
	if _, err := plant.AdvanceBatch(ctx, "line-1", model.StageEqualizing); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if len(activeDwellAlarms(plant)) != 0 {
		t.Fatalf("dwell alarm must resolve on stage advance")
	}
	status, err := plant.Status("line-1")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Dwell == nil || status.Dwell.Stage != model.StageEqualizing ||
		!status.Dwell.EnteredAt.Equal(clock) || status.Dwell.Overtime {
		t.Fatalf("new stage dwell wrong: %+v", status.Dwell)
	}
	_ = batch
}

func TestDwellAlarmResolvesOnClose(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0).UTC()
	clock := t0
	plant := newTestPlant(t, &clock)
	ctx := context.Background()

	if _, err := plant.StartBatch(ctx, "line-1", 500, "north-intake"); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	clock = t0.Add(2 * time.Hour)
	if _, err := plant.EvaluateDwell(ctx); err != nil || len(activeDwellAlarms(plant)) != 1 {
		t.Fatalf("expected one receiving alarm")
	}
	// 急停隔离同样解除驻留告警。
	if _, err := plant.EmergencyStop(ctx, "line-1", "safety"); err != nil {
		t.Fatalf("emergency: %v", err)
	}
	if len(activeDwellAlarms(plant)) != 0 {
		t.Fatalf("dwell alarm must resolve on isolation")
	}
}

func TestDwellSurvivesRestartWithoutResettingTimer(t *testing.T) {
	dir := t.TempDir()
	t0 := time.Unix(1_700_000_000, 0).UTC()
	clock := t0
	policy := shortDwellPolicy(t)
	plant, err := NewPlant(dir, []model.LineID{"line-1"}, policy, func() time.Time { return clock.UTC() })
	if err != nil {
		t.Fatalf("new plant: %v", err)
	}
	if _, err := plant.RecoverLine(context.Background(), "line-1"); err != nil {
		t.Fatalf("recover: %v", err)
	}
	ctx := context.Background()
	if _, err := plant.StartBatch(ctx, "line-1", 600, "north-intake"); err != nil {
		t.Fatalf("start batch: %v", err)
	}

	// 停机 30 分钟后重启：尚未超时，计时必须沿用原进入时间。
	clock = t0.Add(30 * time.Minute)
	restarted, err := NewPlant(dir, []model.LineID{"line-1"}, policy, func() time.Time { return clock.UTC() })
	if err != nil {
		t.Fatalf("restart plant: %v", err)
	}
	for _, lineID := range []model.LineID{"line-1"} {
		if _, err := restarted.RecoverLine(ctx, lineID); err != nil {
			t.Fatalf("recover after restart: %v", err)
		}
	}
	status, err := restarted.Status("line-1")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Batch == nil || status.Dwell == nil {
		t.Fatalf("batch and dwell must be recovered")
	}
	if !status.Dwell.EnteredAt.Equal(t0) {
		t.Fatalf("entered time reset after restart: %v", status.Dwell.EnteredAt)
	}
	// 按原进入时间应剩余 30 分钟，而不是重启后重新给 1 小时。
	if remaining := time.Duration(status.Dwell.RemainingMS) * time.Millisecond; remaining < 29*time.Minute || remaining > 31*time.Minute {
		t.Fatalf("remaining after restart = %v", remaining)
	}

	// 越过原始 deadline，只在重启后的进程里触发一次告警。
	clock = t0.Add(time.Hour + time.Minute)
	if n, err := restarted.EvaluateDwell(ctx); err != nil || n != 1 {
		t.Fatalf("evaluate after restart = %d, %v", n, err)
	}
	alarmID := activeDwellAlarms(restarted)[0].ID
	if _, err := restarted.AcknowledgeAlarm(alarmID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// 再次重启：告警身份与确认态恢复，且不会重复告警。
	clock = t0.Add(3 * time.Hour)
	again, err := NewPlant(dir, []model.LineID{"line-1"}, policy, func() time.Time { return clock.UTC() })
	if err != nil {
		t.Fatalf("second restart plant: %v", err)
	}
	if _, err := again.RecoverLine(ctx, "line-1"); err != nil {
		t.Fatalf("recover after second restart: %v", err)
	}
	active := again.alarms.Active("line-1")
	var found *model.Alarm
	for i := range active {
		if active[i].ID == alarmID {
			found = &active[i]
		}
	}
	if found == nil {
		t.Fatalf("raised alarm must be restored after restart")
	}
	if !found.Acknowledged {
		t.Fatalf("acknowledged state must be restored")
	}
	if n, err := again.EvaluateDwell(ctx); err != nil || n != 0 {
		t.Fatalf("alarm must not re-fire after restart: %d, %v", n, err)
	}
}
