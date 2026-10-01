package process

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

const dwellAlarmCode = "STAGE_DWELL_TIMEOUT"

func dwellMessage(batchID uuid.UUID, stage model.ProcessStage, limit time.Duration, enteredAt time.Time) string {
	return fmt.Sprintf(
		"batch %s dwell in %s exceeded allowed %s since %s",
		batchID.String(), stage, limit, enteredAt.UTC().Format(time.RFC3339),
	)
}

// EvaluateDwell 检查所有处理线的阶段驻留：到点后为每段驻留生成一次可确认告警。
// 告警身份在 Tracker 内原子分配并落盘，重复评估或进程重启都不会重复告警。
func (p *Plant) EvaluateDwell(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	due, err := p.dwell.Tick(p.now(), p.LineIDs()...)
	if err != nil {
		return 0, err
	}
	for _, item := range due {
		alarm := model.NewAlarmWithID(
			item.AlarmID, item.LineID, dwellAlarmCode,
			dwellMessage(item.BatchID, item.Stage, item.Entry.Limit, item.Entry.EnteredAt),
			model.SeverityWarning, item.RaisedAt,
		)
		p.alarms.Ensure(alarm)
		// 极端并发下，告警落盘后、写入内存前阶段可能已推进；若驻留已不属于该告警，立即解除。
		current, tracked := p.dwell.Get(item.LineID)
		if !tracked || current.AlarmID == nil || *current.AlarmID != item.AlarmID {
			p.alarms.Resolve(item.AlarmID, p.now())
		}
	}
	return len(due), nil
}

// restoreDwellAlarmsForLine 恢复单条处理线在重启前已触发、仍未随阶段推进解除的超时告警，
// 使短暂停机既不会重置计时，也不会丢失既有告警及其确认状态。
func (p *Plant) restoreDwellAlarmsForLine(lineID model.LineID) error {
	entry, ok := p.dwell.Get(lineID)
	if !ok || entry.AlarmID == nil {
		return nil
	}
	raisedAt := p.now()
	if entry.AlarmRaisedAt != nil {
		raisedAt = *entry.AlarmRaisedAt
	}
	alarm := model.NewAlarmWithID(
		*entry.AlarmID, entry.LineID, dwellAlarmCode,
		dwellMessage(entry.BatchID, entry.Stage, entry.Limit, entry.EnteredAt),
		model.SeverityWarning, raisedAt,
	)
	if entry.Acknowledged {
		alarm = alarm.Acknowledge()
	}
	p.alarms.Ensure(alarm)
	return nil
}

// startStageDwell 开始新阶段的驻留计时，并自动解除上一阶段遗留的超时告警。
func (p *Plant) startStageDwell(lineID model.LineID, batchID uuid.UUID, stage model.ProcessStage, now time.Time) error {
	_, previous, _, err := p.dwell.Enter(lineID, batchID, stage, now)
	if err != nil {
		return err
	}
	if previous.AlarmID != nil {
		p.alarms.Resolve(*previous.AlarmID, now)
	}
	return nil
}

// clearDwell 停止某处理线的驻留追踪（批次关闭或急停隔离），并自动解除其超时告警。
func (p *Plant) clearDwell(lineID model.LineID) error {
	previous, tracked, err := p.dwell.Close(lineID)
	if err != nil {
		return err
	}
	if tracked && previous.AlarmID != nil {
		p.alarms.Resolve(*previous.AlarmID, p.now())
	}
	return nil
}
