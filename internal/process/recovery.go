package process

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

// dwellReplayPoint 记录重放过程中某处理线最后一次批次驻留状态；
// 重放结束后才与落盘驻留一次性对账，避免历史批次中途覆盖当前批次的告警身份。
type dwellReplayPoint struct {
	batchID   uuid.UUID
	stage     model.ProcessStage
	enteredAt time.Time
	closed    bool
}

func (p *Plant) RecoverLine(ctx context.Context, lineID model.LineID) (uint64, error) {
	if snapshot, found, err := p.snapshots.Load(lineID); err != nil {
		return 0, err
	} else if found {
		p.mu.Lock()
		p.lines[lineID] = snapshot
		p.mu.Unlock()
	}
	p.mu.Lock()
	delete(p.recoveryDwell, lineID)
	p.mu.Unlock()
	replayer, err := journal.NewReplayer(p.store, p)
	if err != nil {
		return 0, err
	}
	last, err := replayer.ReplayLine(ctx, lineID)
	if err != nil {
		return last, err
	}
	p.mu.Lock()
	point, pending := p.recoveryDwell[lineID]
	delete(p.recoveryDwell, lineID)
	p.mu.Unlock()
	if pending {
		// 以 journal 的原始进入时间对账；同批次同阶段保留重启前已指派的告警身份，
		// 短暂停机不会重置计时。
		alarmID, kept, err := p.dwell.RecordReplay(lineID, point.batchID, point.stage, point.enteredAt, point.closed)
		if err != nil {
			return last, err
		}
		if kept {
			if err := p.restoreDwellAlarmsForLine(lineID); err != nil {
				return last, err
			}
		} else if alarmID != uuid.Nil {
			// journal 已推进到新批次/新阶段或关闭：重启前的超时告警不应再保留。
			p.alarms.Resolve(alarmID, p.now())
		}
	} else if stale, ok := p.dwell.Get(lineID); ok {
		// journal 中没有任何批次事件，落盘驻留是上次遗留，清除并解除其告警。
		if _, _, err := p.dwell.Close(lineID); err != nil {
			return last, err
		}
		if stale.AlarmID != nil {
			p.alarms.Resolve(*stale.AlarmID, p.now())
		}
	}
	return last, nil
}

func (p *Plant) ApplyRecovered(ctx context.Context, event model.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	state, ok := p.lines[event.LineID]
	if !ok {
		return errors.New("recovery line is not configured")
	}
	if event.Sequence <= state.LastEventSeq {
		return nil
	}
	if state.Emergency && event.Generation < state.Generation {
		state.LastEventSeq = event.Sequence
		p.lines[event.LineID] = state
		return nil
	}
	if event.IsTerminal() {
		var record EmergencyRecord
		if err := event.Decode(&record); err != nil {
			return err
		}
		if record.LineID != event.LineID || record.Generation != event.Generation {
			return errors.New("emergency recovery payload does not match event identity")
		}
		state = state.Isolate(event.Sequence, event.OccurredAt)
		p.lines[event.LineID] = state
		p.devices.StopLine(event.LineID, state.Generation)
		p.recoveryDwell[event.LineID] = dwellReplayPoint{
			stage: model.StageIsolated, enteredAt: event.OccurredAt, closed: true,
		}
		state.LastEventSeq = event.Sequence
		p.lines[event.LineID] = state
		return nil
	}
	if event.Kind == model.EventDeviceCommanded {
		var command model.DeviceCommand
		if err := event.Decode(&command); err != nil {
			return err
		}
		if err := p.devices.Publish(ctx, command); err != nil {
			return err
		}
	}
	switch event.Kind {
	case model.EventBatchCreated, model.EventStageChanged:
		var batch model.Batch
		if err := event.Decode(&batch); err != nil {
			return err
		}
		// 重建内存中的批次索引，使重启后仍可继续推进与查询该批次。
		p.batches[batch.ID] = batch
		if batch.IsActive() {
			p.lineBatches[event.LineID] = batch.ID
		}
		// 仅记录候选，重放结束后由 RecoverLine 一次性对账。
		p.recoveryDwell[event.LineID] = dwellReplayPoint{
			batchID: batch.ID, stage: batch.Stage,
			enteredAt: event.OccurredAt, closed: batch.Stage == model.StageClosed,
		}
	}
	state.LastEventSeq = event.Sequence
	state.UpdatedAt = event.OccurredAt
	p.lines[event.LineID] = state
	return nil
}
