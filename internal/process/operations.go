package process

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/aeration"
	"github.com/wyw14/cry-97/internal/dosing"
	"github.com/wyw14/cry-97/internal/dwell"
	"github.com/wyw14/cry-97/internal/model"
	"github.com/wyw14/cry-97/internal/settling"
)

func (p *Plant) SubmitAeration(ctx context.Context, lineID model.LineID, basinID string, payload []byte) (aeration.Window, *model.DeviceCommand, error) {
	p.mu.RLock()
	state, ok := p.lines[lineID]
	p.mu.RUnlock()
	if !ok || state.Emergency {
		return aeration.Window{}, nil, errors.New("process line cannot accept aeration samples")
	}
	return p.aeration.SubmitWindow(ctx, lineID, basinID, payload, state.Generation, p.now())
}

func (p *Plant) ChangeDose(ctx context.Context, lineID model.LineID, batchID uuid.UUID, chemical string, rate float64) (model.DeviceCommand, error) {
	p.mu.RLock()
	state, ok := p.lines[lineID]
	p.mu.RUnlock()
	if !ok || state.Emergency {
		return model.DeviceCommand{}, errors.New("process line cannot change dosing")
	}
	change, err := dosing.NewChange(lineID, batchID, chemical, rate, state.Generation, p.now())
	if err != nil {
		return model.DeviceCommand{}, err
	}
	return p.dosing.ChangeRate(ctx, change, p.now())
}

func (p *Plant) UpdateBlanket(lineID model.LineID, level float64) (settling.Snapshot, error) {
	state, ok := p.settling[lineID]
	if !ok {
		return settling.Snapshot{}, errors.New("settling line is not found")
	}
	return state.UpdateBlanket(level, 2.8, p.now()), nil
}

func (p *Plant) ReceiveSample(ctx context.Context, sample model.Sample) error {
	if err := p.receiver.Receive(ctx, sample, p.now()); err != nil {
		return err
	}
	return p.sampler.Publish(sample)
}

func (p *Plant) BeginSettling(lineID model.LineID) (settling.Cycle, error) {
	state, ok := p.settling[lineID]
	if !ok {
		return settling.Cycle{}, errors.New("settling line is not found")
	}
	state.BeginCycle(p.now())
	return p.cycles.Start(lineID, p.now())
}

func (p *Plant) AdvanceSettling(lineID model.LineID) (settling.Cycle, error) {
	return p.cycles.Advance(lineID, p.now())
}

func (p *Plant) Alarms() []model.Alarm {
	return p.alarms.All()
}

// EvaluateDwell raises dwell alarms for batches that have overstayed their
// stage limit. It is driven by the maintenance ticker.
func (p *Plant) EvaluateDwell(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.dwell.Evaluate(p.now())
	return nil
}

// DwellStatuses reports the stage dwell budget of every tracked batch.
func (p *Plant) DwellStatuses() []dwell.Status {
	return p.dwell.Statuses(p.now())
}

func (p *Plant) StatusBlowers() []aeration.Blower {
	return p.aeration.Blowers()
}

func (p *Plant) StatusAerationWindows() []aeration.Window {
	return p.aeration.Windows()
}

func (p *Plant) AcknowledgeAlarm(id uuid.UUID) (model.Alarm, error) {
	return p.alarms.Acknowledge(id)
}

func (p *Plant) RecoverAlarm(id uuid.UUID) (model.Alarm, error) {
	return p.alarms.Recover(id, p.now())
}

func (p *Plant) Clock() time.Time {
	return p.now().UTC()
}
