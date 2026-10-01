package process

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/discharge"
	"github.com/wyw14/cry-97/internal/lab"
	"github.com/wyw14/cry-97/internal/model"
)

type QualificationOutcome struct {
	Batch  model.Batch       `json:"batch"`
	Permit *discharge.Permit `json:"permit,omitempty"`
}

func (p *Plant) SubmitLabPayload(ctx context.Context, payload []byte) (QualificationOutcome, error) {
	now := p.now()
	result, decodeErr := lab.Decode(payload, now)
	outcome, applyErr := p.ApplyLabResult(ctx, result, now)
	if decodeErr != nil {
		return QualificationOutcome{}, decodeErr
	}
	if applyErr != nil {
		return QualificationOutcome{}, applyErr
	}
	return outcome, nil
}

func (p *Plant) ApplyLabResult(ctx context.Context, result model.LabResult, now time.Time) (QualificationOutcome, error) {
	if err := ctx.Err(); err != nil {
		return QualificationOutcome{}, err
	}
	if !result.Valid || result.BatchID == uuid.Nil {
		return QualificationOutcome{}, errors.New("invalid lab result cannot qualify a batch")
	}
	p.mu.Lock()
	batch, exists := p.batches[result.BatchID]
	if !exists {
		p.mu.Unlock()
		return QualificationOutcome{}, errors.New("lab result batch is not found")
	}
	updated, err := batch.ApplyQualification(result, now)
	if err != nil {
		p.mu.Unlock()
		return QualificationOutcome{}, err
	}
	p.batches[result.BatchID] = updated
	line := p.lines[updated.LineID]
	line.Stage = updated.Stage
	line.UpdatedAt = now.UTC()
	p.lines[updated.LineID] = line
	p.mu.Unlock()
	p.dwell.Observe(updated, now)
	if _, err := p.labResults.Apply(result); err != nil {
		return QualificationOutcome{}, err
	}
	permit, permitErr := p.permits.ApplyResult(result, now)
	outcome := QualificationOutcome{Batch: updated}
	if permitErr == nil {
		outcome.Permit = &permit
	} else if result.Qualified() {
		return QualificationOutcome{}, permitErr
	}
	return outcome, nil
}

func (p *Plant) Permits() []discharge.Permit {
	return p.permits.All()
}
