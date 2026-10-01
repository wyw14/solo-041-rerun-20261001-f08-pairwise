package dwell

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/wyw14/cry-97/internal/model"
)

// Limits describes how long a batch may stay in a process stage before a
// dwell alarm is raised. Per-line entries override the defaults.
type Limits struct {
	Defaults map[model.ProcessStage]time.Duration
	Lines    map[model.LineID]map[model.ProcessStage]time.Duration
}

// DefaultLimits returns the built-in dwell budget for each tracked stage.
func DefaultLimits() Limits {
	return Limits{
		Defaults: map[model.ProcessStage]time.Duration{
			model.StageReceiving:   4 * time.Hour,
			model.StageEqualizing:  6 * time.Hour,
			model.StageBiological:  12 * time.Hour,
			model.StageSettling:    8 * time.Hour,
			model.StageQualified:   24 * time.Hour,
			model.StageDischarging: 2 * time.Hour,
		},
		Lines: make(map[model.LineID]map[model.ProcessStage]time.Duration),
	}
}

// LimitFor resolves the allowed dwell duration for a line and stage. The
// second result is false when no limit is configured for the stage.
func (l Limits) LimitFor(line model.LineID, stage model.ProcessStage) (time.Duration, bool) {
	if overrides, ok := l.Lines[line]; ok {
		if limit, ok := overrides[stage]; ok {
			return limit, true
		}
	}
	limit, ok := l.Defaults[stage]
	return limit, ok
}

type limitsFile struct {
	Defaults map[string]string            `json:"defaults"`
	Lines    map[string]map[string]string `json:"lines"`
}

// LoadLimits reads per-line dwell overrides from a JSON file. An empty path
// or a missing file yields DefaultLimits; a malformed file is an error.
func LoadLimits(path string) (Limits, error) {
	limits := DefaultLimits()
	if path == "" {
		return limits, nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return limits, nil
	}
	if err != nil {
		return Limits{}, err
	}
	var raw limitsFile
	if err := json.Unmarshal(data, &raw); err != nil {
		return Limits{}, fmt.Errorf("decode dwell limits: %w", err)
	}
	for stage, value := range raw.Defaults {
		limit, err := parseLimit(model.ProcessStage(stage), value)
		if err != nil {
			return Limits{}, err
		}
		limits.Defaults[model.ProcessStage(stage)] = limit
	}
	for line, stages := range raw.Lines {
		lineID, err := model.ParseLineID(line)
		if err != nil {
			return Limits{}, fmt.Errorf("decode dwell limits: %w", err)
		}
		overrides := limits.Lines[lineID]
		if overrides == nil {
			overrides = make(map[model.ProcessStage]time.Duration)
			limits.Lines[lineID] = overrides
		}
		for stage, value := range stages {
			limit, err := parseLimit(model.ProcessStage(stage), value)
			if err != nil {
				return Limits{}, err
			}
			overrides[model.ProcessStage(stage)] = limit
		}
	}
	return limits, nil
}

func parseLimit(stage model.ProcessStage, raw string) (time.Duration, error) {
	limit, err := time.ParseDuration(raw)
	if err != nil || limit <= 0 {
		return 0, fmt.Errorf("dwell limit for stage %s must be a positive duration", stage)
	}
	return limit, nil
}
