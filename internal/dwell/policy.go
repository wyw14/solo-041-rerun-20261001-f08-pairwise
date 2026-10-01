package dwell

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/wyw14/cry-97/internal/model"
)

// DefaultDurations 是未显式配置的处理线使用的工艺阶段允许停留时长。
var DefaultDurations = map[model.ProcessStage]time.Duration{
	model.StageReceiving:   2 * time.Hour,
	model.StageEqualizing:  3 * time.Hour,
	model.StageBiological:  12 * time.Hour,
	model.StageSettling:    4 * time.Hour,
	model.StageQualified:   6 * time.Hour,
	model.StageDischarging: 2 * time.Hour,
}

// Policy 按“处理线 + 工艺阶段”决定允许停留时长；处理线没有专属配置时回落到默认值。
// 零值表示该阶段不做停留监控。
type Policy struct {
	defaults map[model.ProcessStage]time.Duration
	lines    map[model.LineID]map[model.ProcessStage]time.Duration
}

func NewPolicy(defaults map[model.ProcessStage]time.Duration, overrides map[model.LineID]map[model.ProcessStage]time.Duration) (Policy, error) {
	policy := Policy{
		defaults: make(map[model.ProcessStage]time.Duration, len(DefaultDurations)),
		lines:    make(map[model.LineID]map[model.ProcessStage]time.Duration),
	}
	for stage, duration := range DefaultDurations {
		policy.defaults[stage] = duration
	}
	for stage, duration := range defaults {
		if !isTracked(stage) {
			return Policy{}, fmt.Errorf("dwell duration for stage %s is not supported", stage)
		}
		if duration < 0 {
			return Policy{}, fmt.Errorf("dwell duration for stage %s must not be negative", stage)
		}
		policy.defaults[stage] = duration
	}
	for lineID, stages := range overrides {
		if lineID == "" || len(stages) == 0 {
			return Policy{}, errors.New("dwell line override requires a line and at least one stage")
		}
		values := make(map[model.ProcessStage]time.Duration, len(stages))
		for stage, duration := range stages {
			if !isTracked(stage) {
				return Policy{}, fmt.Errorf("dwell duration for line %s stage %s is not supported", lineID, stage)
			}
			if duration < 0 {
				return Policy{}, fmt.Errorf("dwell duration for line %s stage %s must not be negative", lineID, stage)
			}
			values[stage] = duration
		}
		policy.lines[lineID] = values
	}
	return policy, nil
}

// Limit 返回某处理线某阶段的允许停留时长；第二返回值为 false 表示该阶段不监控。
func (p Policy) Limit(lineID model.LineID, stage model.ProcessStage) (time.Duration, bool) {
	if stages, ok := p.lines[lineID]; ok {
		if duration, ok := stages[stage]; ok {
			return duration, duration > 0
		}
	}
	duration, ok := p.defaults[stage]
	return duration, ok && duration > 0
}

// Source 记录时长的配置来源，供状态页说明“触发依据”。
type Source string

const (
	SourceDefault Source = "default"
	SourceLine    Source = "line_override"
)

func (p Policy) Source(lineID model.LineID, stage model.ProcessStage) Source {
	if stages, ok := p.lines[lineID]; ok {
		if _, ok := stages[stage]; ok {
			return SourceLine
		}
	}
	return SourceDefault
}

func isTracked(stage model.ProcessStage) bool {
	_, ok := DefaultDurations[stage]
	return ok
}

// policyConfig 是 BIOTREAT_STAGE_TIMEOUTS 的 JSON 结构：
// {"defaults":{"settling":"3h"},"lines":{"line-1":{"biological":"10h"}}}
type policyConfig struct {
	Defaults map[string]string            `json:"defaults"`
	Lines    map[string]map[string]string `json:"lines"`
}

// ParsePolicy 解析阶段停留时长配置；raw 为空时返回仅含默认值的策略。
func ParsePolicy(raw string) (Policy, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return NewPolicy(nil, nil)
	}
	var decoded policyConfig
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return Policy{}, fmt.Errorf("decode stage dwell config: %w", err)
	}
	defaults := make(map[model.ProcessStage]time.Duration, len(decoded.Defaults))
	for stage, value := range decoded.Defaults {
		duration, err := time.ParseDuration(value)
		if err != nil {
			return Policy{}, fmt.Errorf("default dwell for stage %s: %w", stage, err)
		}
		defaults[model.ProcessStage(stage)] = duration
	}
	overrides := make(map[model.LineID]map[model.ProcessStage]time.Duration, len(decoded.Lines))
	for lineID, stages := range decoded.Lines {
		values := make(map[model.ProcessStage]time.Duration, len(stages))
		for stage, value := range stages {
			duration, err := time.ParseDuration(value)
			if err != nil {
				return Policy{}, fmt.Errorf("line %s dwell for stage %s: %w", lineID, stage, err)
			}
			values[model.ProcessStage(stage)] = duration
		}
		overrides[model.LineID(lineID)] = values
	}
	return NewPolicy(defaults, overrides)
}
