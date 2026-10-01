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
	"github.com/wyw14/cry-97/internal/model"
)

// Entry 是某处理线当前批次在当前工艺阶段的驻留记录。
type Entry struct {
	LineID    model.LineID       `json:"line_id"`
	BatchID   uuid.UUID          `json:"batch_id"`
	Stage     model.ProcessStage `json:"stage"`
	EnteredAt time.Time          `json:"entered_at"`
	Limit     time.Duration      `json:"limit"`
	Deadline  time.Time          `json:"deadline"`
	// AlarmID 为空表示尚未超时；非空表示已经生成过一次超时告警，重启后据此恢复。
	AlarmID       *uuid.UUID `json:"alarm_id,omitempty"`
	AlarmRaisedAt *time.Time `json:"alarm_raised_at,omitempty"`
	Acknowledged  bool       `json:"acknowledged"`
}

// DueAlarm 是一次新到点的超时指派：告警身份在 Tracker 内生成并落盘，
// 保证同一段驻留只会生成一次告警。
type DueAlarm struct {
	LineID   model.LineID
	BatchID  uuid.UUID
	Stage    model.ProcessStage
	AlarmID  uuid.UUID
	RaisedAt time.Time
	Entry    Entry
}

// Status 是状态页使用的驻留视图，剩余时间按查询时刻实时计算。
type Status struct {
	LineID        model.LineID       `json:"line_id"`
	BatchID       uuid.UUID          `json:"batch_id"`
	Stage         model.ProcessStage `json:"stage"`
	EnteredAt     time.Time          `json:"entered_at"`
	Deadline      time.Time          `json:"deadline"`
	LimitMS       int64              `json:"limit_ms"`
	ElapsedMS     int64              `json:"elapsed_ms"`
	RemainingMS   int64              `json:"remaining_ms"`
	Overtime      bool               `json:"overtime"`
	Source        Source             `json:"source"`
	AlarmID       *uuid.UUID         `json:"alarm_id,omitempty"`
	AlarmRaisedAt *time.Time         `json:"alarm_raised_at,omitempty"`
	Acknowledged  bool               `json:"acknowledged"`
}

func (e Entry) status(now time.Time, source Source) Status {
	elapsed := now.Sub(e.EnteredAt)
	remaining := e.Deadline.Sub(now)
	return Status{
		LineID: e.LineID, BatchID: e.BatchID, Stage: e.Stage,
		EnteredAt: e.EnteredAt, Deadline: e.Deadline,
		LimitMS:     int64(e.Limit / time.Millisecond),
		ElapsedMS:   int64(elapsed / time.Millisecond),
		RemainingMS: int64(remaining / time.Millisecond),
		Overtime:    !now.Before(e.Deadline),
		Source:      source,
		AlarmID:     e.AlarmID, AlarmRaisedAt: e.AlarmRaisedAt, Acknowledged: e.Acknowledged,
	}
}

type persistedFile struct {
	Entries []Entry `json:"entries"`
}

// Tracker 记录各处理线当前阶段的进入时刻，落盘保证进程重启后仍按原进入时间计算。
type Tracker struct {
	mu      sync.Mutex
	path    string
	policy  Policy
	entries map[model.LineID]Entry
}

func NewTracker(path string, policy Policy) (*Tracker, error) {
	if path == "" {
		return nil, errors.New("dwell tracker path is required")
	}
	tracker := &Tracker{path: path, policy: policy, entries: make(map[model.LineID]Entry)}
	if err := tracker.load(); err != nil {
		return nil, err
	}
	return tracker, nil
}

// Enter 开始一段新的阶段驻留，并返回被替换掉的上一段驻留，调用方据此自动解除旧告警。
// 该阶段不监控（limit 为 0）时只清除既有记录。
func (t *Tracker) Enter(lineID model.LineID, batchID uuid.UUID, stage model.ProcessStage, now time.Time) (Entry, Entry, bool, error) {
	limit, tracked := t.policy.Limit(lineID, stage)
	t.mu.Lock()
	defer t.mu.Unlock()
	previous, existed := t.entries[lineID]
	if !tracked {
		if existed {
			delete(t.entries, lineID)
			if err := t.persistLocked(); err != nil {
				return Entry{}, previous, false, err
			}
		}
		return Entry{}, previous, false, nil
	}
	stamp := now.UTC()
	entry := Entry{
		LineID: lineID, BatchID: batchID, Stage: stage,
		EnteredAt: stamp, Limit: limit, Deadline: stamp.Add(limit),
	}
	t.entries[lineID] = entry
	if err := t.persistLocked(); err != nil {
		return Entry{}, previous, false, err
	}
	return entry, previous, true, nil
}

// RecordReplay 在 journal 重放结束后按事件的原始进入时间对账单条处理线：
// 以 journal 的批次/阶段/进入时间为准，但在批次与阶段一致时保留落盘记录里
// 重启前已指派的告警身份与确认态。closed 为 true 时（批次关闭或处理线隔离）
// 直接清除驻留。返回对账前记录着的告警身份（可能为空）以及对账后是否仍保留该身份。
func (t *Tracker) RecordReplay(lineID model.LineID, batchID uuid.UUID, stage model.ProcessStage, enteredAt time.Time, closed bool) (uuid.UUID, bool, error) {
	limit, tracked := t.policy.Limit(lineID, stage)
	t.mu.Lock()
	defer t.mu.Unlock()
	var previousAlarm *uuid.UUID
	if existing, ok := t.entries[lineID]; ok {
		previousAlarm = existing.AlarmID
	}
	if closed || !tracked {
		if _, exists := t.entries[lineID]; exists {
			delete(t.entries, lineID)
			if err := t.persistLocked(); err != nil {
				return uuid.Nil, false, err
			}
		}
		if previousAlarm != nil {
			return *previousAlarm, false, nil
		}
		return uuid.Nil, false, nil
	}
	stamp := enteredAt.UTC()
	entry := Entry{
		LineID: lineID, BatchID: batchID, Stage: stage,
		EnteredAt: stamp, Limit: limit, Deadline: stamp.Add(limit),
	}
	if existing, ok := t.entries[lineID]; ok && existing.BatchID == batchID && existing.Stage == stage {
		// 原驻留仍是当前驻留：沿用权威进入时间，但保留重启前的告警身份。
		entry.AlarmID = existing.AlarmID
		entry.AlarmRaisedAt = existing.AlarmRaisedAt
		entry.Acknowledged = existing.Acknowledged
	}
	t.entries[lineID] = entry
	if err := t.persistLocked(); err != nil {
		return uuid.Nil, false, err
	}
	kept := entry.AlarmID != nil && previousAlarm != nil && *entry.AlarmID == *previousAlarm
	if previousAlarm != nil {
		return *previousAlarm, kept, nil
	}
	return uuid.Nil, false, nil
}

// Close 在批次关闭或处理线隔离时停止驻留追踪，返回被清除的记录以便调用方解除告警。
func (t *Tracker) Close(lineID model.LineID) (Entry, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	previous, ok := t.entries[lineID]
	if !ok {
		return Entry{}, false, nil
	}
	delete(t.entries, lineID)
	if err := t.persistLocked(); err != nil {
		return Entry{}, false, err
	}
	return previous, true, nil
}

func (t *Tracker) Get(lineID model.LineID) (Entry, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.entries[lineID]
	return entry, ok
}

// Tick 找出已经到点且尚未指派告警的驻留，原子地分配告警身份并落盘。
// 已指派过的（包括重启前指派的）不会再次返回，保证一次驻留只告警一次。
// 传入 lines 时只评估这些处理线（用于排除已下线的处理线）；为空表示全部。
func (t *Tracker) Tick(now time.Time, lines ...model.LineID) ([]DueAlarm, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	stamp := now.UTC()
	allowed := lineSet(lines)
	due := make([]DueAlarm, 0)
	lineIDs := make([]model.LineID, 0, len(t.entries))
	for lineID := range t.entries {
		if allowed != nil && !allowed[lineID] {
			continue
		}
		lineIDs = append(lineIDs, lineID)
	}
	sort.Slice(lineIDs, func(i, j int) bool { return lineIDs[i] < lineIDs[j] })
	for _, lineID := range lineIDs {
		entry := t.entries[lineID]
		if entry.AlarmID != nil || stamp.Before(entry.Deadline) {
			continue
		}
		id := uuid.New()
		entry.AlarmID = &id
		entry.AlarmRaisedAt = &stamp
		entry.Acknowledged = false
		t.entries[lineID] = entry
		due = append(due, DueAlarm{
			LineID: lineID, BatchID: entry.BatchID, Stage: entry.Stage,
			AlarmID: id, RaisedAt: stamp, Entry: entry,
		})
	}
	if len(due) == 0 {
		return nil, nil
	}
	if err := t.persistLocked(); err != nil {
		return nil, err
	}
	return due, nil
}

// MarkAcknowledged 在告警被确认时回写确认态；找不到对应驻留时静默跳过。
func (t *Tracker) MarkAcknowledged(alarmID uuid.UUID) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	changed := false
	for lineID, entry := range t.entries {
		if entry.AlarmID != nil && *entry.AlarmID == alarmID && !entry.Acknowledged {
			entry.Acknowledged = true
			t.entries[lineID] = entry
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return t.persistLocked()
}

// Entries 按处理线排序返回全部驻留记录。
func (t *Tracker) Entries() []Entry {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]Entry, 0, len(t.entries))
	for _, entry := range t.entries {
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].LineID < result[j].LineID })
	return result
}

// View 返回状态页视图，剩余/超时时间依据 now 实时计算。
func (t *Tracker) View(lineID model.LineID, now time.Time) (Status, bool) {
	t.mu.Lock()
	entry, ok := t.entries[lineID]
	source := t.policy.Source(lineID, entry.Stage)
	t.mu.Unlock()
	if !ok {
		return Status{}, false
	}
	return entry.status(now, source), true
}

// Views 返回所有驻留的状态页视图；传入 lines 时只包含这些处理线。
func (t *Tracker) Views(now time.Time, lines ...model.LineID) []Status {
	t.mu.Lock()
	allowed := lineSet(lines)
	entries := make([]Entry, 0, len(t.entries))
	for _, entry := range t.entries {
		if allowed != nil && !allowed[entry.LineID] {
			continue
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].LineID < entries[j].LineID })
	result := make([]Status, 0, len(entries))
	for _, entry := range entries {
		result = append(result, entry.status(now, t.policy.Source(entry.LineID, entry.Stage)))
	}
	t.mu.Unlock()
	return result
}

func lineSet(lines []model.LineID) map[model.LineID]bool {
	if len(lines) == 0 {
		return nil
	}
	set := make(map[model.LineID]bool, len(lines))
	for _, lineID := range lines {
		set[lineID] = true
	}
	return set
}

func (t *Tracker) load() error {
	data, err := os.ReadFile(t.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var persisted persistedFile
	if err := json.Unmarshal(data, &persisted); err != nil {
		return fmt.Errorf("decode dwell state: %w", err)
	}
	for _, entry := range persisted.Entries {
		t.entries[entry.LineID] = entry
	}
	return nil
}

// persistLocked 调用方必须持有 t.mu。
func (t *Tracker) persistLocked() error {
	entries := make([]Entry, 0, len(t.entries))
	for _, entry := range t.entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].LineID < entries[j].LineID })
	data, err := json.MarshalIndent(persistedFile{Entries: entries}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(t.path), 0o755); err != nil {
		return err
	}
	temporary := t.path + ".next"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o640); err != nil {
		return err
	}
	return os.Rename(temporary, t.path)
}
