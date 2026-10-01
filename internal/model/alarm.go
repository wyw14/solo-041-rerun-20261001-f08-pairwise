package model

import (
	"time"

	"github.com/google/uuid"
)

type AlarmSeverity string

const (
	SeverityWarning  AlarmSeverity = "warning"
	SeverityCritical AlarmSeverity = "critical"
)

type Alarm struct {
	ID           uuid.UUID     `json:"id"`
	LineID       LineID        `json:"line_id"`
	Code         string        `json:"code"`
	Message      string        `json:"message"`
	Severity     AlarmSeverity `json:"severity"`
	RaisedAt     time.Time     `json:"raised_at"`
	Acknowledged bool          `json:"acknowledged"`
	RecoveredAt  *time.Time    `json:"recovered_at,omitempty"`
}

func NewAlarm(lineID LineID, code, message string, severity AlarmSeverity, now time.Time) Alarm {
	return Alarm{
		ID: uuid.New(), LineID: lineID, Code: code, Message: message,
		Severity: severity, RaisedAt: now.UTC(),
	}
}

// NewAlarmWithID 用既有身份重建告警，供重启后恢复超时告警使用，避免重新分配 ID。
func NewAlarmWithID(id uuid.UUID, lineID LineID, code, message string, severity AlarmSeverity, now time.Time) Alarm {
	alarm := NewAlarm(lineID, code, message, severity, now)
	alarm.ID = id
	return alarm
}

func (a Alarm) Acknowledge() Alarm {
	a.Acknowledged = true
	return a
}

func (a Alarm) Recover(now time.Time) Alarm {
	stamp := now.UTC()
	a.RecoveredAt = &stamp
	return a
}

func (a Alarm) Active() bool {
	return a.RecoveredAt == nil
}
