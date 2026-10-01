package config

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wyw14/cry-97/internal/dwell"
)

type Config struct {
	Address         string
	DataDir         string
	WebDir          string
	ShutdownTimeout time.Duration
	SampleWindow    int
	StageTimeouts   string
}

func Load() (Config, error) {
	cfg := Config{
		Address:         env("BIOTREAT_ADDR", "127.0.0.1:19697"),
		DataDir:         env("BIOTREAT_DATA", filepath.Join("data", "events")),
		WebDir:          env("BIOTREAT_WEB", "web"),
		ShutdownTimeout: 8 * time.Second,
		SampleWindow:    10,
		StageTimeouts:   strings.TrimSpace(os.Getenv("BIOTREAT_STAGE_TIMEOUTS")),
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// DwellPolicy 解析“处理线 × 工艺阶段”的允许停留时长配置；
// 空配置使用内置默认值。格式见 dwell.ParsePolicy。
func (c Config) DwellPolicy() (dwell.Policy, error) {
	return dwell.ParsePolicy(c.StageTimeouts)
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.Address) == "" {
		return errors.New("listen address is required")
	}
	if _, _, err := net.SplitHostPort(c.Address); err != nil {
		return err
	}
	if c.DataDir == "" || c.WebDir == "" {
		return errors.New("data and web directories are required")
	}
	if c.SampleWindow < 3 || c.SampleWindow > 120 {
		return errors.New("sample window must be between 3 and 120")
	}
	return nil
}

func (c Config) EnsureDirectories() error {
	return os.MkdirAll(c.DataDir, 0o755)
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
