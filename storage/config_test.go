package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStorageConfigOperations(t *testing.T) {
	dir := t.TempDir()

	// 1. LoadOrInitProcessConfig when file does not exist
	cfg, err := LoadOrInitProcessConfig(dir, ":8001", ":7001", "leader")
	if err != nil {
		t.Fatalf("LoadOrInitProcessConfig failed: %v", err)
	}

	if cfg.Role != "leader" || cfg.HTTPPort != 8001 || cfg.TCPPort != 7001 || cfg.CurrentTerm != 0 || cfg.VotedFor != -1 {
		t.Fatalf("unexpected init config: %+v", cfg)
	}

	// Verify file path
	expectedFile := filepath.Join(dir, "config", "config.json")
	if fi, err := os.Stat(expectedFile); err != nil || fi.Size() == 0 {
		t.Fatalf("expected config file at %s", expectedFile)
	}

	// 2. Read back config with GetProcessConfig
	readCfg, err := GetProcessConfig(dir)
	if err != nil {
		t.Fatalf("GetProcessConfig failed: %v", err)
	}
	if readCfg.Role != "leader" || readCfg.HTTPPort != 8001 || readCfg.TCPPort != 7001 || readCfg.CurrentTerm != 0 || readCfg.VotedFor != -1 {
		t.Fatalf("read mismatch: %+v", readCfg)
	}

	// 3. Overwrite with new values
	newCfg := &ProcessConfig{
		Role:        "follower",
		HTTPPort:    8002,
		TCPPort:     7002,
		CurrentTerm: 3,
		VotedFor:    1,
	}
	if err := OverwriteProcessConfig(dir, newCfg); err != nil {
		t.Fatalf("OverwriteProcessConfig failed: %v", err)
	}

	// 4. Access and modify
	modified, err := AccessAndOverwriteProcessConfig(dir, func(c *ProcessConfig) error {
		c.CurrentTerm = 10
		c.VotedFor = -1
		return nil
	})
	if err != nil {
		t.Fatalf("AccessAndOverwriteProcessConfig failed: %v", err)
	}
	if modified.CurrentTerm != 10 || modified.VotedFor != -1 {
		t.Fatalf("unexpected modified values: %+v", modified)
	}

	// 5. Verify persistence
	reloaded, err := GetConfig(dir)
	if err != nil {
		t.Fatalf("GetConfig failed: %v", err)
	}
	if reloaded.CurrentTerm != 10 || reloaded.VotedFor != -1 || reloaded.Role != "follower" {
		t.Fatalf("reloaded mismatch: %+v", reloaded)
	}
}
