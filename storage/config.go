package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type ProcessConfig struct {
	Role        string 
	HTTPPort    int    
	TCPPort     int    
	CurrentTerm uint64
	VotedFor    int64  
}

func (p *ProcessConfig) UnmarshalJSON(data []byte) error {
	type Alias struct {
		Role        any 
		HTTPPort    any 
		TCPPort     any 
		CurrentTerm any 
		VotedFor    any 
	}
	var aux Alias
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	toInt := func(val any) (int, error) {
		if val == nil {
			return 0, nil
		}
		switch v := val.(type) {
		case float64:
			return int(v), nil
		case string:
			s := strings.TrimSpace(v)
			if s == "" {
				return 0, nil
			}
			return ParsePort(s)
		default:
			return 0, fmt.Errorf("unexpected type for port: %T", val)
		}
	}

	toUint64 := func(val any) (uint64, error) {
		if val == nil {
			return 0, nil
		}
		switch v := val.(type) {
		case float64:
			return uint64(v), nil
		case string:
			return strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		default:
			return 0, fmt.Errorf("unexpected type for term: %T", val)
		}
	}

	toInt64 := func(val any) (int64, error) {
		if val == nil {
			return 0, nil
		}
		switch v := val.(type) {
		case float64:
			return int64(v), nil
		case string:
			return strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		default:
			return 0, fmt.Errorf("unexpected type for voted_for: %T", val)
		}
	}

	httpP, err := toInt(aux.HTTPPort)
	if err != nil {
		return err
	}
	tcpP, err := toInt(aux.TCPPort)
	if err != nil {
		return err
	}
	term, err := toUint64(aux.CurrentTerm)
	if err != nil {
		return err
	}
	vf, err := toInt64(aux.VotedFor)
	if err != nil {
		return err
	}

	role := ""
	if aux.Role != nil {
		if s, ok := aux.Role.(string); ok {
			role = strings.TrimSpace(s)
		} else {
			role = fmt.Sprintf("%v", aux.Role)
		}
	}

	p.Role = role
	p.HTTPPort = httpP
	p.TCPPort = tcpP
	p.CurrentTerm = term
	p.VotedFor = vf
	return nil
}

func normalizeAddress(addr string) (string, string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", "", errors.New("empty address")
	}
	if p, err := strconv.Atoi(addr); err == nil && p > 0 && p <= 65535 {
		return ":" + addr, addr, nil
	}
	if strings.Contains(addr, ":") {
		host, port, err := net.SplitHostPort(addr)
		if err == nil && port != "" {
			if p, err := strconv.Atoi(port); err == nil && p > 0 && p <= 65535 {
				if host == "" {
					return ":" + port, port, nil
				}
				return addr, port, nil
			}
		}
		last := strings.LastIndex(addr, ":")
		if last != -1 && last < len(addr)-1 {
			candidate := addr[last+1:]
			if p, err := strconv.Atoi(candidate); err == nil && p > 0 && p <= 65535 {
				return addr, candidate, nil
			}
		}
	}
	return "", "", fmt.Errorf("invalid address or port %q", addr)
}

func ExtractPort(addr string) (string, error) {
	_, port, err := normalizeAddress(addr)
	return port, err
}

func ParsePort(addr string) (int, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return 0, nil
	}
	portStr, err := ExtractPort(addr)
	if err != nil {
		return 0, err
	}
	p, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, err
	}
	return p, nil
}

func GetConfigPath(baseDir string) string {
	if strings.HasSuffix(baseDir, ".json") {
		return baseDir
	}
	target := filepath.Join(baseDir, "config", "config.json")
	if _, err := os.Stat(target); err == nil {
		return target
	}
	fallback := filepath.Join(baseDir, "config.json")
	if _, err := os.Stat(fallback); err == nil {
		return fallback
	}
	return target
}

func GetProcessConfig(baseDir string) (*ProcessConfig, error) {
	configPath := GetConfigPath(baseDir)
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	var cfg ProcessConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file %s: %w", configPath, err)
	}
	return &cfg, nil
}

func OverwriteProcessConfig(baseDir string, cfg *ProcessConfig) error {
	if cfg == nil {
		return errors.New("cannot overwrite config with nil")
	}
	configPath := GetConfigPath(baseDir)
	configDir := filepath.Dir(configPath)
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return fmt.Errorf("creating config dir %s: %w", configDir, err)
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	tmp := configPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("writing config file %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, configPath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("committing config file %s: %w", configPath, err)
	}
	return nil
}

func AccessAndOverwriteProcessConfig(baseDir string, updateFn func(cfg *ProcessConfig) error) (*ProcessConfig, error) {
	cfg, err := GetProcessConfig(baseDir)
	if err != nil {
		return nil, err
	}
	if updateFn != nil {
		if err := updateFn(cfg); err != nil {
			return nil, err
		}
	}
	if err := OverwriteProcessConfig(baseDir, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func LoadOrInitProcessConfig(baseDir, httpFlag, tcpFlag string, roleFlag ...string) (*ProcessConfig, error) {
	configPath := GetConfigPath(baseDir)
	if _, err := os.Stat(configPath); err == nil {
		return GetProcessConfig(baseDir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	httpPort, err := ParsePort(httpFlag)
	if err != nil {
		return nil, fmt.Errorf("parsing http port from flag %q: %w", httpFlag, err)
	}

	tcpPort, err := ParsePort(tcpFlag)
	if err != nil {
		return nil, fmt.Errorf("parsing tcp port from flag %q: %w", tcpFlag, err)
	}

	role := ""
	if len(roleFlag) > 0 {
		role = strings.TrimSpace(roleFlag[0])
	}

	cfg := &ProcessConfig{
		Role:        role,
		HTTPPort:    httpPort,
		TCPPort:     tcpPort,
		CurrentTerm: 0,
		VotedFor:    -1,
	}

	if err := OverwriteProcessConfig(baseDir, cfg); err != nil {
		return nil, fmt.Errorf("creating config file: %w", err)
	}
	return cfg, nil
}