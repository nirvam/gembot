package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	AppID                string   `mapstructure:"feishu_app_id"`
	AppSecret            string   `mapstructure:"feishu_app_secret"`
	EncryptKey           string   `mapstructure:"feishu_encrypt_key"`
	VerificationToken    string   `mapstructure:"feishu_verification_token"`
	WorkerCount          int      `mapstructure:"worker_count"`
	SessionRetentionDays int      `mapstructure:"session_retention_days"`
	DBPath               string   `mapstructure:"db_path"`
	Port                 int      `mapstructure:"port"`
	AgentCommand         string   `mapstructure:"agent_command"`
	AgentArgs            []string `mapstructure:"agent_args"`
}

func Load() (*Config, error) {
	v := viper.New()

	// 1. Defaults
	v.SetDefault("worker_count", 10)
	v.SetDefault("session_retention_days", 7)
	v.SetDefault("port", 8080)
	v.SetDefault("db_path", "gembot.db")
	v.SetDefault("agent_command", "gemini")
	v.SetDefault("agent_args", []string{"--acp", "--yolo"})

	// 2. Config file
	v.SetConfigName(".env")
	v.SetConfigType("env")
	v.AddConfigPath(".")
	
	// Ignore if .env file is missing
	_ = v.ReadInConfig()

	// 3. Environment Variables
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// 4. Validation
	if cfg.AppID == "" || cfg.AppSecret == "" {
		return nil, fmt.Errorf("FEISHU_APP_ID and FEISHU_APP_SECRET are required")
	}

	return &cfg, nil
}
