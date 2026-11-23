package config

import (
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type SecretString string

func (s SecretString) String() string {
	if s == "" {
		return ""
	}
	return "[REDACTED]"
}

func (s SecretString) GoString() string {
	return s.String()
}

func (s SecretString) LogValue() slog.Value {
	return slog.StringValue(s.String())
}

func (s SecretString) UnsafeValue() string {
	return string(s)
}

type Config struct {
	Env      string
	LogLevel string
	HTTP     HTTPConfig
	Database DatabaseConfig
	NATS     NATSConfig
	Worker   WorkerConfig
	Chain    ChainConfig
	Auth     AuthConfig
}

type AuthConfig struct {
	APIKeys         SecretString
	RateLimitPerSec float64
	RateLimitBurst  int
}

type HTTPConfig struct {
	Addr            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
}

type DatabaseConfig struct {
	DSN      SecretString
	MaxConns int32
	MinConns int32
}

type NATSConfig struct {
	URL        string
	Stream     string
	AckWait    time.Duration
	MaxDeliver int
}

type WorkerConfig struct {
	Concurrency int
	LeaseTTL    time.Duration
}

type ChainConfig struct {
	ChainID               int64
	RequiredConfirmations uint64
	MaxFeeCapWei          *big.Int
	RPCURLs               []string
}

func Load() (Config, error) {
	return LoadFromLookup(os.LookupEnv)
}

func LoadFromLookup(lookup func(string) (string, bool)) (Config, error) {
	cfg := Config{
		Env:      "development",
		LogLevel: "info",
		HTTP: HTTPConfig{
			Addr:            ":8080",
			ReadTimeout:     5 * time.Second,
			WriteTimeout:    10 * time.Second,
			ShutdownTimeout: 25 * time.Second,
		},
		Database: DatabaseConfig{
			MaxConns: 25,
			MinConns: 5,
		},
		NATS: NATSConfig{
			URL:        "nats://localhost:4222",
			Stream:     "CHAINFLOW_TX",
			AckWait:    45 * time.Second,
			MaxDeliver: 5,
		},
		Worker: WorkerConfig{
			Concurrency: 20,
			LeaseTTL:    30 * time.Second,
		},
		Chain: ChainConfig{
			ChainID:               11155111,
			RequiredConfirmations: 6,
			MaxFeeCapWei:          big.NewInt(100_000_000_000),
		},
		Auth: AuthConfig{
			RateLimitPerSec: 50,
			RateLimitBurst:  100,
		},
	}

	if v, ok := lookup("CHAINFLOW_ENV"); ok && v != "" {
		cfg.Env = v
	}
	if v, ok := lookup("CHAINFLOW_LOG_LEVEL"); ok && v != "" {
		cfg.LogLevel = v
	}
	if v, ok := lookup("CHAINFLOW_HTTP_ADDR"); ok && v != "" {
		cfg.HTTP.Addr = v
	}
	if err := parseDurationEnv(lookup, "CHAINFLOW_HTTP_READ_TIMEOUT", &cfg.HTTP.ReadTimeout); err != nil {
		return Config{}, err
	}
	if err := parseDurationEnv(lookup, "CHAINFLOW_HTTP_WRITE_TIMEOUT", &cfg.HTTP.WriteTimeout); err != nil {
		return Config{}, err
	}
	if err := parseDurationEnv(lookup, "CHAINFLOW_HTTP_SHUTDOWN_TIMEOUT", &cfg.HTTP.ShutdownTimeout); err != nil {
		return Config{}, err
	}

	if v, ok := lookup("CHAINFLOW_DB_DSN"); ok {
		cfg.Database.DSN = SecretString(strings.TrimSpace(v))
	}
	if err := parseInt32Env(lookup, "CHAINFLOW_DB_MAX_CONNS", &cfg.Database.MaxConns); err != nil {
		return Config{}, err
	}
	if err := parseInt32Env(lookup, "CHAINFLOW_DB_MIN_CONNS", &cfg.Database.MinConns); err != nil {
		return Config{}, err
	}

	if v, ok := lookup("CHAINFLOW_NATS_URL"); ok && v != "" {
		cfg.NATS.URL = strings.TrimSpace(v)
	}
	if v, ok := lookup("CHAINFLOW_NATS_STREAM"); ok && v != "" {
		cfg.NATS.Stream = strings.TrimSpace(v)
	}
	if err := parseDurationEnv(lookup, "CHAINFLOW_NATS_ACK_WAIT", &cfg.NATS.AckWait); err != nil {
		return Config{}, err
	}
	if err := parseIntEnv(lookup, "CHAINFLOW_NATS_MAX_DELIVER", &cfg.NATS.MaxDeliver); err != nil {
		return Config{}, err
	}

	if err := parseIntEnv(lookup, "CHAINFLOW_WORKER_CONCURRENCY", &cfg.Worker.Concurrency); err != nil {
		return Config{}, err
	}
	if err := parseDurationEnv(lookup, "CHAINFLOW_WORKER_LEASE_TTL", &cfg.Worker.LeaseTTL); err != nil {
		return Config{}, err
	}

	if err := parseInt64Env(lookup, "CHAINFLOW_CHAIN_ID", &cfg.Chain.ChainID); err != nil {
		return Config{}, err
	}
	if v, ok := lookup("CHAINFLOW_REQUIRED_CONFIRMATIONS"); ok && v != "" {
		n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("invalid CHAINFLOW_REQUIRED_CONFIRMATIONS: %w", err)
		}
		cfg.Chain.RequiredConfirmations = n
	}
	if v, ok := lookup("CHAINFLOW_MAX_FEE_CAP_WEI"); ok && v != "" {
		capWei, okParse := new(big.Int).SetString(strings.TrimSpace(v), 10)
		if !okParse {
			return Config{}, fmt.Errorf("invalid CHAINFLOW_MAX_FEE_CAP_WEI: %q", v)
		}
		cfg.Chain.MaxFeeCapWei = capWei
	}
	if v, ok := lookup("CHAINFLOW_RPC_URLS"); ok && strings.TrimSpace(v) != "" {
		parts := strings.Split(v, ",")
		urls := make([]string, 0, len(parts))
		for _, p := range parts {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				urls = append(urls, trimmed)
			}
		}
		cfg.Chain.RPCURLs = urls
	}
	if v, ok := lookup("CHAINFLOW_API_KEYS"); ok {
		cfg.Auth.APIKeys = SecretString(strings.TrimSpace(v))
	}
	if v, ok := lookup("CHAINFLOW_RATE_LIMIT_RPS"); ok && strings.TrimSpace(v) != "" {
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return Config{}, fmt.Errorf("invalid CHAINFLOW_RATE_LIMIT_RPS: %w", err)
		}
		cfg.Auth.RateLimitPerSec = f
	}
	if err := parseIntEnv(lookup, "CHAINFLOW_RATE_LIMIT_BURST", &cfg.Auth.RateLimitBurst); err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	var errs []error

	if c.HTTP.Addr == "" {
		errs = append(errs, errors.New("http addr must not be empty"))
	}
	if c.HTTP.ReadTimeout <= 0 || c.HTTP.WriteTimeout <= 0 || c.HTTP.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("http timeouts must be positive"))
	}

	if c.Database.DSN.UnsafeValue() == "" {
		errs = append(errs, errors.New("database DSN is required"))
	}
	if c.Database.MinConns < 0 || c.Database.MaxConns <= 0 {
		errs = append(errs, errors.New("database pool limits must be valid positive counts"))
	}
	if c.Database.MinConns > c.Database.MaxConns {
		errs = append(errs, errors.New("database min_conns cannot exceed max_conns"))
	}

	if c.NATS.URL == "" || c.NATS.Stream == "" {
		errs = append(errs, errors.New("nats url and stream must not be empty"))
	}
	if c.NATS.MaxDeliver <= 0 {
		errs = append(errs, errors.New("nats max_deliver must be positive"))
	}

	if c.Worker.Concurrency < 1 || c.Worker.Concurrency > 10000 {
		errs = append(errs, errors.New("worker concurrency must be between 1 and 10000"))
	}
	if c.Worker.LeaseTTL <= 0 {
		errs = append(errs, errors.New("worker lease_ttl must be positive"))
	}
	if c.NATS.AckWait <= c.Worker.LeaseTTL {
		errs = append(errs, errors.New("nats ack_wait must be greater than worker lease_ttl"))
	}

	if c.Chain.ChainID <= 0 {
		errs = append(errs, errors.New("chain_id must be positive"))
	}
	if c.Chain.RequiredConfirmations == 0 {
		errs = append(errs, errors.New("required_confirmations must be positive"))
	}
	if c.Chain.MaxFeeCapWei == nil || c.Chain.MaxFeeCapWei.Sign() <= 0 {
		errs = append(errs, errors.New("max_fee_cap_wei must be positive"))
	}
	if len(c.Chain.RPCURLs) == 0 {
		errs = append(errs, errors.New("at least one RPC URL is required"))
	}
	for _, raw := range c.Chain.RPCURLs {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" {
			errs = append(errs, fmt.Errorf("invalid rpc url %q", raw))
			continue
		}
		switch u.Scheme {
		case "http", "https", "ws", "wss":
		default:
			errs = append(errs, fmt.Errorf("unsupported rpc url scheme %q", u.Scheme))
		}
	}

	return errors.Join(errs...)
}

func parseDurationEnv(lookup func(string) (string, bool), key string, dst *time.Duration) error {
	v, ok := lookup(key)
	if !ok || strings.TrimSpace(v) == "" {
		return nil
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return fmt.Errorf("invalid %s: %w", key, err)
	}
	*dst = d
	return nil
}

func parseIntEnv(lookup func(string) (string, bool), key string, dst *int) error {
	v, ok := lookup(key)
	if !ok || strings.TrimSpace(v) == "" {
		return nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return fmt.Errorf("invalid %s: %w", key, err)
	}
	*dst = n
	return nil
}

func parseInt32Env(lookup func(string) (string, bool), key string, dst *int32) error {
	v, ok := lookup(key)
	if !ok || strings.TrimSpace(v) == "" {
		return nil
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 32)
	if err != nil {
		return fmt.Errorf("invalid %s: %w", key, err)
	}
	*dst = int32(n)
	return nil
}

func parseInt64Env(lookup func(string) (string, bool), key string, dst *int64) error {
	v, ok := lookup(key)
	if !ok || strings.TrimSpace(v) == "" {
		return nil
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid %s: %w", key, err)
	}
	*dst = n
	return nil
}
