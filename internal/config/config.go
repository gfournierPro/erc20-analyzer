package config

import (
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Env      string         `mapstructure:"env"`
	Postgres PostgresConfig `mapstructure:"postgres"`
	Redis    RedisConfig    `mapstructure:"redis"`
	NATS     NATSConfig     `mapstructure:"nats"`
	Chains   []ChainConfig  `mapstructure:"chains"`
	Service  ServiceConfig  `mapstructure:"service"`
}

type PostgresConfig struct {
	DSN string `mapstructure:"dsn"`
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

type NATSConfig struct {
	URL string `mapstructure:"url"`
}

type ChainConfig struct {
	Name        string `mapstructure:"name"`
	ChainID     int64  `mapstructure:"chain_id"`
	RPCURL      string `mapstructure:"rpc_url"`
	IsArchive   bool   `mapstructure:"is_archive"`
	MaxLogRange uint64 `mapstructure:"max_log_range"`
	RPSLimit    int    `mapstructure:"rps_limit"`
}

type ServiceConfig struct {
	HTTPPort    int `mapstructure:"http_port"`
	GRPCPort    int `mapstructure:"grpc_port"`
	MetricsPort int `mapstructure:"metrics_port"`
}

func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetEnvPrefix("ERC20")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
