package config

import (
	"github.com/spf13/viper"
)

type Config struct {
	HTTPPort          string   `mapstructure:"PLAYBACK_HTTP_PORT"`
	ScyllaHosts       []string `mapstructure:"SCYLLA_HOSTS"`
	ScyllaPort        int      `mapstructure:"SCYLLA_PORT"`
	ScyllaUsername    string   `mapstructure:"SCYLLA_USERNAME"`
	ScyllaPassword    string   `mapstructure:"SCYLLA_PASSWORD"`
	ScyllaDatacenter  string   `mapstructure:"SCYLLA_DATACENTER"`
	ReplicationFactor int      `mapstructure:"SCYLLA_REPLICATION_FACTOR"`
	JWTIssuer         string   `mapstructure:"JWT_ISSUER"`
	JWKSURL           string   `mapstructure:"JWKS_URL"`
	TokenTTL          int64    `mapstructure:"PLAYBACK_TOKEN_TTL"`
}

func Load() (Config, error) {
	viper.SetConfigName(".env")
	viper.SetConfigType("env")
	viper.AddConfigPath(".")
	viper.AddConfigPath("../..")
	_ = viper.ReadInConfig()
	viper.AutomaticEnv()

	viper.SetDefault("PLAYBACK_HTTP_PORT", "50070")
	viper.SetDefault("SCYLLA_HOSTS", "localhost")
	viper.SetDefault("SCYLLA_PORT", 9042)
	viper.SetDefault("SCYLLA_USERNAME", "cassandra")
	viper.SetDefault("SCYLLA_PASSWORD", "cassandra")
	viper.SetDefault("SCYLLA_DATACENTER", "datacenter1")
	viper.SetDefault("SCYLLA_REPLICATION_FACTOR", 1)
	viper.SetDefault("JWKS_URL", "http://localhost:50051/.well-known/jwks.json")
	viper.SetDefault("JWT_ISSUER", "go-notion-auth")
	viper.SetDefault("PLAYBACK_TOKEN_TTL", 300)

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}
