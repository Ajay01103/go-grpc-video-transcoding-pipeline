package config

import (
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	HTTPPort          string        `mapstructure:"ASSET_HTTP_PORT"`
	NATSURL           string        `mapstructure:"NATS_URL"`
	ScyllaHosts       []string      `mapstructure:"SCYLLA_HOSTS"`
	ScyllaPort        int           `mapstructure:"SCYLLA_PORT"`
	ScyllaUsername    string        `mapstructure:"SCYLLA_USERNAME"`
	ScyllaPassword    string        `mapstructure:"SCYLLA_PASSWORD"`
	ScyllaDatacenter  string        `mapstructure:"SCYLLA_DATACENTER"`
	ReplicationFactor int           `mapstructure:"SCYLLA_REPLICATION_FACTOR"`
	RustFSURL         string        `mapstructure:"RUSTFS_URL"`
	RustFSAccessKey   string        `mapstructure:"RUSTFS_ACCESS_KEY"`
	RustFSSecretKey   string        `mapstructure:"RUSTFS_SECRET_KEY"`
	RustFSBucket      string        `mapstructure:"RUSTFS_BUCKET"`
	UploadURLTTL      time.Duration `mapstructure:"UPLOAD_URL_TTL"`
	ReconcileInterval time.Duration `mapstructure:"ASSET_RECONCILE_INTERVAL"`
	ReconcileAge      time.Duration `mapstructure:"ASSET_RECONCILE_AGE"`
}

func Load() (Config, error) {
	viper.SetConfigName(".env")
	viper.SetConfigType("env")
	viper.AddConfigPath(".")
	viper.AddConfigPath("../..")
	_ = viper.ReadInConfig()
	viper.AutomaticEnv()

	viper.SetDefault("ASSET_HTTP_PORT", "50060")
	viper.SetDefault("NATS_URL", "nats://localhost:4222")
	viper.SetDefault("SCYLLA_HOSTS", "localhost")
	viper.SetDefault("SCYLLA_PORT", 9042)
	viper.SetDefault("SCYLLA_USERNAME", "cassandra")
	viper.SetDefault("SCYLLA_PASSWORD", "cassandra")
	viper.SetDefault("SCYLLA_DATACENTER", "datacenter1")
	viper.SetDefault("SCYLLA_REPLICATION_FACTOR", 1)
	viper.SetDefault("RUSTFS_URL", "http://localhost:9000")
	viper.SetDefault("RUSTFS_ACCESS_KEY", "rustfsadmin")
	viper.SetDefault("RUSTFS_SECRET_KEY", "rustfsadmin")
	viper.SetDefault("RUSTFS_BUCKET", "uploads")
	viper.SetDefault("UPLOAD_URL_TTL", "30m")
	viper.SetDefault("ASSET_RECONCILE_INTERVAL", "30s")
	viper.SetDefault("ASSET_RECONCILE_AGE", "5m")

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}
