package config

import "github.com/spf13/viper"

type Config struct {
	HTTPPort          string   `mapstructure:"SUBTITLE_HTTP_PORT"`
	NATSURL           string   `mapstructure:"NATS_URL"`
	WhisperServerURL  string   `mapstructure:"WHISPER_SERVER_URL"`
	ScyllaHosts       []string `mapstructure:"SCYLLA_HOSTS"`
	ScyllaPort        int      `mapstructure:"SCYLLA_PORT"`
	ScyllaUsername    string   `mapstructure:"SCYLLA_USERNAME"`
	ScyllaPassword    string   `mapstructure:"SCYLLA_PASSWORD"`
	ScyllaDatacenter  string   `mapstructure:"SCYLLA_DATACENTER"`
	ReplicationFactor int      `mapstructure:"SCYLLA_REPLICATION_FACTOR"`
}

func Load() (Config, error) {
	viper.SetConfigName(".env")
	viper.SetConfigType("env")
	viper.AddConfigPath(".")
	viper.AddConfigPath("../..")
	_ = viper.ReadInConfig()
	viper.AutomaticEnv()

	viper.SetDefault("SUBTITLE_HTTP_PORT", "50090")
	viper.SetDefault("NATS_URL", "nats://localhost:4222")
	viper.SetDefault("WHISPER_SERVER_URL", "http://localhost:5000")
	viper.SetDefault("SCYLLA_HOSTS", "localhost")
	viper.SetDefault("SCYLLA_PORT", 9042)
	viper.SetDefault("SCYLLA_USERNAME", "cassandra")
	viper.SetDefault("SCYLLA_PASSWORD", "cassandra")
	viper.SetDefault("SCYLLA_DATACENTER", "datacenter1")
	viper.SetDefault("SCYLLA_REPLICATION_FACTOR", 1)

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}
