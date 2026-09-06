package config

import "github.com/spf13/viper"

type Config struct {
	HTTPPort          string   `mapstructure:"JOB_HTTP_PORT"`
	NATSURL           string   `mapstructure:"NATS_URL"`
	PlaybackURL       string   `mapstructure:"PLAYBACK_URL"`
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

	viper.SetDefault("JOB_HTTP_PORT", "50080")
	viper.SetDefault("NATS_URL", "nats://localhost:4222")
	viper.SetDefault("PLAYBACK_URL", "http://localhost:50070")
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
