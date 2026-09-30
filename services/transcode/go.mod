module github.com/Ajay01103/go-mux/transcode

go 1.27.0

require (
	github.com/Ajay01103/go-mux/pkg v0.0.0-00010101000000-000000000000
	github.com/nats-io/nats.go v1.47.0
	go.uber.org/zap v1.27.1
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/klauspost/compress v1.19.1 // indirect
	github.com/nats-io/nkeys v0.4.11 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/Ajay01103/go-mux/pkg => ../../pkg
