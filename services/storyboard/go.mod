module github.com/Ajay01103/go-notion/storyboard

go 1.25.0

require (
	github.com/Ajay01103/go-notion/pkg v0.0.0-00010101000000-000000000000
	github.com/nats-io/nats.go v1.47.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/nats-io/nkeys v0.4.11 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.49.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
)

replace github.com/Ajay01103/go-notion/pkg => ../../pkg
