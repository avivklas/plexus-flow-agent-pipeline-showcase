module github.com/avivklas/agent-pipeline-showcase

go 1.27.0

replace github.com/avivklas/plexus-flow => ../plexus-flow

replace github.com/avivklas/plexus => ../plexus

require github.com/avivklas/plexus-flow v0.0.0-00010101000000-000000000000

require (
	github.com/avivklas/plexus v0.2.0 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/grpc v1.82.1 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
