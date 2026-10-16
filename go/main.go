// Command tools runs the Go "tool worker" of the agent pipeline showcase.
//
// It serves the side-effecting activities (refunds, customer replies) and
// their compensations. It connects to plexus-flow over gRPC and never talks to
// the Python agents directly: plexus-flow decides when each tool runs.
//
//	go run ./go --grpc-addr=127.0.0.1:9090
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/avivklas/agent-pipeline-showcase/go/tools"
	"github.com/avivklas/plexus-flow/pkg/workerclient"
)

func main() {
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:9090", "plexus-flow gRPC address")
	workerID := flag.String("worker-id", "go-tool-worker", "unique worker ID")
	maxConcurrency := flag.Int("max-concurrency", 8, "tasks run in parallel by this process")
	flag.Parse()

	w := workerclient.New(*grpcAddr, workerclient.Options{WorkerID: *workerID, MaxConcurrency: *maxConcurrency})
	reg := w.Registry()
	reg.Register("issue-refund", tools.IssueRefund)
	reg.Register("reverse-refund", tools.ReverseRefund)
	reg.Register("send-reply", tools.SendReply)
	reg.Register("retract-reply", tools.RetractReply)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("\033[36m[Go Tool Worker]\033[0m %s connected to %s, serving %v\n", *workerID, *grpcAddr, reg.Names())
	if err := w.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("worker: %v", err)
	}
}
