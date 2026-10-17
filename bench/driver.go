// Command bench-driver submits concurrent workflow bursts over HTTP and streams
// terminal completions over SSE (GET /api/v1/flows/{flow}/watch?terminal=1).
// Running the HTTP client in Go eliminates single-threaded Python httpx/GIL
// overhead when launching 512–1,024 workflows in parallel.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type InputSpec struct {
	Flow       string          `json:"flow"`
	Prefix     string          `json:"prefix"`
	Workload   string          `json:"workload"`
	Definition json.RawMessage `json:"definition"`
	Input      json.RawMessage `json:"input"`
	Approval   json.RawMessage `json:"approval"`
	Concurrency int            `json:"concurrency"`
	Rounds     int             `json:"rounds"`
	Warmup     int             `json:"warmup"`
	BudgetSec  float64         `json:"budget_sec"`
	StepMs     float64         `json:"step_ms"`
}

type Summary struct {
	WorkflowID string `json:"workflow_id"`
	Status     string `json:"status"`
	StartedAt  string `json:"started_at"`
	EndedAt    string `json:"ended_at"`
}

type OutputSpec struct {
	Summaries []Summary `json:"summaries"`
	RoundIds  [][]string `json:"round_ids"`
	Stuck     []string  `json:"stuck"`
	WallSec   float64   `json:"wall_sec"`
}

var terminalStatuses = map[string]bool{
	"COMPLETED":           true,
	"FAILED":              true,
	"COMPENSATED":         true,
	"CANCELLED":           true,
	"COMPENSATION_FAILED": true,
}

func main() {
	httpAddr := flag.String("http-addr", "127.0.0.1:28080", "plexus-flow HTTP address")
	flag.Parse()

	var spec InputSpec
	if err := json.NewDecoder(os.Stdin).Decode(&spec); err != nil {
		fmt.Fprintf(os.Stderr, "decode spec: %v\n", err)
		os.Exit(1)
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        2048,
		MaxIdleConnsPerHost: 2048,
		MaxConnsPerHost:     2048,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second,
	}
	sseClient := &http.Client{
		Transport: transport,
		Timeout:   0,
	}

	baseURL := "http://" + *httpAddr
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchReq, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/v1/flows/"+spec.Flow+"/watch?terminal=1", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "watch req: %v\n", err)
		os.Exit(1)
	}
	watchResp, err := sseClient.Do(watchReq)
	if err != nil {
		fmt.Fprintf(os.Stderr, "watch connect: %v\n", err)
		os.Exit(1)
	}
	defer watchResp.Body.Close()

	events := make(chan Summary, 4096)
	go func() {
		scanner := bufio.NewScanner(watchResp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			var s Summary
			if err := json.Unmarshal([]byte(strings.TrimSpace(line[5:])), &s); err == nil {
				if terminalStatuses[s.Status] {
					events <- s
				}
			}
		}
		close(events)
	}()

	startSuffix := fmt.Sprintf(`,"definition":%s,"input":%s}`, string(spec.Definition), string(spec.Input))

	startAndSignal := func(wfID string) error {
		body := []byte(fmt.Sprintf(`{"workflow_id":%q%s`, wfID, startSuffix))
		resp, err := client.Post(baseURL+"/api/v1/workflows/start", "application/json", bytes.NewReader(body))
		if err != nil {
			return err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			return fmt.Errorf("start %s: status %d", wfID, resp.StatusCode)
		}
		if spec.Workload == "agent" {
			if spec.StepMs >= 20 {
				time.Sleep(time.Duration(spec.StepMs*1.5) * time.Millisecond)
			}
			sresp, err := client.Post(baseURL+"/api/v1/workflows/"+wfID+"/signal", "application/json", bytes.NewReader(spec.Approval))
			if err != nil {
				return err
			}
			_, _ = io.Copy(io.Discard, sresp.Body)
			sresp.Body.Close()
			if sresp.StatusCode >= 300 {
				return fmt.Errorf("signal %s: status %d", wfID, sresp.StatusCode)
			}
		}
		return nil
	}

	collect := func(pending map[string]bool, deadline time.Time) map[string]Summary {
		got := make(map[string]Summary, len(pending))
		for len(pending) > 0 {
			rem := time.Until(deadline)
			if rem <= 0 {
				break
			}
			select {
			case s, ok := <-events:
				if !ok {
					return got
				}
				if pending[s.WorkflowID] {
					got[s.WorkflowID] = s
					delete(pending, s.WorkflowID)
				}
			case <-time.After(rem):
				return got
			}
		}
		return got
	}

	if spec.Warmup > 0 {
		warmPending := make(map[string]bool, spec.Warmup)
		var wg sync.WaitGroup
		for i := 0; i < spec.Warmup; i++ {
			id := fmt.Sprintf("%s-warmup-%d", spec.Prefix, i)
			warmPending[id] = true
			wg.Add(1)
			go func(wfID string) {
				defer wg.Done()
				if err := startAndSignal(wfID); err != nil {
					fmt.Fprintf(os.Stderr, "warmup error: %v\n", err)
				}
			}(id)
		}
		wg.Wait()
		collect(warmPending, time.Now().Add(30*time.Second))
		if len(warmPending) > 0 {
			fmt.Fprintf(os.Stderr, "warmup timed out (%d remaining)\n", len(warmPending))
			os.Exit(1)
		}
	}

	var out OutputSpec
	t0 := time.Now()
	deadline := t0.Add(time.Duration(spec.BudgetSec * float64(time.Second)))

	for rnd := 0; rnd < spec.Rounds; rnd++ {
		ids := make([]string, spec.Concurrency)
		pending := make(map[string]bool, spec.Concurrency)
		for i := 0; i < spec.Concurrency; i++ {
			id := fmt.Sprintf("%s-r%d-%d", spec.Prefix, rnd, i)
			ids[i] = id
			pending[id] = true
		}
		out.RoundIds = append(out.RoundIds, ids)

		var wg sync.WaitGroup
		for _, id := range ids {
			wg.Add(1)
			go func(wfID string) {
				defer wg.Done()
				if err := startAndSignal(wfID); err != nil {
					fmt.Fprintf(os.Stderr, "burst error: %v\n", err)
				}
			}(id)
		}
		wg.Wait()

		got := collect(pending, deadline)
		for _, id := range ids {
			if s, ok := got[id]; ok {
				out.Summaries = append(out.Summaries, s)
			}
		}
		if len(pending) > 0 {
			for id := range pending {
				out.Stuck = append(out.Stuck, id)
			}
			break
		}
	}

	out.WallSec = time.Since(t0).Seconds()
	_ = json.NewEncoder(os.Stdout).Encode(out)
}
