// Package tools holds the side-effecting "tools" the agent pipeline uses.
//
// Agents (Python) only *propose* actions. These Go activities are the only
// code allowed to touch the systems of record: the payments ledger and the
// helpdesk outbox. Every action has a compensating action, so plexus-flow can
// undo it automatically when a later step (guardrail, human reviewer) says no.
package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/avivklas/plexus-flow/pkg/worker"
	"github.com/avivklas/plexus-flow/pkg/workerclient"
)

const (
	reset  = "\033[0m"
	red    = "\033[31m"
	green  = "\033[32m"
	yellow = "\033[33m"
	purple = "\033[35m"
	cyan   = "\033[36m"
	bold   = "\033[1m"
)

// quiet silences per-step logs (set by the benchmark).
var quiet = os.Getenv("SHOWCASE_QUIET") == "1"

// benchStepDur simulates external API latency (e.g. Stripe/Helpdesk) when BENCH_STEP_MS > 0.
var benchStepDur = func() time.Duration {
	if ms, err := strconv.ParseFloat(os.Getenv("BENCH_STEP_MS"), 64); err == nil && ms > 0 {
		return time.Duration(ms * float64(time.Millisecond))
	}
	return 0
}()

func benchSleep(ctx context.Context) error {
	if benchStepDur <= 0 {
		return nil
	}
	select {
	case <-time.After(benchStepDur):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func logf(color, tag, format string, args ...any) {
	if quiet {
		return
	}
	fmt.Printf("%s %s[%s]%s %s\n", time.Now().Format("15:04:05"), color, tag, reset, fmt.Sprintf(format, args...))
}

// Ticket is the workflow input every step receives.
type Ticket struct {
	TicketID   string `json:"ticket_id"`
	CustomerID string `json:"customer_id"`
	OrderID    string `json:"order_id"`
	Subject    string `json:"subject"`
}

// Plan is the output of the resolution-plan agent.
type Plan struct {
	Action       string  `json:"action"`
	RefundAmount float64 `json:"refund_amount"`
	Currency     string  `json:"currency"`
	Rationale    string  `json:"rationale"`
}

// Refund is the output of issue-refund and the input of reverse-refund.
type Refund struct {
	RefundID   string  `json:"refund_id,omitempty"`
	Amount     float64 `json:"amount"`
	Currency   string  `json:"currency,omitempty"`
	OrderID    string  `json:"order_id,omitempty"`
	CustomerID string  `json:"customer_id,omitempty"`
	WorkflowID string  `json:"workflow_id,omitempty"`
	Skipped    bool    `json:"skipped,omitempty"`
}

// ledger is a stand-in for a payments provider. It is keyed by workflow ID,
// so a redelivered task can never refund the same ticket twice.
var ledger = struct {
	sync.Mutex
	refunds map[string]*Refund
	issued  float64
}{refunds: map[string]*Refund{}}

func dep[T any](ctx context.Context, step string) (T, error) {
	var v T
	task, ok := workerclient.TaskFromContext(ctx)
	if !ok {
		return v, errors.New("no task in context")
	}
	raw, ok := task.DependencyOutputs[step]
	if !ok || len(raw) == 0 {
		return v, worker.NonRetryable(fmt.Errorf("missing output of step %q", step))
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return v, worker.NonRetryable(fmt.Errorf("decode %s output: %w", step, err))
	}
	return v, nil
}

func workflowID(ctx context.Context) string {
	if task, ok := workerclient.TaskFromContext(ctx); ok {
		return task.WorkflowID
	}
	return ""
}

// IssueRefund executes the refund the planner proposed.
func IssueRefund(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	if err := benchSleep(ctx); err != nil {
		return nil, err
	}
	var t Ticket
	_ = json.Unmarshal(input, &t)
	plan, err := dep[Plan](ctx, "resolution-plan")
	if err != nil {
		return nil, err
	}
	wf := workflowID(ctx)

	if plan.RefundAmount <= 0 {
		logf(cyan, "Payments", "%s: plan %q needs no refund, nothing to do", t.TicketID, plan.Action)
		return json.Marshal(Refund{Skipped: true, WorkflowID: wf})
	}

	ledger.Lock()
	defer ledger.Unlock()
	if r, ok := ledger.refunds[wf]; ok { // idempotent: same workflow, same refund
		logf(yellow, "Payments", "%s: refund %s already issued, returning it (idempotent)", t.TicketID, r.RefundID)
		return json.Marshal(r)
	}
	r := &Refund{
		RefundID:   fmt.Sprintf("RF-%d", time.Now().UnixNano()%1_000_000),
		Amount:     plan.RefundAmount,
		Currency:   plan.Currency,
		OrderID:    t.OrderID,
		CustomerID: t.CustomerID,
		WorkflowID: wf,
	}
	ledger.refunds[wf] = r
	ledger.issued += r.Amount
	logf(green, "Payments", "%s: 💸 refund %s of %s$%.2f%s issued to %s (order %s) | ledger total $%.2f",
		t.TicketID, r.RefundID, bold, r.Amount, reset, r.CustomerID, r.OrderID, ledger.issued)
	return json.Marshal(r)
}

// ReverseRefund undoes IssueRefund. Its input is the output of IssueRefund.
func ReverseRefund(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	var r Refund
	if err := json.Unmarshal(input, &r); err != nil {
		return nil, worker.NonRetryable(err)
	}
	if r.Skipped || r.RefundID == "" {
		return json.Marshal(map[string]any{"reversed": false, "reason": "nothing was refunded"})
	}
	ledger.Lock()
	defer ledger.Unlock()
	if _, ok := ledger.refunds[r.WorkflowID]; ok {
		delete(ledger.refunds, r.WorkflowID)
		ledger.issued -= r.Amount
	}
	logf(purple, "Payments ↩ COMPENSATION", "refund %s of $%.2f reversed for %s | ledger total $%.2f",
		r.RefundID, r.Amount, r.CustomerID, ledger.issued)
	return json.Marshal(map[string]any{"reversed": true, "refund_id": r.RefundID})
}
