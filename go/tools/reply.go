package tools

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/avivklas/plexus-flow/pkg/worker"
)

// Draft is the output of the draft-reply agent.
type Draft struct {
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// Approval is the payload of the "approval" signal, which becomes the output
// of the await-approval step.
type Approval struct {
	Approved bool   `json:"approved"`
	Reviewer string `json:"reviewer"`
	Note     string `json:"note"`
}

// Sent is the output of send-reply and the input of retract-reply.
type Sent struct {
	MessageID string `json:"message_id"`
	TicketID  string `json:"ticket_id"`
	Reviewer  string `json:"approved_by"`
}

// RetractReplyResult is the output of retract-reply.
type RetractReplyResult struct {
	Retracted bool   `json:"retracted"`
	MessageID string `json:"message_id"`
}

var outbox = struct {
	sync.Mutex
	sent map[string]Sent // workflow ID -> message
}{sent: map[string]Sent{}}

// SendReply posts the approved reply to the customer. A reviewer rejection
// arrives here as an ordinary signal payload; turning it into a permanent
// failure is what makes plexus-flow roll back the refund.
func SendReply(ctx context.Context, t Ticket) (Sent, error) {
	if err := benchSleep(ctx); err != nil {
		return Sent{}, err
	}
	approval, err := dep[Approval](ctx, "await-approval")
	if err != nil {
		return Sent{}, err
	}
	if !approval.Approved {
		logf(red, "Helpdesk", "%s: reviewer %s rejected the reply: %q", t.TicketID, approval.Reviewer, approval.Note)
		return Sent{}, worker.NonRetryable(worker.WithCode("REJECTED_BY_HUMAN",
			fmt.Errorf("reviewer %s rejected the resolution: %s", approval.Reviewer, approval.Note)))
	}
	draft, err := dep[Draft](ctx, "draft-reply")
	if err != nil {
		return Sent{}, err
	}
	if strings.TrimSpace(draft.Body) == "" {
		return Sent{}, worker.NonRetryable(errors.New("empty reply body"))
	}

	wf := workflowID(ctx)
	outbox.Lock()
	defer outbox.Unlock()
	if s, ok := outbox.sent[wf]; ok { // idempotent
		return s, nil
	}
	s := Sent{MessageID: fmt.Sprintf("MSG-%d", time.Now().UnixNano()%1_000_000), TicketID: t.TicketID, Reviewer: approval.Reviewer}
	outbox.sent[wf] = s

	logf(green, "Helpdesk", "%s: ✉️  reply %s sent (approved by %s)", t.TicketID, s.MessageID, approval.Reviewer)
	if quiet {
		return s, nil
	}
	fmt.Printf("%s    ┌─ Subject: %s%s\n", cyan, draft.Subject, reset)
	for _, line := range strings.Split(strings.TrimSpace(draft.Body), "\n") {
		fmt.Printf("%s    │ %s%s\n", cyan, line, reset)
	}
	fmt.Printf("%s    └─%s\n", cyan, reset)
	return s, nil
}

// RetractReply undoes SendReply by withdrawing the message from the ticket thread.
func RetractReply(ctx context.Context, s Sent) (RetractReplyResult, error) {
	outbox.Lock()
	for wf, m := range outbox.sent {
		if m.MessageID == s.MessageID {
			delete(outbox.sent, wf)
		}
	}
	outbox.Unlock()
	logf(purple, "Helpdesk ↩ COMPENSATION", "%s: reply %s retracted from the ticket thread", s.TicketID, s.MessageID)
	return RetractReplyResult{Retracted: true, MessageID: s.MessageID}, nil
}
