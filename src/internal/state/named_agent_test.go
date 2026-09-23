package state

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestNamedAgentLookupPersistsAcrossActorsAndRestart(t *testing.T) {
	ctx := context.Background()
	path := testPath(t)
	s := openTestStore(t, path)
	requireOK(t, s.Bind(ctx, "stable", "node"))
	c := lifecycleCommand(t, 1)
	want, _, err := s.CreateCommand(ctx, c, commandTestTime)
	requireOK(t, err)
	got, err := s.FindNamedAgent(ctx, "node", "", "agent")
	requireOK(t, err)
	if !proto.Equal(want, got) {
		t.Fatal("lookup changed the original pending command")
	}
	requireOK(t, s.Close())
	s = openTestStore(t, path)
	got, err = s.FindNamedAgent(ctx, "node", "", "agent")
	requireOK(t, err)
	if !proto.Equal(got.Command, want.Command) {
		t.Fatal("restart lost original actor, key or generation")
	}
	for _, selectors := range [][3]string{{"node", "", "missing"}, {"other", "", "agent"}, {"node", "main", "agent"}} {
		if _, err := s.FindNamedAgent(ctx, selectors[0], selectors[1], selectors[2]); !errors.Is(err, ErrCommandNotFound) {
			t.Fatalf("unknown scope was guessed: %v", err)
		}
	}
}

func TestNamedAgentHistoricalAmbiguityIsNotNewestWins(t *testing.T) {
	ctx := context.Background()
	s := commandStore(t)
	for i := 1; i <= 2; i++ {
		c := lifecycleCommand(t, i)
		c.AgentStart.WorkspaceId = fmt.Sprintf("workspace-%d", i)
		c.SubmittedRequest.AgentStart.WorkspaceId = c.AgentStart.WorkspaceId
		if i == 2 {
			c.Actor.ActorId = "different-controller"
			c.AgentStart.Name = "historical-name"
			c.SubmittedRequest.AgentStart.Name = c.AgentStart.Name
		}
		record, _, err := s.CreateCommand(ctx, c, commandTestTime)
		requireOK(t, err)
		if i == 2 {
			// Simulate a valid journal imported from before name reservations.
			record.Command.AgentStart.Name = "agent"
			record.Command.SubmittedRequest.AgentStart.Name = "agent"
			payload, err := marshalCommandMessage(record)
			requireOK(t, err)
			_, err = s.conn.ExecContext(ctx, "UPDATE commands SET record = ? WHERE command_id = ?", payload, c.CommandId)
			requireOK(t, err)
		}
	}
	if _, err := s.FindNamedAgent(ctx, "node", "", "agent"); !errors.Is(err, ErrNamedAgentAmbiguous) {
		t.Fatalf("ambiguous original starts returned %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.FindNamedAgent(cancelled, "node", "", "agent"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation ignored: %v", err)
	}
}

func TestNamedAgentConcurrentActorsClaimAndSubmitAtomically(t *testing.T) {
	ctx := context.Background()
	path := testPath(t)
	s := openTestStore(t, path)
	requireOK(t, s.Bind(ctx, "stable", "node"))
	start := make(chan struct{})
	var wait sync.WaitGroup
	results := make(chan error, 2)
	for i := 1; i <= 2; i++ {
		command := lifecycleCommand(t, i)
		command.Actor.ActorId = fmt.Sprintf("controller-%d", i)
		command.AgentStart.WorkspaceId = fmt.Sprintf("workspace-%d", i)
		command.SubmittedRequest.AgentStart.WorkspaceId = command.AgentStart.WorkspaceId
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, created, err := s.CreateCommand(ctx, command, commandTestTime)
			if err == nil && !created {
				err = errors.New("distinct actor unexpectedly treated as exact retry")
			}
			results <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	successes, collisions := 0, 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrNamedAgentExists):
			collisions++
		default:
			t.Fatalf("unexpected concurrent admission result: %v", err)
		}
	}
	if successes != 1 || collisions != 1 {
		t.Fatalf("admitted %d same-name starts, fenced %d", successes, collisions)
	}
	original, err := s.FindNamedAgent(ctx, "node", "", "agent")
	requireOK(t, err)
	requireOK(t, s.Close())
	s = openTestStore(t, path)
	got, err := s.FindNamedAgent(ctx, "node", "", "agent")
	requireOK(t, err)
	if !proto.Equal(got.Command, original.Command) {
		t.Fatal("durable central claim changed winner after restart")
	}
	retry, created, err := s.CreateCommand(ctx, original.Command, commandTestTime)
	requireOK(t, err)
	if created || !proto.Equal(retry.Command, original.Command) {
		t.Fatal("exact winning actor retry created another start")
	}
}
