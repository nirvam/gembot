package core

import (
	"context"
	"os"
	"testing"

	"github.com/nirvam/gembot/internal/acp"
	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/nirvam/gembot/internal/config"
	"github.com/nirvam/gembot/internal/store"
)

type mockBridge struct {
	newSessionFunc func(ctx context.Context) (string, error)
	sendMessageFunc func(ctx context.Context, sessionID string, prompt ...acpsdk.ContentBlock) (<-chan acp.StreamEvent, error)
}

func (m *mockBridge) NewSession(ctx context.Context) (string, error) {
	return m.newSessionFunc(ctx)
}

func (m *mockBridge) SendMessage(ctx context.Context, sessionID string, prompt ...acpsdk.ContentBlock) (<-chan acp.StreamEvent, error) {
	return m.sendMessageFunc(ctx, sessionID, prompt...)
}

func (m *mockBridge) Close() error { return nil }

func TestManager_HandleMessage(t *testing.T) {
	dbPath := "test_gembot.db"
	defer os.Remove(dbPath)

	s, err := store.NewStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to init store: %v", err)
	}
	defer s.Close()

	mock := &mockBridge{
		newSessionFunc: func(ctx context.Context) (string, error) {
			return "mock-session-id", nil
		},
		sendMessageFunc: func(ctx context.Context, sessionID string, prompt ...acpsdk.ContentBlock) (<-chan acp.StreamEvent, error) {
			ch := make(chan acp.StreamEvent, 2)
			go func() {
				ch <- acp.StreamEvent{Type: acp.EventTypeText, Data: "Hello "}
				ch <- acp.StreamEvent{Type: acp.EventTypeText, Data: "World"}
				close(ch)
			}()
			return ch, nil
		},
	}

	cfg := &config.Config{WorkerCount: 1}
	m := NewManager(cfg, s, mock)
	defer m.Stop()

	updateCh := m.HandleMessage("topic-1", "Hi", "chat-1", "thread-1")
	
	var responses []acp.StreamEvent
	for resp := range updateCh {
		responses = append(responses, resp)
	}

	if len(responses) != 2 {
		t.Fatalf("Expected 2 responses, got %d", len(responses))
	}

	if responses[0].Data != "Hello " {
		t.Errorf("Expected first response chunk 'Hello ', got '%s'", responses[0].Data)
	}
	if responses[1].Data != "World" {
		t.Errorf("Expected second response chunk 'World', got '%s'", responses[1].Data)
	}

	// Verify session was saved
	record, err := s.GetSessionRecord(context.Background(), "topic-1")
	if err != nil {
		t.Fatalf("Failed to get session from store: %v", err)
	}
	if record == nil || record.SessionID != "mock-session-id" {
		t.Errorf("Expected session ID 'mock-session-id', got '%v'", record)
	}
}

func TestManager_HashRouting(t *testing.T) {
	cfg := &config.Config{WorkerCount: 10}
	m := &Manager{cfg: cfg}

	topic1 := "topic-1"
	topic2 := "topic-2"
	topic3 := "topic-1" // Same as topic1

	idx1 := m.hashRouting(topic1)
	idx2 := m.hashRouting(topic2)
	idx3 := m.hashRouting(topic3)

	if idx1 < 0 || idx1 >= cfg.WorkerCount {
		t.Errorf("idx1 out of bounds: %d", idx1)
	}
	if idx2 < 0 || idx2 >= cfg.WorkerCount {
		t.Errorf("idx2 out of bounds: %d", idx2)
	}
	if idx1 != idx3 {
		t.Errorf("idx1 (%d) and idx3 (%d) should be equal for the same topic", idx1, idx3)
	}
}
