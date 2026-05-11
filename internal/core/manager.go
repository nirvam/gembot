package core

import (
	"context"
	"hash/fnv"
	"log"
	"sync"
	"time"

	"github.com/nirvam/gembot/internal/acp"
	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/nirvam/gembot/internal/config"
	"github.com/nirvam/gembot/internal/store"
)

// HistoryProvider defines the interface for retrieving chat history.
type HistoryProvider interface {
	GetHistory(ctx context.Context, chatID, threadID, topicID string) ([]acpsdk.ContentBlock, error)
}

// Adapter defines the abstract interface for an IM platform integration.
type Adapter interface {
	// Start connects to the IM platform and begins listening for events.
	Start(ctx context.Context) error

	// Reply sends a reply to a specific message (initial response).
	Reply(ctx context.Context, msgID string, content string) (replyMsgID string, err error)

	// Patch updates an existing message (used for streaming updates).
	Patch(ctx context.Context, msgID string, content string, logs []*acp.LogEntry) error

	// FormatMarkdown formats text for the specific platform's markdown flavor.
	FormatMarkdown(text string) string

	// HistoryProvider ensures the adapter can provide context.
	HistoryProvider
}

type Manager struct {
	cfg          *config.Config
	store        *store.Store
	bridge       acp.Bridge
	adapter      Adapter
	workers      []chan *Task
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	liveSessions sync.Map // topicID -> bool
}

type Task struct {
	TopicID  string
	Message  string
	ChatID   string
	ThreadID string
	UpdateCh chan<- acp.StreamEvent
}

func NewManager(cfg *config.Config, s *store.Store, b acp.Bridge) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		cfg:     cfg,
		store:   s,
		bridge:  b,
		workers: make([]chan *Task, cfg.WorkerCount),
		ctx:     ctx,
		cancel:  cancel,
	}

	for i := 0; i < cfg.WorkerCount; i++ {
		m.workers[i] = make(chan *Task, 100)
		m.wg.Add(1)
		go m.workerLoop(i, m.workers[i])
	}

	m.wg.Add(1)
	go m.cleanupLoop()

	return m
}

func (m *Manager) cleanupLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	// Run once on start
	m.doCleanup()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.doCleanup()
		}
	}
}

func (m *Manager) doCleanup() {
	affected, err := m.store.CleanupExpired(m.ctx, m.cfg.SessionRetentionDays)
	if err != nil {
		log.Printf("Failed to cleanup expired sessions: %v", err)
	} else if affected > 0 {
		log.Printf("Cleaned up %d expired sessions", affected)
	}
}

// hashRouting implements FNV-1a hash routing.
func (m *Manager) hashRouting(topicID string) int {
	h := fnv.New32a()
	h.Write([]byte(topicID))
	return int(h.Sum32()) % m.cfg.WorkerCount
}

func (m *Manager) workerLoop(id int, ch <-chan *Task) {
	defer m.wg.Done()
	for {
		select {
		case <-m.ctx.Done():
			return
		case task := <-ch:
			m.processTask(task)
		}
	}
}

func (m *Manager) SetAdapter(a Adapter) {
	m.adapter = a
}

func (m *Manager) processTask(task *Task) {
	defer close(task.UpdateCh)
	// 1. Get or create session
	record, err := m.store.GetSessionRecord(m.ctx, task.TopicID)
	if err != nil {
		log.Printf("Error getting session for topic %s: %v", task.TopicID, err)
		task.UpdateCh <- acp.StreamEvent{Type: acp.EventTypeText, Data: "Error: Internal server error"}
		return
	}

	var sessionID string
	var needsSync bool

	if record == nil {
		// New session
		var err error
		sessionID, err = m.bridge.NewSession(m.ctx)
		if err != nil {
			log.Printf("Error creating new session for topic %s: %v", task.TopicID, err)
			task.UpdateCh <- acp.StreamEvent{Type: acp.EventTypeText, Data: "Error: Agent unavailable"}
			return
		}
		// If it's a new topic to us, but it might have history in Feishu (Full Mode)
		needsSync = true
	} else {
		sessionID = record.SessionID
		
		// Check if we need to sync (first time in this process)
		if _, loaded := m.liveSessions.LoadOrStore(task.TopicID, true); !loaded {
			needsSync = true
			// The agent process is fresh, so the session ID from the DB is likely invalid
			// for this agent instance. We must create a new one.
			var err error
			sessionID, err = m.bridge.NewSession(m.ctx)
			if err != nil {
				log.Printf("Error creating replacement session for topic %s: %v", task.TopicID, err)
				// Fallback: we'll continue with the old ID, but it will likely fail in SendMessage
			}
		}
	}

	// Save or refresh session record
	if err := m.store.SaveSession(m.ctx, task.TopicID, sessionID, task.ChatID, task.ThreadID); err != nil {
		log.Printf("Error saving session record for topic %s: %v", task.TopicID, err)
	}

	var prompts []acpsdk.ContentBlock
	if needsSync && m.adapter != nil {
		history, err := m.adapter.GetHistory(m.ctx, task.ChatID, task.ThreadID, task.TopicID)
		if err != nil {
			log.Printf("Failed to get history for topic %s: %v", task.TopicID, err)
		} else if len(history) > 0 {
			log.Printf("Syncing %d historical blocks for topic %s", len(history), task.TopicID)
			prompts = append(prompts, history...)
		}
	}
	prompts = append(prompts, acpsdk.TextBlock(task.Message))

	// 2. Call ACP Bridge
	stream, err := m.bridge.SendMessage(m.ctx, sessionID, prompts...)
	if err != nil {
		log.Printf("Error sending message to ACP: %v", err)
		task.UpdateCh <- acp.StreamEvent{Type: acp.EventTypeText, Data: "Error: Agent unavailable"}
		return
	}

	// 3. Consume stream and push updates
	for event := range stream {
		task.UpdateCh <- event
	}
}

func (m *Manager) HandleMessage(topicID, message, chatID, threadID string) <-chan acp.StreamEvent {
	workerIdx := m.hashRouting(topicID)
	updateCh := make(chan acp.StreamEvent, 100)
	
	m.workers[workerIdx] <- &Task{
		TopicID:  topicID,
		Message:  message,
		ChatID:   chatID,
		ThreadID: threadID,
		UpdateCh: updateCh,
	}

	return updateCh
}

func (m *Manager) Context() context.Context {
	return m.ctx
}

func (m *Manager) Stop() {
	m.cancel()
	m.wg.Wait()
	m.bridge.Close()
	m.store.Close()
}
