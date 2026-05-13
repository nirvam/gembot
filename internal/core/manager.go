package core

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"runtime/debug"
	"sync"
	"time"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/nirvam/gembot/internal/acp"
	"github.com/nirvam/gembot/internal/config"
	"github.com/nirvam/gembot/internal/store"
)

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
	TopicID   string
	Message   Message
	MessageID string
	ChatID    string
	ThreadID  string
	UpdateCh  chan<- acp.StreamEvent
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
	
	// Fail-safe: panic recovery
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in processTask: %v\n%s", r, debug.Stack())
			task.UpdateCh <- acp.StreamEvent{Type: acp.EventTypeText, Data: fmt.Sprintf("\n❌ 处理失败: 内部异常 (%v)", r)}
		}
	}()

	// 1. Get or create session
	record, err := m.store.GetSessionRecord(m.ctx, task.TopicID)
	if err != nil {
		log.Printf("Error getting session for topic %s: %v", task.TopicID, err)
		task.UpdateCh <- acp.StreamEvent{Type: acp.EventTypeText, Data: "\n❌ 处理失败: 无法读取会话记录"}
		return
	}

	var sessionID string

	if record == nil {
		// New session or thread we haven't tracked yet
		var err error
		sessionID, err = m.bridge.NewSession(m.ctx)
		if err != nil {
			log.Printf("Error creating new session for topic %s: %v", task.TopicID, err)
			task.UpdateCh <- acp.StreamEvent{Type: acp.EventTypeText, Data: fmt.Sprintf("\n❌ 处理失败: Agent 会话创建失败 (%v)", err)}
			return
		}
	} else {
		sessionID = record.SessionID

		// Check if we need to restore context (first time in this process for this topic)
		if _, loaded := m.liveSessions.LoadOrStore(task.TopicID, true); !loaded {
			if m.bridge.SupportsLoadSession() {
				// Try to load session via ACP
				err := m.bridge.LoadSession(m.ctx, sessionID)
				if err != nil {
					log.Printf("Failed to load session %s: %v", sessionID, err)
					task.UpdateCh <- acp.StreamEvent{Type: acp.EventTypeText, Data: "\n❌ 该会话已过期或加载失败，请重新开启新话题。"}
					m.liveSessions.Delete(task.TopicID) // Remove from live sessions so we don't assume it's valid
					return
				}
			} else {
				log.Printf("Session load requested but not supported for topic %s", task.TopicID)
				task.UpdateCh <- acp.StreamEvent{Type: acp.EventTypeText, Data: "\n❌ 该会话已过期，且底层不支持恢复，请重新开启新话题。"}
				m.liveSessions.Delete(task.TopicID)
				return
			}
		}
	}

	// Save or refresh session record
	if err := m.store.SaveSession(m.ctx, task.TopicID, sessionID, task.ChatID, task.ThreadID); err != nil {
		log.Printf("Error saving session record for topic %s: %v", task.TopicID, err)
	}

	var prompts []acpsdk.ContentBlock
	for _, block := range task.Message.Blocks {
		switch block.Type {
		case BlockTypeText:
			prompts = append(prompts, acpsdk.TextBlock(block.Text))
		case BlockTypeImage:
			prompts = append(prompts, acpsdk.ImageBlock(block.Data, block.MimeType))
		case BlockTypeAudio:
			prompts = append(prompts, acpsdk.AudioBlock(block.Data, block.MimeType))
		case BlockTypeResourceLink:
			prompts = append(prompts, acpsdk.ResourceLinkBlock(block.Name, block.URI))
		case BlockTypeResource:
			mimeType := block.MimeType
			prompts = append(prompts, acpsdk.ResourceBlock(acpsdk.EmbeddedResourceResource{
				BlobResourceContents: &acpsdk.BlobResourceContents{
					Uri:      block.URI,
					MimeType: &mimeType,
					Blob:     block.Data,
				},
			}))
		}
	}

	// 2. Call ACP Bridge
	stream, err := m.bridge.SendMessage(m.ctx, sessionID, prompts...)
	if err != nil {
		log.Printf("Error sending message to ACP: %v", err)
		task.UpdateCh <- acp.StreamEvent{Type: acp.EventTypeText, Data: fmt.Sprintf("\n❌ 处理失败: Agent 通信异常 (%v)", err)}
		return
	}

	// 3. Consume stream and push updates
	for event := range stream {
		task.UpdateCh <- event
	}
}

func (m *Manager) HandleMessage(topicID string, message Message, messageID, chatID, threadID string) <-chan acp.StreamEvent {
	workerIdx := m.hashRouting(topicID)
	updateCh := make(chan acp.StreamEvent, 100)

	m.workers[workerIdx] <- &Task{
		TopicID:   topicID,
		Message:   message,
		MessageID: messageID,
		ChatID:    chatID,
		ThreadID:  threadID,
		UpdateCh:  updateCh,
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
