package acp

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	acpsdk "github.com/coder/acp-go-sdk"
)

type EventType string

const (
	EventTypeText  EventType = "text"
	EventTypeLog   EventType = "log"
	EventTypeDone  EventType = "done"
	EventTypeError EventType = "error"
)

type LogEntry struct {
	ID      string
	Tag     string // "thinking", "tool"
	Title   string
	Content string
	Status  string // "running", "success", "failed"
}

type StreamEvent struct {
	Type EventType
	Data string
	Log  *LogEntry
}

type Bridge interface {
	SendMessage(ctx context.Context, sessionID string, prompt ...acpsdk.ContentBlock) (<-chan StreamEvent, error)
	NewSession(ctx context.Context) (string, error)
	SupportsLoadSession() bool
	LoadSession(ctx context.Context, sessionID string) error
	Close() error
}

type bridgeClient struct {
	streams map[string]chan StreamEvent
	states  map[string]*sessionState
	mu      sync.RWMutex
}

type sessionState struct {
	currentLogID string
	isThinking   bool
}

func (c *bridgeClient) ReadTextFile(ctx context.Context, params acpsdk.ReadTextFileRequest) (acpsdk.ReadTextFileResponse, error) {
	return acpsdk.ReadTextFileResponse{}, fmt.Errorf("not implemented")
}
func (c *bridgeClient) WriteTextFile(ctx context.Context, params acpsdk.WriteTextFileRequest) (acpsdk.WriteTextFileResponse, error) {
	return acpsdk.WriteTextFileResponse{}, fmt.Errorf("not implemented")
}
func (c *bridgeClient) RequestPermission(ctx context.Context, params acpsdk.RequestPermissionRequest) (acpsdk.RequestPermissionResponse, error) {
	return acpsdk.RequestPermissionResponse{}, nil
}
func (c *bridgeClient) SessionUpdate(ctx context.Context, params acpsdk.SessionNotification) error {
	sessionID := string(params.SessionId)
	c.mu.Lock()
	ch, ok := c.streams[sessionID]
	state, stateOk := c.states[sessionID]
	if !stateOk {
		state = &sessionState{}
		c.states[sessionID] = state
	}
	c.mu.Unlock()

	if !ok {
		return nil
	}

	u := params.Update

	if u.AgentMessageChunk != nil && u.AgentMessageChunk.Content.Text != nil {
		if state.isThinking {
			ch <- StreamEvent{Type: EventTypeLog, Log: &LogEntry{
				ID:     state.currentLogID,
				Tag:    "thinking",
				Status: "success",
			}}
			state.isThinking = false
		}
		ch <- StreamEvent{Type: EventTypeText, Data: u.AgentMessageChunk.Content.Text.Text}
	} else if u.AgentThoughtChunk != nil && u.AgentThoughtChunk.Content.Text != nil {
		if !state.isThinking {
			state.currentLogID = fmt.Sprintf("think-%d", os.Getpid()+int(time.Now().UnixNano()))
			state.isThinking = true
			ch <- StreamEvent{Type: EventTypeLog, Log: &LogEntry{
				ID:     state.currentLogID,
				Tag:    "thinking",
				Status: "running",
			}}
		}
		ch <- StreamEvent{Type: EventTypeLog, Log: &LogEntry{
			ID:      state.currentLogID,
			Tag:     "thinking",
			Content: u.AgentThoughtChunk.Content.Text.Text,
			Status:  "running",
		}}
	} else if u.ToolCall != nil {
		if state.isThinking {
			ch <- StreamEvent{Type: EventTypeLog, Log: &LogEntry{
				ID:     state.currentLogID,
				Tag:    "thinking",
				Status: "success",
			}}
			state.isThinking = false
		}
		state.currentLogID = string(u.ToolCall.ToolCallId)
		ch <- StreamEvent{Type: EventTypeLog, Log: &LogEntry{
			ID:     state.currentLogID,
			Tag:    "tool",
			Title:  u.ToolCall.Title,
			Status: "running",
		}}
	} else if u.ToolCallUpdate != nil && u.ToolCallUpdate.Status != nil {
		status := *u.ToolCallUpdate.Status
		resStatus := "running"
		if status == acpsdk.ToolCallStatusCompleted {
			resStatus = "success"
		} else if string(status) == "failed" {
			resStatus = "failed"
		}
		title := ""
		if u.ToolCallUpdate.Title != nil {
			title = *u.ToolCallUpdate.Title
		}
		ch <- StreamEvent{Type: EventTypeLog, Log: &LogEntry{
			ID:     string(u.ToolCallUpdate.ToolCallId),
			Tag:    "tool",
			Title:  title,
			Status: resStatus,
		}}
	}

	return nil
}
func (c *bridgeClient) CreateTerminal(ctx context.Context, params acpsdk.CreateTerminalRequest) (acpsdk.CreateTerminalResponse, error) {
	return acpsdk.CreateTerminalResponse{}, fmt.Errorf("not implemented")
}
func (c *bridgeClient) KillTerminal(ctx context.Context, params acpsdk.KillTerminalRequest) (acpsdk.KillTerminalResponse, error) {
	return acpsdk.KillTerminalResponse{}, fmt.Errorf("not implemented")
}
func (c *bridgeClient) TerminalOutput(ctx context.Context, params acpsdk.TerminalOutputRequest) (acpsdk.TerminalOutputResponse, error) {
	return acpsdk.TerminalOutputResponse{}, fmt.Errorf("not implemented")
}
func (c *bridgeClient) ReleaseTerminal(ctx context.Context, params acpsdk.ReleaseTerminalRequest) (acpsdk.ReleaseTerminalResponse, error) {
	return acpsdk.ReleaseTerminalResponse{}, fmt.Errorf("not implemented")
}
func (c *bridgeClient) WaitForTerminalExit(ctx context.Context, params acpsdk.WaitForTerminalExitRequest) (acpsdk.WaitForTerminalExitResponse, error) {
	return acpsdk.WaitForTerminalExitResponse{}, fmt.Errorf("not implemented")
}

type bridgeImpl struct {
	cmd          *exec.Cmd
	client       *bridgeClient
	conn         *acpsdk.ClientSideConnection
	capabilities acpsdk.AgentCapabilities
}

// NewBridge starts the Agent subprocess and initializes the ACP connection via stdio.
func NewBridge(agentCmd string, args ...string) (Bridge, error) {
	cmd := exec.Command(agentCmd, args...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to get stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to get stdout pipe: %w", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to get stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start agent process: %w", err)
	}

	go func() {
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			log.Printf("[Agent] %s", scanner.Text())
		}
	}()

	client := &bridgeClient{
		streams: make(map[string]chan StreamEvent),
		states:  make(map[string]*sessionState),
	}
	conn := acpsdk.NewClientSideConnection(client, stdin, stdout)

	ctx := context.Background()
	initReq := acpsdk.InitializeRequest{
		ClientInfo: &acpsdk.Implementation{
			Name:    "gembot",
			Version: "1.0",
		},
		ClientCapabilities: acpsdk.ClientCapabilities{},
	}
	resp, err := conn.Initialize(ctx, initReq)
	if err != nil {
		cmd.Process.Kill()
		return nil, fmt.Errorf("initialize failed: %w", err)
	}

	b := &bridgeImpl{
		cmd:          cmd,
		client:       client,
		conn:         conn,
		capabilities: resp.AgentCapabilities,
	}

	return b, nil
}

func (b *bridgeImpl) SupportsLoadSession() bool {
	return b.capabilities.LoadSession
}

func (b *bridgeImpl) LoadSession(ctx context.Context, sessionID string) error {
	req := acpsdk.LoadSessionRequest{
		SessionId:  acpsdk.SessionId(sessionID),
		McpServers: []acpsdk.McpServer{},
	}
	_, err := b.conn.LoadSession(ctx, req)
	return err
}

func (b *bridgeImpl) NewSession(ctx context.Context) (string, error) {
	req := acpsdk.NewSessionRequest{
		McpServers: []acpsdk.McpServer{},
	}
	resp, err := b.conn.NewSession(ctx, req)
	if err != nil {
		return "", err
	}
	return string(resp.SessionId), nil
}

func (b *bridgeImpl) SendMessage(ctx context.Context, sessionID string, prompt ...acpsdk.ContentBlock) (<-chan StreamEvent, error) {
	ch := make(chan StreamEvent, 100)

	b.client.mu.Lock()
	b.client.streams[sessionID] = ch
	b.client.mu.Unlock()

	go func() {
		defer close(ch)
		defer func() {
			b.client.mu.Lock()
			delete(b.client.streams, sessionID)
			delete(b.client.states, sessionID)
			b.client.mu.Unlock()
		}()

		req := acpsdk.PromptRequest{
			SessionId: acpsdk.SessionId(sessionID),
			Prompt:    prompt,
		}
		_, err := b.conn.Prompt(ctx, req)
		if err != nil {
			ch <- StreamEvent{Type: EventTypeError, Data: err.Error()}
		}
		ch <- StreamEvent{Type: EventTypeDone, Data: ""}
	}()

	return ch, nil
}

func (b *bridgeImpl) Close() error {
	if b.cmd != nil && b.cmd.Process != nil {
		return b.cmd.Process.Kill()
	}
	return nil
}
