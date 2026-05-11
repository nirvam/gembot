package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	acpsdk "github.com/coder/acp-go-sdk"
)

type simpleClient struct{}

func (c *simpleClient) SessionUpdate(ctx context.Context, params acpsdk.SessionNotification) error {
	if params.Update.AgentMessageChunk != nil && params.Update.AgentMessageChunk.Content.Text != nil {
		fmt.Printf("%s", params.Update.AgentMessageChunk.Content.Text.Text)
	}
	return nil
}

// Implement other required methods with nops
func (c *simpleClient) ReadTextFile(ctx context.Context, p acpsdk.ReadTextFileRequest) (acpsdk.ReadTextFileResponse, error) { return acpsdk.ReadTextFileResponse{}, nil }
func (c *simpleClient) WriteTextFile(ctx context.Context, p acpsdk.WriteTextFileRequest) (acpsdk.WriteTextFileResponse, error) { return acpsdk.WriteTextFileResponse{}, nil }
func (c *simpleClient) RequestPermission(ctx context.Context, p acpsdk.RequestPermissionRequest) (acpsdk.RequestPermissionResponse, error) { return acpsdk.RequestPermissionResponse{}, nil }
func (c *simpleClient) CreateTerminal(ctx context.Context, p acpsdk.CreateTerminalRequest) (acpsdk.CreateTerminalResponse, error) { return acpsdk.CreateTerminalResponse{}, nil }
func (c *simpleClient) KillTerminal(ctx context.Context, p acpsdk.KillTerminalRequest) (acpsdk.KillTerminalResponse, error) { return acpsdk.KillTerminalResponse{}, nil }
func (c *simpleClient) TerminalOutput(ctx context.Context, p acpsdk.TerminalOutputRequest) (acpsdk.TerminalOutputResponse, error) { return acpsdk.TerminalOutputResponse{}, nil }
func (c *simpleClient) ReleaseTerminal(ctx context.Context, p acpsdk.ReleaseTerminalRequest) (acpsdk.ReleaseTerminalResponse, error) { return acpsdk.ReleaseTerminalResponse{}, nil }
func (c *simpleClient) WaitForTerminalExit(ctx context.Context, p acpsdk.WaitForTerminalExitRequest) (acpsdk.WaitForTerminalExitResponse, error) { return acpsdk.WaitForTerminalExitResponse{}, nil }

func runACP(args []string, fn func(conn *acpsdk.ClientSideConnection) error) error {
	cmd := exec.Command("gemini", args...)
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return err
	}
	defer cmd.Process.Kill()

	client := &simpleClient{}
	conn := acpsdk.NewClientSideConnection(client, stdin, stdout)

	ctx := context.Background()
	_, err := conn.Initialize(ctx, acpsdk.InitializeRequest{
		ClientInfo: &acpsdk.Implementation{Name: "test", Version: "1.0"},
	})
	if err != nil {
		return err
	}

	return fn(conn)
}

func main() {
	var sessionID string
	secret := "MAGIC_PASSWORD_123"

	fmt.Println("=== Phase 1: Create Session and Tell Secret ===")
	err := runACP([]string{"--acp", "--yolo"}, func(conn *acpsdk.ClientSideConnection) error {
		resp, err := conn.NewSession(context.Background(), acpsdk.NewSessionRequest{
			McpServers: []acpsdk.McpServer{},
		})
		if err != nil {
			return err
		}
		sessionID = string(resp.SessionId)
		fmt.Printf("Created Session: %s\n", sessionID)

		fmt.Print("Agent: ")
		_, err = conn.Prompt(context.Background(), acpsdk.PromptRequest{
			SessionId: resp.SessionId,
			Prompt:    []acpsdk.ContentBlock{acpsdk.TextBlock("Remember this secret: " + secret + ". Just say OK.")},
		})
		fmt.Println()
		return err
	})

	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	fmt.Println("=== Phase 2: Resume Session and Ask for Secret ===")
	// Note: We try to pass --resume to the command line
	err = runACP([]string{"--acp", "--yolo", "--resume", sessionID}, func(conn *acpsdk.ClientSideConnection) error {
		// Even with --resume, we likely still need a NewSession call or just try to use the old ID?
		// Usually ACP expects a NewSession, but let's see if the Agent allows using the resumed ID.
		fmt.Printf("Attempting to use Session: %s\n", sessionID)

		fmt.Print("Agent: ")
		_, err = conn.Prompt(context.Background(), acpsdk.PromptRequest{
			SessionId: acpsdk.SessionId(sessionID),
			Prompt:    []acpsdk.ContentBlock{acpsdk.TextBlock("What was the secret I told you?")},
		})
		fmt.Println()
		return err
	})

	if err != nil {
		if strings.Contains(err.Error(), "Session not found") {
			fmt.Println("RESULT: Session NOT found after resume. gemini-cli does not seem to support session resumption in ACP mode via --resume flag.")
		} else {
			fmt.Printf("RESULT: Failed with error: %v\n", err)
		}
	} else {
		fmt.Println("RESULT: Success? Check the agent output above.")
	}
}
