package im

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/larksuite/oapi-sdk-go/v3/ws"
	"github.com/nirvam/gembot/internal/acp"
	"github.com/nirvam/gembot/internal/core"
	"github.com/nirvam/gembot/internal/store"
)

type FeishuAdapter struct {
	client   *lark.Client
	wsClient *ws.Client
	manager  *core.Manager
	store    *store.Store
}

func buildCardContent(text string, logs []*acp.LogEntry) string {
	elements := []interface{}{
		map[string]interface{}{
			"tag":     "markdown",
			"content": text,
		},
	}

	if len(logs) > 0 {
		logElements := []interface{}{}
		for _, l := range logs {
			color := "grey"
			tagText := "未知"

			if l.Tag == "thinking" {
				tagText = "思考中"
				color = "blue"
			} else if l.Tag == "tool" {
				tagText = "执行指令"
				color = "purple"
			}

			if l.Status == "success" {
				color = "green"
			} else if l.Status == "failed" {
				color = "red"
			} else {
				color = "neutral"
			}

			content := l.Title
			if l.Tag == "thinking" {
				content = l.Content
				if len(content) > 100 {
					content = content[:100] + "..."
				}
				if content == "" {
					content = "正在构思..."
				}
			}

			logElements = append(logElements, map[string]interface{}{
				"tag": "column_set",
				"columns": []interface{}{
					map[string]interface{}{
						"tag":   "column",
						"width": "auto",
						"elements": []interface{}{
							map[string]interface{}{
								"tag":     "markdown",
								"content": fmt.Sprintf("<text_tag color='%s'>%s</text_tag>", color, tagText),
							},
						},
					},
					map[string]interface{}{
						"tag":    "column",
						"width":  "weighted",
						"weight": 1,
						"elements": []interface{}{
							map[string]interface{}{
								"tag":     "markdown",
								"content": content,
							},
						},
					},
				},
			})
		}

		elements = append(elements, map[string]interface{}{
			"tag":              "collapsible_panel",
			"element_id":       "execution_details",
			"expanded":         false,
			"background_color": "neutral",
			"vertical_spacing": "8px",
			"padding":          "8px 8px 8px 8px",
			"header": map[string]interface{}{
				"title": map[string]interface{}{
					"tag":     "markdown",
					"content": fmt.Sprintf("**执行详情 (%d 条记录)**", len(logs)),
				},
				"vertical_align": "center",
				"icon": map[string]interface{}{
					"tag":   "standard_icon",
					"token": "down-small-ccm_outlined",
					"size":  "16px 16px",
				},
				"icon_position":       "right",
				"icon_expanded_angle": -180,
			},
			"elements": logElements,
		})
	}

	card := map[string]interface{}{
		"schema": "2.0",
		"config": map[string]interface{}{
			"width_mode": "fill",
		},
		"body": map[string]interface{}{
			"elements": elements,
		},
	}
	b, _ := json.Marshal(card)
	return string(b)
}

func NewFeishuAdapter(appID, appSecret, verificationToken, encryptKey string, manager *core.Manager, s *store.Store) core.Adapter {
	client := lark.NewClient(appID, appSecret)

	a := &FeishuAdapter{
		client:  client,
		manager: manager,
		store:   s,
	}

	handler := dispatcher.NewEventDispatcher(verificationToken, encryptKey).
		OnP2MessageReceiveV1(func(ctx context.Context, event *larkim.P2MessageReceiveV1) error {
			if event.Event == nil || event.Event.Message == nil {
				return nil
			}

			msg := event.Event.Message
			msgID := *msg.MessageId

			// Idempotency check
			processed, err := a.store.IsProcessed(ctx, msgID)
			if err != nil {
				log.Printf("Failed to check if message is processed: %v", err)
				return err
			}
			if processed {
				log.Printf("Message %s already processed, skipping", msgID)
				return nil
			}
			if err := a.store.MarkProcessed(ctx, msgID); err != nil {
				log.Printf("Failed to mark message as processed: %v", err)
			}

			topicID := ""
			if msg.RootId != nil && *msg.RootId != "" {
				topicID = *msg.RootId
			} else {
				topicID = msgID
			}

			// Parse content
			var contentObj struct {
				Text string `json:"text"`
			}
			var textContent string
			if msg.Content != nil {
				if err := json.Unmarshal([]byte(*msg.Content), &contentObj); err == nil {
					textContent = contentObj.Text
				} else {
					textContent = *msg.Content
				}
			}

			log.Printf("Received message for topic %s: %s", topicID, textContent)

			// Send immediate acknowledgment
			replyMsgID, err := a.Reply(ctx, msgID, "⏳ 正在思考...")
			if err != nil {
				log.Printf("Failed to reply initial message: %v", err)
				return err
			}

			log.Printf("Created immediate reply with ID: %s", replyMsgID)

			chatID := *msg.ChatId
			threadID := ""
			if msg.ThreadId != nil {
				threadID = *msg.ThreadId
			}

			// Start streaming updates
			go a.streamUpdates(a.manager.Context(), topicID, textContent, msgID, chatID, threadID, replyMsgID)

			return nil
		}).
		OnP2MessageReadV1(func(ctx context.Context, event *larkim.P2MessageReadV1) error {
			return nil
		})

	a.wsClient = ws.NewClient(appID, appSecret, ws.WithEventHandler(handler))

	return a
}

func (a *FeishuAdapter) streamUpdates(ctx context.Context, topicID, message, messageID, chatID, threadID, replyMsgID string) {
	updateCh := a.manager.HandleMessage(topicID, message, messageID, chatID, threadID)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastPatchedText string
	var currentText string
	var logs []*acp.LogEntry
	logMap := make(map[string]int) // ID -> index in logs

	for {
		select {
		case event, ok := <-updateCh:
			if !ok {
				if currentText != lastPatchedText && currentText != "" {
					a.Patch(ctx, replyMsgID, currentText, logs)
				}
				return
			}
			if event.Type == acp.EventTypeText {
				currentText += event.Data
			} else if event.Type == acp.EventTypeLog {
				entry := event.Log
				if idx, found := logMap[entry.ID]; found {
					// Update existing log
					if entry.Content != "" {
						logs[idx].Content += entry.Content
					}
					if entry.Status != "" {
						logs[idx].Status = entry.Status
					}
					if entry.Title != "" {
						logs[idx].Title = entry.Title
					}
				} else {
					// Add new log
					logMap[entry.ID] = len(logs)
					logs = append(logs, entry)
				}
			}
		case <-ticker.C:
			if (currentText != lastPatchedText && currentText != "") || len(logMap) > 0 {
				if err := a.Patch(ctx, replyMsgID, currentText, logs); err != nil {
					log.Printf("Failed to patch message: %v", err)
				}
				lastPatchedText = currentText
			}
		case <-ctx.Done():
			return
		}
	}
}

func (a *FeishuAdapter) Reply(ctx context.Context, msgID string, text string) (string, error) {
	content := buildCardContent(text, nil)
	req := larkim.NewReplyMessageReqBuilder().
		MessageId(msgID).
		Body(larkim.NewReplyMessageReqBodyBuilder().
			Content(content).
			MsgType("interactive").
			Build()).
		Build()

	resp, err := a.client.Im.Message.Reply(ctx, req)
	if err != nil {
		return "", err
	}
	if !resp.Success() {
		return "", fmt.Errorf("reply failed: %s", resp.Msg)
	}

	return *resp.Data.MessageId, nil
}

func (a *FeishuAdapter) Patch(ctx context.Context, messageID string, text string, logs []*acp.LogEntry) error {
	content := buildCardContent(text, logs)
	log.Printf("Patching message %s with content: %s", messageID, content)
	req := larkim.NewPatchMessageReqBuilder().
		MessageId(messageID).
		Body(larkim.NewPatchMessageReqBodyBuilder().
			Content(content).
			Build()).
		Build()

	resp, err := a.client.Im.Message.Patch(ctx, req)
	if err != nil {
		return err
	}
	if !resp.Success() {
		return fmt.Errorf("patch failed, code: %d, msg: %s", resp.Code, resp.Msg)
	}
	return nil
}

func (a *FeishuAdapter) FormatMarkdown(text string) string {
	// For Feishu, we wrap it in our card content JSON
	return buildCardContent(text, nil)
}

func (a *FeishuAdapter) Start(ctx context.Context) error {
	log.Printf("Starting Feishu WebSocket client...")
	return a.wsClient.Start(ctx)
}
