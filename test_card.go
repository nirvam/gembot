package main

import (
	"encoding/json"
	"fmt"
)

type LogEntry struct {
	ID      string
	Tag     string
	Title   string
	Content string
	Status  string
}

func buildCardContent(text string, logs []*LogEntry) string {
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
								"tag": "tag",
								"text": map[string]interface{}{
									"tag":     "plain_text",
									"content": tagText,
								},
								"color": color,
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
			"tag":      "collapsible_panel",
			"expanded": false,
			"header": map[string]interface{}{
				"title": map[string]interface{}{
					"tag":     "plain_text",
					"content": fmt.Sprintf("执行详情 (%d 条记录)", len(logs)),
				},
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
	b, _ := json.MarshalIndent(card, "", "  ")
	return string(b)
}

func main() {
	fmt.Println(buildCardContent("Hello world", []*LogEntry{
		{ID: "1", Tag: "thinking", Content: "Thinking...", Status: "success"},
	}))
}
