package core

type MessageBlockType string

const (
	BlockTypeText         MessageBlockType = "text"
	BlockTypeImage        MessageBlockType = "image"
	BlockTypeAudio        MessageBlockType = "audio"
	BlockTypeResource     MessageBlockType = "resource"
	BlockTypeResourceLink MessageBlockType = "resource_link"
)

type MessageBlock struct {
	Type     MessageBlockType
	Text     string // For text block
	Data     string // Base64 encoded data for image/audio/resource
	MimeType string // For image/audio/resource
	URI      string // For resource link / resource
	Name     string // For resource link
}

type Message struct {
	Blocks []MessageBlock
}
