package store

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestStore_SessionManagement(t *testing.T) {
	dbPath := "test_store.db"
	defer os.Remove(dbPath)

	s, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to init store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	topicID := "topic-1"
	sessionID := "session-1"
	chatID := "chat-1"
	threadID := "thread-1"

	// 1. Get non-existent session
	res, err := s.GetSessionRecord(ctx, topicID)
	if err != nil {
		t.Fatalf("GetSessionRecord failed: %v", err)
	}
	if res != nil {
		t.Errorf("Expected nil record, got %v", res)
	}

	// 2. Save session
	if err := s.SaveSession(ctx, topicID, sessionID, chatID, threadID); err != nil {
		t.Fatalf("SaveSession failed: %v", err)
	}

	// 3. Get session
	res, err = s.GetSessionRecord(ctx, topicID)
	if err != nil {
		t.Fatalf("GetSessionRecord failed: %v", err)
	}
	if res.SessionID != sessionID {
		t.Errorf("Expected session ID %s, got %s", sessionID, res.SessionID)
	}
	if res.ChatID != chatID {
		t.Errorf("Expected chat ID %s, got %s", chatID, res.ChatID)
	}

	// 4. Update session
	newSessionID := "session-2"
	if err := s.SaveSession(ctx, topicID, newSessionID, chatID, threadID); err != nil {
		t.Fatalf("SaveSession failed: %v", err)
	}

	res, err = s.GetSessionRecord(ctx, topicID)
	if err != nil {
		t.Fatalf("GetSessionRecord failed: %v", err)
	}
	if res.SessionID != newSessionID {
		t.Errorf("Expected session ID %s, got %s", newSessionID, res.SessionID)
	}
}

func TestStore_Idempotency(t *testing.T) {
	dbPath := "test_idempotency.db"
	defer os.Remove(dbPath)

	s, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to init store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	msgID := "msg-1"

	// 1. Check if processed (should be false)
	processed, err := s.IsProcessed(ctx, msgID)
	if err != nil {
		t.Fatalf("IsProcessed failed: %v", err)
	}
	if processed {
		t.Errorf("Expected false, got true")
	}

	// 2. Mark processed
	if err := s.MarkProcessed(ctx, msgID); err != nil {
		t.Fatalf("MarkProcessed failed: %v", err)
	}

	// 3. Check again (should be true)
	processed, err = s.IsProcessed(ctx, msgID)
	if err != nil {
		t.Fatalf("IsProcessed failed: %v", err)
	}
	if !processed {
		t.Errorf("Expected true, got false")
	}

	// 4. Mark again (should be no-op/success)
	if err := s.MarkProcessed(ctx, msgID); err != nil {
		t.Fatalf("MarkProcessed failed: %v", err)
	}
}

func TestStore_CleanupExpired(t *testing.T) {
	dbPath := "test_cleanup.db"
	defer os.Remove(dbPath)

	s, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to init store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	
	s.SaveSession(ctx, "old", "old-sess", "c", "t")
	s.SaveSession(ctx, "new", "new-sess", "c", "t")

	// Update 'old' to be 10 days ago
	cutoff := time.Now().AddDate(0, 0, -10)
	_, err = s.db.Exec(`UPDATE sessions SET updated_at = ? WHERE topic_id = ?`, cutoff, "old")
	if err != nil {
		t.Fatalf("Failed to update old session: %v", err)
	}

	affected, err := s.CleanupExpired(ctx, 7)
	if err != nil {
		t.Fatalf("CleanupExpired failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("Expected 1 row affected, got %d", affected)
	}

	// Verify 'new' still exists
	res, _ := s.GetSessionRecord(ctx, "new")
	if res == nil || res.SessionID != "new-sess" {
		t.Errorf("New session should still exist")
	}

	// Verify 'old' is gone
	res, _ = s.GetSessionRecord(ctx, "old")
	if res != nil {
		t.Errorf("Old session should be deleted")
	}
}
