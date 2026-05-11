package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// SessionRecord maps a Feishu topic ID to an ACP session ID and metadata.
type SessionRecord struct {
	TopicID   string
	SessionID string
	ChatID    string
	ThreadID  string
	UpdatedAt time.Time
}

type Store struct {
	db *sql.DB
}

// NewStore initializes a new SQLite store and creates necessary tables.
func NewStore(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	s := &Store{db: db}
	if err := s.initSchema(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Store) initSchema() error {
	query := `
	CREATE TABLE IF NOT EXISTS sessions (
		topic_id TEXT PRIMARY KEY,
		session_id TEXT NOT NULL,
		chat_id TEXT,
		thread_id TEXT,
		updated_at DATETIME NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_updated_at ON sessions(updated_at);

	CREATE TABLE IF NOT EXISTS processed_messages (
		message_id TEXT PRIMARY KEY,
		created_at DATETIME NOT NULL
	);
	`
	_, err := s.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to init schema: %w", err)
	}

	// Simple migration: add missing columns if they don't exist
	columns := []string{"chat_id", "thread_id"}
	for _, col := range columns {
		if !s.columnExists("sessions", col) {
			alterQuery := fmt.Sprintf("ALTER TABLE sessions ADD COLUMN %s TEXT", col)
			if _, err := s.db.Exec(alterQuery); err != nil {
				return fmt.Errorf("failed to add column %s: %w", col, err)
			}
		}
	}

	return nil
}

func (s *Store) columnExists(table, column string) bool {
	query := fmt.Sprintf("PRAGMA table_info(%s)", table)
	rows, err := s.db.Query(query)
	if err != nil {
		return false
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid       int
			name      string
			ctype     string
			notnull   int
			dfltValue interface{}
			pk        int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err != nil {
			continue
		}
		if name == column {
			return true
		}
	}
	return false
}

// IsProcessed checks if a message has already been processed.
func (s *Store) IsProcessed(ctx context.Context, messageID string) (bool, error) {
	var exists int
	query := `SELECT 1 FROM processed_messages WHERE message_id = ?`
	err := s.db.QueryRowContext(ctx, query, messageID).Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("failed to check processed message: %w", err)
	}
	return true, nil
}

// MarkProcessed marks a message as processed.
func (s *Store) MarkProcessed(ctx context.Context, messageID string) error {
	query := `INSERT OR IGNORE INTO processed_messages (message_id, created_at) VALUES (?, ?)`
	_, err := s.db.ExecContext(ctx, query, messageID, time.Now())
	if err != nil {
		return fmt.Errorf("failed to mark message as processed: %w", err)
	}
	return nil
}

// GetSessionRecord retrieves the session record for a given Feishu topic ID.
func (s *Store) GetSessionRecord(ctx context.Context, topicID string) (*SessionRecord, error) {
	var r SessionRecord
	query := `SELECT topic_id, session_id, chat_id, thread_id, updated_at FROM sessions WHERE topic_id = ?`
	err := s.db.QueryRowContext(ctx, query, topicID).Scan(&r.TopicID, &r.SessionID, &r.ChatID, &r.ThreadID, &r.UpdatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get session record: %w", err)
	}
	return &r, nil
}

// SaveSession saves or updates the ACP session ID and metadata for a given Feishu topic ID.
func (s *Store) SaveSession(ctx context.Context, topicID, sessionID, chatID, threadID string) error {
	query := `
	INSERT INTO sessions (topic_id, session_id, chat_id, thread_id, updated_at) 
	VALUES (?, ?, ?, ?, ?)
	ON CONFLICT(topic_id) DO UPDATE SET 
		session_id = excluded.session_id,
		chat_id = COALESCE(excluded.chat_id, sessions.chat_id),
		thread_id = COALESCE(excluded.thread_id, sessions.thread_id),
		updated_at = excluded.updated_at
	`
	_, err := s.db.ExecContext(ctx, query, topicID, sessionID, chatID, threadID, time.Now())
	if err != nil {
		return fmt.Errorf("failed to save session: %w", err)
	}
	return nil
}

// CleanupExpired deletes sessions that haven't been updated within retentionDays.
func (s *Store) CleanupExpired(ctx context.Context, retentionDays int) (int64, error) {
	cutoff := time.Now().AddDate(0, 0, -retentionDays)
	query := `DELETE FROM sessions WHERE updated_at < ?`
	res, err := s.db.ExecContext(ctx, query, cutoff)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup expired sessions: %w", err)
	}
	return res.RowsAffected()
}

// Close closes the database connection.
func (s *Store) Close() error {
	return s.db.Close()
}
