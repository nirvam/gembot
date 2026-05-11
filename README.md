# GemBot

GemBot is a high-performance Go-based bridge between IM platforms (like Feishu/Lark) and Agent Control Protocol (ACP) agents.

## Core Features
- **Multi-session Isolation**: Leverages Feishu Topics to map independent agent sessions.
- **Real-time Streaming**: Throttled UI updates for a smooth user experience.
- **Persistent Storage**: SQLite-backed session management with auto-cleanup.
- **Hash Routing**: Consistent routing ensures session state integrity.

## Quick Start
1. Copy `.env.example` to `.env` and fill in your Feishu App credentials.
2. Build the project: `go build -o gembot ./cmd/gembot`
3. Run: `./gembot`

## License
MIT - See [LICENSE](LICENSE) for details.
