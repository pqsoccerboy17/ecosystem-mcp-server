# Ecosystem MCP Server

MCP server that provides unified control over the personal automation ecosystem. Lets Claude act as an operator, not just a reader.

## What This Does

This MCP server wraps and orchestrates all personal automation tools:

- **Downloads Organizer** - Trigger PDF and media organization
- **Treehouse Context Sync** - Sync Notion context to repos
- **Notion Rules** - Run tax document OCR pipeline
- **Monarch Money** - Financial data (wraps existing MCP)
- **System Health** - Monitor all automation systems

## Available Tools

| Tool | Description |
|------|-------------|
| `get_ecosystem_status()` | Health of all systems at a glance |
| `organize_downloads(type)` | Trigger file organization (pdf, media, all) |
| `sync_notion_context()` | Run treehouse-context-sync |
| `extract_tax_documents()` | Run notion-rules OCR pipeline |
| `get_financial_summary()` | Pull from Monarch Money |
| `search_documents(query)` | Search across Notion databases |
| `get_automation_history()` | Recent operations and results |
| `run_reconciliation()` | Verify all systems in sync |

## Installation

```bash
# Clone the repository
cd ~/Documents
git clone https://github.com/pqsoccerboy17/ecosystem-mcp-server.git
cd ecosystem-mcp-server

# Install with pip
pip install -e .
```

## Claude Desktop Configuration

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "ecosystem": {
      "command": "python",
      "args": ["-m", "ecosystem_mcp_server.server"],
      "cwd": "/Users/mdmac/Documents/ecosystem-mcp-server/src"
    }
  }
}
```

## Architecture

```
ecosystem-mcp-server
├── Wraps: downloads-organizer (PDFs + media)
├── Wraps: treehouse-context-sync
├── Wraps: notion-rules (tax OCR)
├── Wraps: monarch-mcp-server (financial)
└── Logs: SQLite history database
```

All operations are logged to `~/Library/Application Support/ecosystem-mcp-server/history.db` for tracking and debugging.

## Example Usage

In Claude:

```
"What's the status of my automation systems?"
→ Calls get_ecosystem_status()

"Organize my downloads"
→ Calls organize_downloads("all")

"Sync my Notion context"
→ Calls sync_notion_context()
```

## Development

```bash
# Run tests
pytest

# Format code
black src/

# Lint
ruff check src/
```

## License

MIT
