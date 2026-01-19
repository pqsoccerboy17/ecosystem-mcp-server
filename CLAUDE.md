# Ecosystem MCP Server

## Project Overview
MCP server that provides unified control over the personal automation ecosystem. Acts as an orchestration layer that wraps and coordinates all automation tools.

## About the Developer
- **Non-developer user** - I rely on Claude Code to write, test, and manage code
- Always explain what you're doing in plain English before executing
- Prefer small, incremental changes that can be easily reviewed
- Ask for confirmation before any destructive or irreversible actions

## Tech Stack
- **Language**: Python 3.10+
- **MCP Framework**: FastMCP from `mcp` library
- **Database**: SQLite for operation history
- **Wrapped Tools**: downloads-organizer, treehouse-context-sync, notion-rules, monarch-mcp-server

## Key Commands
```bash
# Test server imports
python -c "from ecosystem_mcp_server import server; print('OK')"

# Run server directly (for testing)
python -m ecosystem_mcp_server.server

# View operation history
sqlite3 ~/Library/Application\ Support/ecosystem-mcp-server/history.db "SELECT * FROM operations ORDER BY timestamp DESC LIMIT 10"
```

## File Structure
```
src/ecosystem_mcp_server/
├── __init__.py
├── server.py           # Main MCP server with all tools
├── monarch_sync.py     # Monarch → Notion transaction sync module
├── daily_briefing.py   # Daily briefing generation
├── notion_control.py   # Notion automation request polling
└── (future modules as needed)
```

## Tool Implementation Pattern
Each tool follows this pattern:
1. Log start time
2. Execute operation (call wrapped tool/script)
3. Log result to SQLite
4. Return JSON result

## Database Schema
```sql
CREATE TABLE operations (
    id INTEGER PRIMARY KEY,
    timestamp TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    parameters TEXT,
    result TEXT,
    success INTEGER NOT NULL,
    duration_ms INTEGER
);
```

## Wrapped Components
| Component | Location | What It Does |
|-----------|----------|--------------|
| downloads-organizer | ~/dev/automation/downloads-organizer | PDF + media organization |
| treehouse-context-sync | ~/dev/notion/treehouse-context-sync | Notion → repo sync |
| notion-rules | ~/dev/notion/notion-rules | Tax document OCR |
| monarch-mcp-server | ~/dev/automation/monarch-mcp-server | Financial data |
| monarch_sync | (internal module) | Monarch → Notion transaction sync |

## Development Workflow
1. **Understand** - Explain what needs to be done
2. **Plan** - Show approach before coding
3. **Implement** - Make small, focused changes
4. **Test** - Verify tool works before moving on
5. **Commit** - Use clear, descriptive commit messages

## Safety Rules
- **Never** delete files without confirmation
- **Always** log operations to SQLite
- **Always** return structured JSON from tools
- **Prefer** dry-run modes when available
