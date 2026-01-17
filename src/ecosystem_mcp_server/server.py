"""
Ecosystem MCP Server - Main server implementation.

This MCP server provides unified control over the personal automation ecosystem:
- Downloads organizer (PDFs + media)
- Treehouse context sync
- Notion rules (tax OCR)
- Monarch Money (via existing MCP)
- System health monitoring

All operations are logged to SQLite for history tracking.
"""

import json
import logging
import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastMCP server
mcp = FastMCP("Ecosystem MCP Server")

# =============================================================================
# Configuration
# =============================================================================

HOME = Path.home()
DOCUMENTS = HOME / "Documents"

# Repository paths
REPOS = {
    "downloads_organizer": DOCUMENTS / "downloads-organizer",
    "tax_organizer": DOCUMENTS / "tax-pdf-organizer",
    "media_organizer": DOCUMENTS / "media-organizer",
    "monarch_mcp": DOCUMENTS / "monarch-mcp-server",
    "context_sync": DOCUMENTS / "treehouse-context-sync",
    "notion_rules": DOCUMENTS / "notion-rules",
}

# Log files
LOGS = {
    "tax_schedule": HOME / "tax_organizer_schedule.log",
    "tax_watcher": HOME / "tax_organizer_watcher.log",
    "downloads_organizer": HOME / "downloads_organizer.log",
}

# Database for operation history
DB_PATH = HOME / "Library/Application Support/ecosystem-mcp-server/history.db"

# LaunchAgent identifiers
LAUNCHAGENTS = {
    "tax_schedule": "com.taxorganizer.schedule",
    "tax_watcher": "com.taxorganizer.watcher",
}

# Monarch session file
MONARCH_SESSION = HOME / "Library/Application Support/monarch-mcp-server/mm_session.pickle"


# =============================================================================
# Database Setup
# =============================================================================

def init_database():
    """Initialize SQLite database for operation history."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS operations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            tool_name TEXT NOT NULL,
            parameters TEXT,
            result TEXT,
            success INTEGER NOT NULL,
            duration_ms INTEGER
        )
    """)

    conn.commit()
    conn.close()


def log_operation(tool_name: str, parameters: Dict, result: str, success: bool, duration_ms: int = 0):
    """Log an operation to the database."""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO operations (timestamp, tool_name, parameters, result, success, duration_ms)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            datetime.now().isoformat(),
            tool_name,
            json.dumps(parameters),
            result[:1000] if result else None,  # Truncate long results
            1 if success else 0,
            duration_ms
        ))

        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to log operation: {e}")


# Initialize database on module load
init_database()


# =============================================================================
# Utility Functions
# =============================================================================

def format_time_ago(dt: datetime) -> str:
    """Format a datetime as 'X minutes/hours/days ago'."""
    now = datetime.now()
    delta = now - dt

    if delta.total_seconds() < 60:
        return "just now"
    elif delta.total_seconds() < 3600:
        mins = int(delta.total_seconds() / 60)
        return f"{mins} minute{'s' if mins != 1 else ''} ago"
    elif delta.total_seconds() < 86400:
        hours = int(delta.total_seconds() / 3600)
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    else:
        days = int(delta.total_seconds() / 86400)
        return f"{days} day{'s' if days != 1 else ''} ago"


def get_file_mtime(path: Path) -> Optional[datetime]:
    """Get file modification time."""
    try:
        if path.exists():
            return datetime.fromtimestamp(path.stat().st_mtime)
    except Exception:
        pass
    return None


def get_launchctl_status(label: str) -> tuple:
    """Check if a LaunchAgent is loaded and get its PID."""
    try:
        result = subprocess.run(
            ["launchctl", "list"],
            capture_output=True,
            text=True,
            timeout=5
        )
        for line in result.stdout.splitlines():
            if label in line:
                parts = line.split()
                if len(parts) >= 2:
                    pid = parts[0]
                    if pid != "-" and pid.isdigit():
                        return True, int(pid)
                    return True, None
        return False, None
    except Exception:
        return False, None


def count_files_in_downloads(extensions: List[str]) -> int:
    """Count files with given extensions in Downloads."""
    downloads = HOME / "Downloads"
    count = 0
    try:
        for ext in extensions:
            count += len(list(downloads.glob(f"*.{ext}")))
            count += len(list(downloads.glob(f"*.{ext.upper()}")))
    except Exception:
        pass
    return count


def run_command(cmd: List[str], cwd: Optional[Path] = None, timeout: int = 300) -> tuple:
    """Run a command and return (success, stdout, stderr)."""
    try:
        result = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Command timed out"
    except Exception as e:
        return False, "", str(e)


# =============================================================================
# Status Checking Functions
# =============================================================================

def check_downloads_organizer() -> Dict[str, Any]:
    """Check downloads-organizer status."""
    status = {
        "name": "Downloads Organizer",
        "icon": "📥",
        "status": "unknown",
        "details": [],
        "attention": [],
    }

    repo = REPOS["downloads_organizer"]
    if not repo.exists():
        status["status"] = "not_installed"
        status["details"].append("Repository not found")
        return status

    status["status"] = "installed"
    status["details"].append(f"Location: {repo}")

    # Check pending files
    pdf_count = count_files_in_downloads(["pdf"])
    media_exts = ["jpg", "jpeg", "png", "heic", "mov", "mp4", "mp3", "m4a"]
    media_count = count_files_in_downloads(media_exts)

    if pdf_count > 0:
        status["attention"].append(f"{pdf_count} PDFs pending")
    if media_count > 0:
        status["attention"].append(f"{media_count} media files pending")

    status["pending_pdfs"] = pdf_count
    status["pending_media"] = media_count

    return status


def check_tax_organizer() -> Dict[str, Any]:
    """Check tax-pdf-organizer status (legacy)."""
    status = {
        "name": "Tax PDF Organizer (Legacy)",
        "icon": "📁",
        "status": "unknown",
        "details": [],
        "attention": [],
    }

    # Check LaunchAgents
    watcher_loaded, watcher_pid = get_launchctl_status(LAUNCHAGENTS["tax_watcher"])
    schedule_loaded, _ = get_launchctl_status(LAUNCHAGENTS["tax_schedule"])

    if watcher_loaded and watcher_pid:
        status["status"] = "watching"
        status["details"].append(f"Watcher running (PID {watcher_pid})")
    elif watcher_loaded:
        status["status"] = "loaded"
        status["details"].append("Watcher loaded but not running")
    else:
        status["status"] = "not_running"
        status["details"].append("Watcher not loaded")

    if schedule_loaded:
        status["details"].append("Scheduler loaded")

    return status


def check_monarch_money() -> Dict[str, Any]:
    """Check Monarch Money MCP Server status."""
    status = {
        "name": "Monarch Money",
        "icon": "💰",
        "status": "unknown",
        "details": [],
        "attention": [],
    }

    # Check session file
    if MONARCH_SESSION.exists():
        mtime = get_file_mtime(MONARCH_SESSION)
        if mtime:
            age_days = (datetime.now() - mtime).days
            status["last_activity"] = mtime.isoformat()
            status["details"].append(f"Session: {format_time_ago(mtime)}")

            if age_days > 7:
                status["status"] = "stale"
                status["attention"].append("Session may need refresh (>7 days old)")
            else:
                status["status"] = "connected"
    else:
        status["status"] = "not_authenticated"
        status["attention"].append("Run login_setup.py to authenticate")

    return status


def check_context_sync() -> Dict[str, Any]:
    """Check treehouse-context-sync status."""
    status = {
        "name": "Context Sync",
        "icon": "🔄",
        "status": "unknown",
        "details": [],
        "attention": [],
    }

    repo = REPOS["context_sync"]
    if not repo.exists():
        status["status"] = "not_installed"
        status["details"].append("Repository not found")
        return status

    # Check CHANGELOG.md for last sync
    changelog = repo / "docs/context/CHANGELOG.md"
    if changelog.exists():
        mtime = get_file_mtime(changelog)
        if mtime:
            status["last_activity"] = mtime.isoformat()
            age_hours = (datetime.now() - mtime).total_seconds() / 3600
            status["details"].append(f"Last sync: {format_time_ago(mtime)}")

            if age_hours > 36:
                status["status"] = "stale"
                status["attention"].append("Sync may be stale (>36 hours)")
            else:
                status["status"] = "synced"
    else:
        status["status"] = "not_configured"
        status["details"].append("CHANGELOG.md not found")

    return status


def check_notion_rules() -> Dict[str, Any]:
    """Check notion-rules (Tax OCR) status."""
    status = {
        "name": "Notion Rules (Tax OCR)",
        "icon": "📄",
        "status": "idle",
        "details": [],
        "attention": [],
    }

    repo = REPOS["notion_rules"]
    if not repo.exists():
        status["status"] = "not_installed"
        status["details"].append("Repository not found")
        return status

    # Check checkpoint file
    checkpoint = repo / "tax-years/data/processing_checkpoint.json"
    if checkpoint.exists():
        mtime = get_file_mtime(checkpoint)
        if mtime:
            status["last_activity"] = mtime.isoformat()
            status["details"].append(f"Last run: {format_time_ago(mtime)}")

        # Try to read checkpoint for pending items
        try:
            with open(checkpoint) as f:
                data = json.load(f)
                if "results" in data:
                    needs_review = sum(
                        1 for r in data["results"]
                        if r.get("needs_review", False)
                    )
                    if needs_review > 0:
                        status["attention"].append(f"{needs_review} documents need review")
        except Exception:
            pass

    return status


# =============================================================================
# MCP Tools
# =============================================================================

@mcp.tool()
def get_ecosystem_status() -> str:
    """
    Get comprehensive status of all automation systems.

    Returns health and status information for:
    - Downloads Organizer (PDFs + media)
    - Tax PDF Organizer (legacy watcher)
    - Monarch Money connection
    - Treehouse Context Sync
    - Notion Rules (Tax OCR)

    Also reports pending files and attention items.
    """
    start_time = datetime.now()

    try:
        # Collect all statuses
        checks = [
            check_downloads_organizer(),
            check_tax_organizer(),
            check_monarch_money(),
            check_context_sync(),
            check_notion_rules(),
        ]

        # Build result
        result = {
            "timestamp": datetime.now().isoformat(),
            "systems": checks,
            "attention_items": [],
            "summary": {
                "total_systems": len(checks),
                "healthy": 0,
                "needs_attention": 0,
                "not_running": 0,
            }
        }

        # Aggregate attention items and count statuses
        for check in checks:
            for item in check.get("attention", []):
                result["attention_items"].append(f"{check['icon']} {check['name']}: {item}")

            status = check.get("status", "unknown")
            if status in ["watching", "connected", "synced", "installed"]:
                result["summary"]["healthy"] += 1
            elif status in ["stale", "loaded"]:
                result["summary"]["needs_attention"] += 1
            else:
                result["summary"]["not_running"] += 1

        # Log operation
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        log_operation("get_ecosystem_status", {}, json.dumps(result["summary"]), True, duration_ms)

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        error_msg = f"Error getting ecosystem status: {str(e)}"
        log_operation("get_ecosystem_status", {}, error_msg, False)
        return json.dumps({"error": error_msg})


@mcp.tool()
def get_automation_history(limit: int = 20) -> str:
    """
    Get recent automation operation history.

    Args:
        limit: Number of recent operations to return (default: 20)

    Returns log of recent tool invocations with results.
    """
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()

        cursor.execute("""
            SELECT timestamp, tool_name, parameters, result, success, duration_ms
            FROM operations
            ORDER BY timestamp DESC
            LIMIT ?
        """, (limit,))

        rows = cursor.fetchall()
        conn.close()

        operations = []
        for row in rows:
            operations.append({
                "timestamp": row[0],
                "tool": row[1],
                "parameters": json.loads(row[2]) if row[2] else None,
                "result": row[3],
                "success": bool(row[4]),
                "duration_ms": row[5]
            })

        return json.dumps({"operations": operations, "count": len(operations)}, indent=2)

    except Exception as e:
        return json.dumps({"error": f"Failed to get history: {str(e)}"})


@mcp.tool()
def organize_downloads(file_type: str = "all", dry_run: bool = False) -> str:
    """
    Trigger file organization from Downloads folder.

    Args:
        file_type: Type of files to organize - "pdf", "media", or "all" (default: "all")
        dry_run: If True, preview what would be organized without moving files

    Returns:
        Result of the organization operation including files moved.
    """
    start_time = datetime.now()
    params = {"file_type": file_type, "dry_run": dry_run}

    try:
        repo = REPOS["downloads_organizer"]
        if not repo.exists():
            error_msg = f"downloads-organizer not found at {repo}"
            log_operation("organize_downloads", params, error_msg, False)
            return json.dumps({"error": error_msg})

        results = {
            "file_type": file_type,
            "dry_run": dry_run,
            "pdf": None,
            "media": None,
        }

        # Run PDF organizer
        if file_type in ["pdf", "all"]:
            cmd = [sys.executable, "-m", "downloads_organizer", "pdf"]
            if dry_run:
                cmd.append("--dry-run")
            else:
                cmd.append("--yes")

            success, stdout, stderr = run_command(cmd, cwd=repo / "src", timeout=300)
            results["pdf"] = {
                "success": success,
                "output": stdout[-2000:] if stdout else None,  # Last 2000 chars
                "error": stderr if not success else None,
            }

        # Run media organizer
        if file_type in ["media", "all"]:
            cmd = [sys.executable, "-m", "downloads_organizer", "media"]
            if dry_run:
                cmd.append("--dry-run")
            else:
                cmd.append("--yes")

            success, stdout, stderr = run_command(cmd, cwd=repo / "src", timeout=600)
            results["media"] = {
                "success": success,
                "output": stdout[-2000:] if stdout else None,
                "error": stderr if not success else None,
            }

        # Check remaining files
        results["remaining"] = {
            "pdfs": count_files_in_downloads(["pdf"]),
            "media": count_files_in_downloads(["jpg", "jpeg", "png", "heic", "mov", "mp4", "mp3"]),
        }

        # Determine overall success
        overall_success = True
        if results["pdf"] and not results["pdf"]["success"]:
            overall_success = False
        if results["media"] and not results["media"]["success"]:
            overall_success = False

        # Log operation
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        log_operation("organize_downloads", params, json.dumps(results["remaining"]), overall_success, duration_ms)

        return json.dumps(results, indent=2, default=str)

    except Exception as e:
        error_msg = f"Error organizing downloads: {str(e)}"
        log_operation("organize_downloads", params, error_msg, False)
        return json.dumps({"error": error_msg})


@mcp.tool()
def sync_notion_context() -> str:
    """
    Trigger treehouse-context-sync to sync Notion context to repositories.

    Syncs the latest Notion database content to the treehouse-context-sync repo,
    making it available for Claude Code sessions.

    Returns:
        Result of the sync operation.
    """
    start_time = datetime.now()

    try:
        repo = REPOS["context_sync"]
        if not repo.exists():
            error_msg = f"treehouse-context-sync not found at {repo}"
            log_operation("sync_notion_context", {}, error_msg, False)
            return json.dumps({"error": error_msg})

        # Run the sync script
        sync_script = repo / "sync.py"
        if not sync_script.exists():
            # Try alternative locations
            sync_script = repo / "src/sync.py"

        if not sync_script.exists():
            error_msg = "sync.py not found in treehouse-context-sync"
            log_operation("sync_notion_context", {}, error_msg, False)
            return json.dumps({"error": error_msg})

        success, stdout, stderr = run_command(
            [sys.executable, str(sync_script)],
            cwd=repo,
            timeout=300
        )

        result = {
            "success": success,
            "output": stdout[-2000:] if stdout else None,
            "error": stderr if not success else None,
        }

        # Check last sync time
        changelog = repo / "docs/context/CHANGELOG.md"
        if changelog.exists():
            mtime = get_file_mtime(changelog)
            if mtime:
                result["last_sync"] = mtime.isoformat()

        # Log operation
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        log_operation("sync_notion_context", {}, "success" if success else stderr, success, duration_ms)

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        error_msg = f"Error syncing context: {str(e)}"
        log_operation("sync_notion_context", {}, error_msg, False)
        return json.dumps({"error": error_msg})


# =============================================================================
# Server Entry Point
# =============================================================================

def main():
    """Main entry point for the server."""
    logger.info("Starting Ecosystem MCP Server...")
    try:
        mcp.run()
    except Exception as e:
        logger.error(f"Failed to run server: {str(e)}")
        raise


# Export for mcp run
app = mcp

if __name__ == "__main__":
    main()
