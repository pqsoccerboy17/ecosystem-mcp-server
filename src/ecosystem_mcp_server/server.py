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
from datetime import datetime, timedelta
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
    # New integrations (Jan 2026)
    "notebooklm_mcp": HOME / "dev/tools/notebooklm-mcp",
    "ai_code_connect": HOME / "dev/tools/ai-code-connect",
    "google_workspace_mcp": HOME / "dev/automation/google-workspace-mcp",
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

# Monarch session file (legacy)
MONARCH_SESSION = HOME / "Library/Application Support/monarch-mcp-server/mm_session.pickle"

# Monarch health report file (new proactive monitoring)
MONARCH_HEALTH_REPORT = HOME / ".monarch-mcp/health_report.json"
MONARCH_SESSION_FILE = HOME / ".monarch-mcp/session.json"
MONARCH_TOKEN_FILE = HOME / ".monarch-mcp/token"


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

    # New table for Monarch health check history
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS monarch_health_checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            status TEXT NOT NULL,
            session_valid INTEGER,
            session_age_days REAL,
            api_reachable INTEGER,
            error_message TEXT,
            library_version TEXT,
            update_available INTEGER
        )
    """)

    conn.commit()
    conn.close()


def log_monarch_health_check(health_data: Dict[str, Any]) -> None:
    """Log a Monarch health check to the database for trend analysis."""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO monarch_health_checks
            (timestamp, status, session_valid, session_age_days, api_reachable, error_message, library_version, update_available)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now().isoformat(),
            health_data.get("status", "unknown"),
            1 if health_data.get("session_valid") else 0,
            health_data.get("session_age_days"),
            1 if health_data.get("api_reachable") else 0,
            health_data.get("error_message"),
            health_data.get("library_version"),
            1 if health_data.get("update_available") else 0,
        ))

        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to log Monarch health check: {e}")


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
    """Check Monarch Money MCP Server status using health report file."""
    status = {
        "name": "Monarch Money",
        "icon": "💰",
        "status": "unknown",
        "details": [],
        "attention": [],
    }

    # First, try to read the health report file (proactive monitoring)
    health_report = None
    if MONARCH_HEALTH_REPORT.exists():
        try:
            with open(MONARCH_HEALTH_REPORT) as f:
                health_report = json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read Monarch health report: {e}")

    if health_report:
        # Use the health report data (real API validation)
        report_status = health_report.get("status", "unknown")
        status["health_report"] = health_report

        if report_status == "healthy":
            status["status"] = "connected"
            status["details"].append("API verified healthy")
        elif report_status == "degraded":
            status["status"] = "degraded"
            if health_report.get("recommendation"):
                status["attention"].append(health_report["recommendation"])
        elif report_status == "unhealthy":
            status["status"] = "unhealthy"
            if health_report.get("error_message"):
                status["details"].append(f"Error: {health_report['error_message']}")
            if health_report.get("recommendation"):
                status["attention"].append(health_report["recommendation"])
        else:
            status["status"] = "unknown"

        # Add session age info
        if health_report.get("session_age_days") is not None:
            age_days = health_report["session_age_days"]
            status["session_age_days"] = age_days
            status["details"].append(f"Session age: {age_days:.1f} days")

        # Add API reachability info
        if health_report.get("api_reachable"):
            status["details"].append("API reachable: Yes")
        elif "api_reachable" in health_report:
            status["details"].append("API reachable: No")

        # Add library version info
        if health_report.get("library_version"):
            status["library_version"] = health_report["library_version"]
        if health_report.get("update_available"):
            status["attention"].append(
                f"Library update available: {health_report.get('library_version')} → {health_report.get('latest_library_version')}"
            )

        # Record check time
        if health_report.get("last_check"):
            status["last_check"] = health_report["last_check"]

    else:
        # Fallback to legacy session file check
        has_session = MONARCH_SESSION_FILE.exists() or MONARCH_TOKEN_FILE.exists()

        if has_session:
            # Check session file modification time
            session_file = MONARCH_SESSION_FILE if MONARCH_SESSION_FILE.exists() else MONARCH_TOKEN_FILE
            mtime = get_file_mtime(session_file)
            if mtime:
                age_days = (datetime.now() - mtime).days
                status["last_activity"] = mtime.isoformat()
                status["details"].append(f"Session file: {format_time_ago(mtime)}")

                if age_days > 14:
                    status["status"] = "likely_expired"
                    status["attention"].append("Session likely expired (>14 days old)")
                elif age_days > 10:
                    status["status"] = "stale"
                    status["attention"].append("Session may need refresh (>10 days old)")
                else:
                    status["status"] = "unknown"
                    status["details"].append("No health report - status unverified")
        elif MONARCH_SESSION.exists():
            # Legacy pickle session
            mtime = get_file_mtime(MONARCH_SESSION)
            if mtime:
                age_days = (datetime.now() - mtime).days
                status["last_activity"] = mtime.isoformat()
                status["details"].append(f"Legacy session: {format_time_ago(mtime)}")

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
# New Tool Status Checks (Jan 2026)
# =============================================================================

def check_notebooklm() -> Dict[str, Any]:
    """Check NotebookLM MCP status."""
    status = {
        "name": "NotebookLM",
        "icon": "📓",
        "status": "unknown",
        "details": [],
        "attention": [],
    }

    auth_file = HOME / ".notebooklm-mcp/auth.json"
    repo = REPOS.get("notebooklm_mcp")

    if repo and repo.exists():
        status["details"].append(f"Location: {repo}")
        if auth_file.exists():
            status["status"] = "authenticated"
            mtime = get_file_mtime(auth_file)
            if mtime:
                status["details"].append(f"Auth: {format_time_ago(mtime)}")
        else:
            status["status"] = "not_authenticated"
            status["attention"].append("Run 'uv run notebooklm-mcp auth' to authenticate")
    else:
        status["status"] = "not_installed"
        status["details"].append("Repository not found")

    return status


def check_google_workspace() -> Dict[str, Any]:
    """Check Google Workspace MCP status."""
    status = {
        "name": "Google Workspace",
        "icon": "📧",
        "status": "unknown",
        "details": [],
        "attention": [],
    }

    token_file = HOME / ".config/g-workspace-mcp/token.json"
    repo = REPOS.get("google_workspace_mcp")

    if repo and repo.exists():
        status["details"].append(f"Location: {repo}")
        status["details"].append("Mode: read-only")
        if token_file.exists():
            status["status"] = "connected"
            mtime = get_file_mtime(token_file)
            if mtime:
                status["details"].append(f"Token: {format_time_ago(mtime)}")
        else:
            status["status"] = "not_authenticated"
            status["attention"].append("Run OAuth setup to connect Google account")
    else:
        status["status"] = "not_installed"
        status["details"].append("Repository not found")

    return status


def check_ai_code_connect() -> Dict[str, Any]:
    """Check ai-code-connect status."""
    import shutil

    status = {
        "name": "AI Code Connect",
        "icon": "🔄",
        "status": "unknown",
        "details": [],
        "attention": [],
    }

    repo = REPOS.get("ai_code_connect")
    gemini_available = shutil.which("gemini") is not None

    if repo and repo.exists():
        status["details"].append(f"Location: {repo}")

        if gemini_available:
            status["status"] = "ready"
            status["details"].append("Gemini CLI: installed")
        else:
            status["status"] = "gemini_missing"
            status["attention"].append("Install Gemini CLI: brew install gemini-cli")
    else:
        status["status"] = "not_installed"
        status["details"].append("Repository not found")

    return status


def check_statusline() -> Dict[str, Any]:
    """Check Claude Code statusline configuration."""
    status = {
        "name": "Statusline",
        "icon": "📊",
        "status": "unknown",
        "details": [],
        "attention": [],
    }

    script_path = HOME / ".claude/statusline-command.sh"
    settings_path = HOME / ".claude/settings.json"

    if script_path.exists():
        status["status"] = "configured"
        status["details"].append("Script installed")

        # Check if configured in settings
        if settings_path.exists():
            try:
                with open(settings_path) as f:
                    settings = json.load(f)
                    if "statusLine" in settings:
                        status["details"].append("Enabled in settings.json")
            except Exception:
                pass
    else:
        status["status"] = "not_configured"
        status["attention"].append("Install statusline script to ~/.claude/")

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
    - NotebookLM MCP (NEW)
    - Google Workspace MCP (NEW)
    - AI Code Connect (NEW)
    - Claude Code Statusline (NEW)

    Also reports pending files and attention items.
    """
    start_time = datetime.now()

    try:
        # Collect all statuses (original + new tools)
        checks = [
            check_downloads_organizer(),
            check_tax_organizer(),
            check_monarch_money(),
            check_context_sync(),
            check_notion_rules(),
            # New tools (Jan 2026)
            check_notebooklm(),
            check_google_workspace(),
            check_ai_code_connect(),
            check_statusline(),
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

        # Run media organizer (--no-audit for speed, skip recursive folder scan)
        if file_type in ["media", "all"]:
            cmd = [sys.executable, "-m", "downloads_organizer", "media", "--no-audit"]
            if dry_run:
                cmd.append("--dry-run")
            else:
                cmd.append("--yes")

            success, stdout, stderr = run_command(cmd, cwd=repo / "src", timeout=60)
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


@mcp.tool()
def extract_tax_documents() -> str:
    """
    Run the notion-rules tax document OCR pipeline.

    Processes tax documents through OCR extraction and updates Notion database
    with extracted information (dates, amounts, categories).

    Returns:
        Result of the extraction operation.
    """
    start_time = datetime.now()

    try:
        repo = REPOS["notion_rules"]
        if not repo.exists():
            error_msg = f"notion-rules not found at {repo}"
            log_operation("extract_tax_documents", {}, error_msg, False)
            return json.dumps({"error": error_msg})

        # Find the main extraction script
        extract_script = repo / "tax-years/extract_tax_data.py"
        if not extract_script.exists():
            extract_script = repo / "extract.py"

        if not extract_script.exists():
            error_msg = "Tax extraction script not found in notion-rules"
            log_operation("extract_tax_documents", {}, error_msg, False)
            return json.dumps({"error": error_msg})

        success, stdout, stderr = run_command(
            [sys.executable, str(extract_script)],
            cwd=repo,
            timeout=600  # 10 minutes for OCR processing
        )

        result = {
            "success": success,
            "output": stdout[-2000:] if stdout else None,
            "error": stderr if not success else None,
        }

        # Check checkpoint for results
        checkpoint = repo / "tax-years/data/processing_checkpoint.json"
        if checkpoint.exists():
            try:
                with open(checkpoint) as f:
                    data = json.load(f)
                    result["processed"] = len(data.get("results", []))
                    result["needs_review"] = sum(
                        1 for r in data.get("results", [])
                        if r.get("needs_review", False)
                    )
            except Exception:
                pass

        # Log operation
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        log_operation("extract_tax_documents", {}, "success" if success else stderr, success, duration_ms)

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        error_msg = f"Error extracting tax documents: {str(e)}"
        log_operation("extract_tax_documents", {}, error_msg, False)
        return json.dumps({"error": error_msg})


@mcp.tool()
def get_financial_summary(days: int = 30) -> str:
    """
    Get financial summary from Monarch Money.

    Note: This tool provides instructions for using Monarch Money MCP directly,
    as financial data requires the specialized Monarch MCP server.

    Args:
        days: Number of days to summarize (default: 30)

    Returns:
        Instructions for accessing Monarch Money data.
    """
    start_time = datetime.now()
    params = {"days": days}

    try:
        # Check Monarch session status
        session_status = "unknown"
        if MONARCH_SESSION.exists():
            mtime = get_file_mtime(MONARCH_SESSION)
            if mtime:
                age_days = (datetime.now() - mtime).days
                session_status = "connected" if age_days <= 7 else "stale"
        else:
            session_status = "not_authenticated"

        result = {
            "monarch_status": session_status,
            "instructions": """
To get financial data, use the Monarch Money MCP server tools directly:

1. get_accounts() - List all financial accounts
2. get_transactions(limit=100, start_date="YYYY-MM-DD") - Get recent transactions
3. get_budgets() - View budget information
4. get_cashflow(start_date="YYYY-MM-DD") - Get cashflow analysis

If session is stale, run: python ~/Documents/monarch-mcp-server/login_setup.py
            """.strip(),
            "quick_commands": [
                "get_accounts()",
                f"get_transactions(limit=100)",
                "get_budgets()",
                "get_cashflow()",
            ]
        }

        if session_status == "not_authenticated":
            result["attention"] = "Monarch Money not authenticated. Run login_setup.py first."
        elif session_status == "stale":
            result["attention"] = "Monarch session may be stale. Consider re-authenticating."

        # Log operation
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        log_operation("get_financial_summary", params, session_status, True, duration_ms)

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        error_msg = f"Error getting financial summary: {str(e)}"
        log_operation("get_financial_summary", params, error_msg, False)
        return json.dumps({"error": error_msg})


@mcp.tool()
def validate_monarch_connection() -> str:
    """
    Perform an on-demand validation of the Monarch Money connection.

    This reads the health report from the Monarch MCP server and logs
    the result for trend analysis. Use this to check if Monarch is
    working without making API calls through the Monarch MCP server.

    Returns:
        Current health status with details.
    """
    start_time = datetime.now()

    try:
        # Read health report
        if not MONARCH_HEALTH_REPORT.exists():
            result = {
                "status": "no_health_report",
                "message": "No health report found. The Monarch MCP server may not have started yet.",
                "recommendation": "Start the Monarch MCP server or run login_setup.py",
            }
            log_operation("validate_monarch_connection", {}, "no_health_report", False)
            return json.dumps(result, indent=2)

        with open(MONARCH_HEALTH_REPORT) as f:
            health_data = json.load(f)

        # Log to database for trend analysis
        log_monarch_health_check(health_data)

        # Build response
        status = health_data.get("status", "unknown")
        result = {
            "status": status,
            "session_valid": health_data.get("session_valid"),
            "session_age_days": health_data.get("session_age_days"),
            "api_reachable": health_data.get("api_reachable"),
            "last_check": health_data.get("last_check"),
        }

        if health_data.get("error_message"):
            result["error_message"] = health_data["error_message"]
        if health_data.get("recommendation"):
            result["recommendation"] = health_data["recommendation"]

        # Library info
        if health_data.get("library_version"):
            result["library_version"] = health_data["library_version"]
        if health_data.get("update_available"):
            result["update_available"] = True
            result["latest_version"] = health_data.get("latest_library_version")

        # Add interpretation
        if status == "healthy":
            result["interpretation"] = "Monarch Money is working correctly"
        elif status == "degraded":
            result["interpretation"] = "Monarch Money is working but may need attention soon"
        elif status == "unhealthy":
            result["interpretation"] = "Monarch Money is not working - action required"
        else:
            result["interpretation"] = "Monarch Money status cannot be determined"

        # Log operation
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        log_operation("validate_monarch_connection", {}, status, status == "healthy", duration_ms)

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        error_msg = f"Error validating Monarch connection: {str(e)}"
        log_operation("validate_monarch_connection", {}, error_msg, False)
        return json.dumps({"error": error_msg})


@mcp.tool()
def get_monarch_health_history(days: int = 7, limit: int = 50) -> str:
    """
    Get historical Monarch Money health check data for trend analysis.

    Useful for identifying patterns in connection issues, session expiry,
    and API availability over time.

    Args:
        days: Number of days of history to retrieve (default: 7)
        limit: Maximum number of records to return (default: 50)

    Returns:
        Historical health check data with trend analysis.
    """
    start_time = datetime.now()
    params = {"days": days, "limit": limit}

    try:
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()

        # Get recent health checks
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()

        cursor.execute("""
            SELECT timestamp, status, session_valid, session_age_days, api_reachable, error_message, library_version, update_available
            FROM monarch_health_checks
            WHERE timestamp > ?
            ORDER BY timestamp DESC
            LIMIT ?
        """, (cutoff, limit))

        rows = cursor.fetchall()
        conn.close()

        # Build history
        history = []
        for row in rows:
            history.append({
                "timestamp": row[0],
                "status": row[1],
                "session_valid": bool(row[2]),
                "session_age_days": row[3],
                "api_reachable": bool(row[4]),
                "error_message": row[5],
                "library_version": row[6],
                "update_available": bool(row[7]),
            })

        # Calculate trend metrics
        if history:
            healthy_count = sum(1 for h in history if h["status"] == "healthy")
            unhealthy_count = sum(1 for h in history if h["status"] == "unhealthy")
            api_failures = sum(1 for h in history if not h["api_reachable"])

            trend = {
                "total_checks": len(history),
                "healthy_count": healthy_count,
                "unhealthy_count": unhealthy_count,
                "api_failure_count": api_failures,
                "health_rate": round(healthy_count / len(history) * 100, 1) if history else 0,
                "api_success_rate": round((len(history) - api_failures) / len(history) * 100, 1) if history else 0,
            }

            # Identify patterns
            if unhealthy_count > len(history) * 0.3:
                trend["pattern"] = "frequent_issues"
                trend["recommendation"] = "Consider re-authenticating or checking for library updates"
            elif api_failures > len(history) * 0.2:
                trend["pattern"] = "api_instability"
                trend["recommendation"] = "API has been intermittently unreachable"
            else:
                trend["pattern"] = "stable"
                trend["recommendation"] = None
        else:
            trend = {
                "total_checks": 0,
                "message": "No health check history available",
            }

        result = {
            "period_days": days,
            "trend": trend,
            "history": history[:20],  # Return last 20 for readability
        }

        # Log operation
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        log_operation("get_monarch_health_history", params, f"{len(history)} records", True, duration_ms)

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        error_msg = f"Error getting health history: {str(e)}"
        log_operation("get_monarch_health_history", params, error_msg, False)
        return json.dumps({"error": error_msg})


@mcp.tool()
def run_reconciliation() -> str:
    """
    Verify all automation systems are in sync.

    Performs comprehensive checks:
    - All repos exist and are on main branch
    - No uncommitted changes in repos
    - All LaunchAgents are healthy
    - Session files are valid
    - No stale data

    Returns:
        Reconciliation report with any issues found.
    """
    start_time = datetime.now()

    try:
        issues = []
        checks = []

        # Check all repos
        for name, path in REPOS.items():
            repo_check = {"repo": name, "path": str(path), "status": "unknown", "issues": []}

            if not path.exists():
                repo_check["status"] = "missing"
                repo_check["issues"].append("Repository not found")
                issues.append(f"{name}: Repository not found")
            else:
                # Check if it's a git repo
                git_dir = path / ".git"
                if git_dir.exists():
                    # Check for uncommitted changes
                    success, stdout, _ = run_command(
                        ["git", "status", "--porcelain"],
                        cwd=path,
                        timeout=10
                    )
                    if success and stdout.strip():
                        repo_check["status"] = "dirty"
                        repo_check["issues"].append("Uncommitted changes")
                        issues.append(f"{name}: Has uncommitted changes")
                    else:
                        repo_check["status"] = "clean"

                    # Check current branch
                    success, stdout, _ = run_command(
                        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                        cwd=path,
                        timeout=10
                    )
                    if success:
                        branch = stdout.strip()
                        repo_check["branch"] = branch
                        if branch != "main":
                            repo_check["issues"].append(f"Not on main branch (on {branch})")
                            issues.append(f"{name}: On branch {branch}, not main")
                else:
                    repo_check["status"] = "not_git"
                    repo_check["issues"].append("Not a git repository")

            checks.append(repo_check)

        # Check LaunchAgents
        for name, label in LAUNCHAGENTS.items():
            loaded, pid = get_launchctl_status(label)
            if not loaded:
                issues.append(f"LaunchAgent {name}: Not loaded")

        # Check Monarch session
        if MONARCH_SESSION.exists():
            mtime = get_file_mtime(MONARCH_SESSION)
            if mtime:
                age_days = (datetime.now() - mtime).days
                if age_days > 7:
                    issues.append("Monarch session is stale (>7 days old)")

        result = {
            "timestamp": datetime.now().isoformat(),
            "repos": checks,
            "issues": issues,
            "status": "healthy" if not issues else "issues_found",
            "issue_count": len(issues),
        }

        # Log operation
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        log_operation("run_reconciliation", {}, f"{len(issues)} issues", len(issues) == 0, duration_ms)

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        error_msg = f"Error running reconciliation: {str(e)}"
        log_operation("run_reconciliation", {}, error_msg, False)
        return json.dumps({"error": error_msg})


# =============================================================================
# Notion Control Plane Tools
# =============================================================================

@mcp.tool()
def get_pending_requests() -> str:
    """
    Get pending automation requests from Notion Control Plane.

    Returns queued requests that are waiting to be processed.
    These requests can be created from any device via the Notion app.

    Returns:
        List of pending requests with their details.
    """
    start_time = datetime.now()

    try:
        from . import notion_control

        requests = notion_control.get_pending_requests()

        result = {
            "pending_count": len(requests),
            "requests": requests,
        }

        if not requests:
            result["message"] = "No pending requests"

        # Log operation
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        log_operation("get_pending_requests", {}, f"{len(requests)} pending", True, duration_ms)

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        error_msg = f"Error getting pending requests: {str(e)}"
        log_operation("get_pending_requests", {}, error_msg, False)
        return json.dumps({"error": error_msg})


@mcp.tool()
def process_automation_request(request_id: str) -> str:
    """
    Manually process a specific automation request from Notion.

    Args:
        request_id: The Notion page ID of the request to process

    Returns:
        Result of processing the request.
    """
    start_time = datetime.now()
    params = {"request_id": request_id}

    try:
        from . import notion_control

        # Get the request details
        client = notion_control.get_notion_client()
        if not client:
            return json.dumps({"error": "Notion client not available"})

        page = client.pages.retrieve(page_id=request_id)
        request = notion_control.parse_request_page(page)

        if not request:
            return json.dumps({"error": "Failed to parse request"})

        # Mark as running
        notion_control.update_request_status(request_id, notion_control.STATUS_RUNNING)

        # Execute
        success, result_msg = notion_control.execute_request(request)

        # Update status
        if success:
            notion_control.update_request_status(
                request_id,
                notion_control.STATUS_DONE,
                result=result_msg
            )
        else:
            notion_control.update_request_status(
                request_id,
                notion_control.STATUS_FAILED,
                result=result_msg
            )

        result = {
            "request": request,
            "success": success,
            "result": result_msg,
        }

        # Log operation
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        log_operation("process_automation_request", params, result_msg, success, duration_ms)

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        error_msg = f"Error processing request: {str(e)}"
        log_operation("process_automation_request", params, error_msg, False)
        return json.dumps({"error": error_msg})


@mcp.tool()
def setup_notion_control_plane(parent_page_id: Optional[str] = None, database_id: Optional[str] = None) -> str:
    """
    Setup or configure the Notion Control Plane.

    Either creates a new Automation Requests database or configures an existing one.

    Args:
        parent_page_id: If provided, creates a new database under this page
        database_id: If provided, uses this existing database ID

    Returns:
        Setup result with database ID.
    """
    start_time = datetime.now()
    params = {"parent_page_id": parent_page_id, "database_id": database_id}

    try:
        from . import notion_control

        if database_id:
            # Just save the database ID
            config = notion_control.load_config()
            config["automation_requests_db_id"] = database_id
            notion_control.save_config(config)

            result = {
                "success": True,
                "message": "Database ID configured",
                "database_id": database_id,
            }

        elif parent_page_id:
            # Create a new database
            db_id = notion_control.create_automation_requests_database(parent_page_id)
            if db_id:
                result = {
                    "success": True,
                    "message": "Database created successfully",
                    "database_id": db_id,
                }
            else:
                result = {
                    "success": False,
                    "error": "Failed to create database. Check Notion token and permissions.",
                }

        else:
            # Return current config
            config = notion_control.load_config()
            db_id = config.get("automation_requests_db_id")

            result = {
                "configured": bool(db_id),
                "database_id": db_id,
                "instructions": """
To setup Notion Control Plane:

1. Create database under existing page:
   setup_notion_control_plane(parent_page_id="your-page-id")

2. Or use existing database:
   setup_notion_control_plane(database_id="your-db-id")

The database should have these properties:
- Request (title)
- Type (select): organize, extract, sync, reconcile, custom
- Target (select): tax, media, all, treehouse, yourco, tap, personal
- Status (select): queued, running, done, failed
- Created (date)
- Completed (date)
- Result (rich_text)
- Error (rich_text)
                """.strip()
            }

        # Log operation
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        log_operation("setup_notion_control_plane", params, str(result.get("database_id")), result.get("success", True), duration_ms)

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        error_msg = f"Error setting up control plane: {str(e)}"
        log_operation("setup_notion_control_plane", params, error_msg, False)
        return json.dumps({"error": error_msg})


# =============================================================================
# Daily Briefing
# =============================================================================

@mcp.tool()
def get_daily_briefing(include_financial: bool = True, include_calendar: bool = True) -> str:
    """
    Generate a comprehensive daily briefing.

    Combines information from all ecosystem systems into a single morning summary:
    - Ecosystem status (all 5 systems)
    - Pending documents needing attention
    - Financial summary from Monarch Money (if available)
    - Automation requests pending in Notion
    - Calendar events (if icalBuddy is installed)

    Args:
        include_financial: Include Monarch Money data (default: True)
        include_calendar: Include calendar events (default: True)

    Returns:
        Formatted briefing as markdown text.
    """
    start_time = datetime.now()
    params = {"include_financial": include_financial, "include_calendar": include_calendar}

    try:
        from . import daily_briefing

        briefing = daily_briefing.generate_briefing(
            include_financial=include_financial,
            include_calendar=include_calendar,
        )

        # Return both formatted text and raw data
        result = {
            "formatted": daily_briefing.format_briefing_text(briefing),
            "data": briefing,
        }

        # Log operation
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        log_operation("get_daily_briefing", params, briefing.get("summary", ""), True, duration_ms)

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        error_msg = f"Error generating briefing: {str(e)}"
        log_operation("get_daily_briefing", params, error_msg, False)
        return json.dumps({"error": error_msg})


# =============================================================================
# Monarch → Notion Sync
# =============================================================================

@mcp.tool()
def sync_monarch_to_notion(days: int = 7, dry_run: bool = False) -> str:
    """
    Sync transactions from Monarch Money to Notion.

    Pulls recent transactions and creates entries in the Treehouse Transactions database.
    Automatically skips duplicates based on Monarch transaction ID.

    Args:
        days: Number of days to sync (default: 7)
        dry_run: If True, preview changes without creating Notion pages

    Returns:
        JSON with sync summary: synced count, skipped duplicates, errors
    """
    import asyncio
    start_time = datetime.now()
    params = {"days": days, "dry_run": dry_run}

    try:
        from . import monarch_sync

        # Run the async sync function
        result = asyncio.run(monarch_sync.sync_transactions(
            days=days,
            dry_run=dry_run
        ))

        # Log operation
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        log_operation(
            "sync_monarch_to_notion",
            params,
            result.get("summary", ""),
            result.get("success", False),
            duration_ms
        )

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        error_msg = f"Error syncing Monarch to Notion: {str(e)}"
        log_operation("sync_monarch_to_notion", params, error_msg, False)
        return json.dumps({"error": error_msg, "success": False})


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
