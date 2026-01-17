"""
Daily Briefing - Morning summary combining all ecosystem data.

This module generates a comprehensive daily briefing that includes:
- Ecosystem status (all 5 systems)
- Pending documents needing attention
- Financial summary from Monarch Money
- Automation requests pending in Notion
- Calendar/upcoming items (if available)
"""

import json
import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# =============================================================================
# Component: Ecosystem Status
# =============================================================================

def get_ecosystem_status_summary() -> Dict[str, Any]:
    """
    Get summary of all ecosystem systems.

    Returns:
        Dict with status of each system and attention items.
    """
    from . import server

    try:
        status = {
            "downloads_organizer": server.check_downloads_organizer(),
            "tax_organizer": server.check_tax_organizer(),
            "monarch_money": server.check_monarch_money(),
            "context_sync": server.check_context_sync(),
            "notion_rules": server.check_notion_rules(),
        }

        # Count healthy vs needs attention
        healthy = 0
        attention_needed = 0
        attention_items = []

        for name, check in status.items():
            check_status = check.get("status", "unknown")
            if check_status in ["watching", "connected", "synced", "installed", "idle"]:
                healthy += 1
            else:
                attention_needed += 1

            for item in check.get("attention", []):
                attention_items.append(f"{check.get('icon', '•')} {check.get('name', name)}: {item}")

        return {
            "healthy": healthy,
            "attention_needed": attention_needed,
            "attention_items": attention_items,
            "details": status,
        }
    except Exception as e:
        logger.error(f"Failed to get ecosystem status: {e}")
        return {"error": str(e)}


# =============================================================================
# Component: Pending Documents
# =============================================================================

def get_pending_documents() -> Dict[str, Any]:
    """
    Get count of documents pending organization or review.

    Returns:
        Dict with pending PDFs, media files, and documents needing review.
    """
    from . import server

    try:
        home = Path.home()
        downloads = home / "Downloads"

        # Count pending files in Downloads
        pdf_count = server.count_files_in_downloads(["pdf"])
        media_exts = ["jpg", "jpeg", "png", "heic", "mov", "mp4", "mp3", "m4a"]
        media_count = server.count_files_in_downloads(media_exts)

        # Check notion-rules for documents needing review
        needs_review = 0
        notion_rules_repo = server.REPOS.get("notion_rules")
        if notion_rules_repo and notion_rules_repo.exists():
            checkpoint = notion_rules_repo / "tax-years/data/processing_checkpoint.json"
            if checkpoint.exists():
                try:
                    with open(checkpoint) as f:
                        data = json.load(f)
                        needs_review = sum(
                            1 for r in data.get("results", [])
                            if r.get("needs_review", False)
                        )
                except Exception:
                    pass

        return {
            "pending_pdfs": pdf_count,
            "pending_media": media_count,
            "needs_review": needs_review,
            "total_pending": pdf_count + media_count + needs_review,
        }
    except Exception as e:
        logger.error(f"Failed to get pending documents: {e}")
        return {"error": str(e)}


# =============================================================================
# Component: Financial Summary
# =============================================================================

def get_financial_summary() -> Dict[str, Any]:
    """
    Get financial summary from Monarch Money.

    Note: Requires monarch-mcp-server to be authenticated.

    Returns:
        Dict with account balances and recent spending summary.
    """
    try:
        # Import monarch-mcp-server functions
        import sys
        monarch_path = Path.home() / "Documents/monarch-mcp-server/src"
        if str(monarch_path) not in sys.path:
            sys.path.insert(0, str(monarch_path))

        from monarch_mcp_server.server import get_accounts, get_cashflow

        # Get accounts summary
        accounts_json = get_accounts()
        accounts = json.loads(accounts_json) if not accounts_json.startswith("Error") else []

        if isinstance(accounts, list):
            # Calculate totals by account type
            totals = {}
            for account in accounts:
                if account.get("is_active", True):
                    acct_type = account.get("type", "Other")
                    balance = account.get("balance", 0) or 0
                    if acct_type not in totals:
                        totals[acct_type] = 0
                    totals[acct_type] += balance

            # Get recent cashflow
            today = datetime.now()
            start_of_month = today.replace(day=1).strftime("%Y-%m-%d")
            cashflow_json = get_cashflow(start_date=start_of_month)
            cashflow = json.loads(cashflow_json) if not cashflow_json.startswith("Error") else {}

            return {
                "account_count": len([a for a in accounts if a.get("is_active", True)]),
                "totals_by_type": totals,
                "net_worth": sum(totals.values()),
                "mtd_income": cashflow.get("summary", {}).get("sumIncome", 0),
                "mtd_expenses": cashflow.get("summary", {}).get("sumExpense", 0),
                "mtd_savings": cashflow.get("summary", {}).get("savings", 0),
            }
        else:
            return {"error": "Could not parse accounts", "raw": accounts_json[:200]}

    except ImportError as e:
        return {"error": f"Monarch MCP not available: {e}", "hint": "Run login_setup.py"}
    except Exception as e:
        logger.error(f"Failed to get financial summary: {e}")
        return {"error": str(e)}


# =============================================================================
# Component: Automation Requests
# =============================================================================

def get_pending_requests() -> Dict[str, Any]:
    """
    Get pending automation requests from Notion Control Plane.

    Returns:
        Dict with count and list of pending requests.
    """
    try:
        from . import notion_control

        requests = notion_control.get_pending_requests()

        return {
            "pending_count": len(requests),
            "requests": [
                {
                    "name": r.get("name", ""),
                    "command": r.get("command", ""),
                    "arguments": r.get("arguments", ""),
                    "created": r.get("created", ""),
                }
                for r in requests[:5]  # Limit to 5 for briefing
            ],
        }
    except Exception as e:
        logger.error(f"Failed to get pending requests: {e}")
        return {"error": str(e)}


# =============================================================================
# Component: Calendar
# =============================================================================

def get_calendar_events(days: int = 1) -> Dict[str, Any]:
    """
    Get upcoming calendar events using icalBuddy (if available).

    Args:
        days: Number of days to look ahead (default: 1)

    Returns:
        Dict with upcoming events.
    """
    import re

    try:
        # Check if icalBuddy is installed
        result = subprocess.run(
            ["which", "icalBuddy"],
            capture_output=True,
            text=True,
            timeout=5
        )

        if result.returncode != 0:
            return {
                "available": False,
                "hint": "Install icalBuddy for calendar integration: brew install ical-buddy",
            }

        # Get events for today and upcoming days
        # Use eventsToday+N syntax (N=0 means just today)
        result = subprocess.run(
            [
                "icalBuddy",
                "-nc",  # No calendar names
                "-nrd",  # No relative dates
                "-n",  # Include only unfinished events
                f"eventsToday+{days - 1}",
            ],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode == 0:
            events = []
            current_event = None

            # Remove ANSI escape codes from output
            clean_output = re.sub(r'\x1b\[[0-9;]*m', '', result.stdout)

            for line in clean_output.strip().split("\n"):
                if not line.strip():
                    continue

                # Event titles start with bullet
                if line.startswith("•") or line.startswith("*"):
                    if current_event:
                        events.append(current_event)
                    current_event = {"title": line.lstrip("•* ").strip()}
                elif current_event and line.strip():
                    stripped = line.strip()
                    # Check if it looks like a time (contains AM/PM)
                    if "PM" in stripped or "AM" in stripped:
                        current_event["time"] = stripped
                    elif stripped.startswith("attendees:"):
                        pass  # Skip attendees line

            if current_event:
                events.append(current_event)

            return {
                "available": True,
                "event_count": len(events),
                "events": events[:10],  # Limit to 10 events
            }
        else:
            return {
                "available": True,
                "event_count": 0,
                "events": [],
                "note": "No events found",
            }

    except subprocess.TimeoutExpired:
        return {"error": "Calendar query timed out"}
    except FileNotFoundError:
        return {
            "available": False,
            "hint": "Install icalBuddy for calendar integration: brew install ical-buddy",
        }
    except Exception as e:
        logger.error(f"Failed to get calendar events: {e}")
        return {"error": str(e)}


# =============================================================================
# Main Briefing Generator
# =============================================================================

def generate_briefing(include_financial: bool = True, include_calendar: bool = True) -> Dict[str, Any]:
    """
    Generate the complete daily briefing.

    Args:
        include_financial: Include Monarch Money data (requires auth)
        include_calendar: Include calendar events (requires icalBuddy)

    Returns:
        Complete briefing dict with all components.
    """
    briefing = {
        "generated_at": datetime.now().isoformat(),
        "greeting": _get_greeting(),
        "date": datetime.now().strftime("%A, %B %d, %Y"),
    }

    # Always include these
    briefing["ecosystem"] = get_ecosystem_status_summary()
    briefing["documents"] = get_pending_documents()
    briefing["automation"] = get_pending_requests()

    # Optional components
    if include_financial:
        briefing["financial"] = get_financial_summary()

    if include_calendar:
        briefing["calendar"] = get_calendar_events()

    # Generate summary
    briefing["summary"] = _generate_summary(briefing)

    return briefing


def _get_greeting() -> str:
    """Get time-appropriate greeting."""
    hour = datetime.now().hour
    if hour < 12:
        return "Good morning"
    elif hour < 17:
        return "Good afternoon"
    else:
        return "Good evening"


def _generate_summary(briefing: Dict[str, Any]) -> str:
    """Generate a brief text summary of the briefing."""
    parts = []

    # Ecosystem status
    eco = briefing.get("ecosystem", {})
    if eco.get("attention_needed", 0) > 0:
        parts.append(f"{eco['attention_needed']} system(s) need attention")
    else:
        parts.append(f"All {eco.get('healthy', 0)} systems healthy")

    # Pending documents
    docs = briefing.get("documents", {})
    total_pending = docs.get("total_pending", 0)
    if total_pending > 0:
        parts.append(f"{total_pending} document(s) pending")

    # Automation requests
    auto = briefing.get("automation", {})
    pending_requests = auto.get("pending_count", 0)
    if pending_requests > 0:
        parts.append(f"{pending_requests} automation request(s) queued")

    # Calendar
    cal = briefing.get("calendar", {})
    if cal.get("available") and cal.get("event_count", 0) > 0:
        parts.append(f"{cal['event_count']} event(s) today")

    return ". ".join(parts) + "." if parts else "All clear!"


def format_briefing_text(briefing: Dict[str, Any]) -> str:
    """
    Format briefing as readable text for display or notification.

    Args:
        briefing: Briefing dict from generate_briefing()

    Returns:
        Formatted text string.
    """
    lines = []

    # Header
    lines.append(f"# {briefing.get('greeting', 'Hello')}!")
    lines.append(f"**{briefing.get('date', '')}**")
    lines.append("")

    # Summary
    lines.append(f"*{briefing.get('summary', '')}*")
    lines.append("")

    # Ecosystem Status
    lines.append("## Ecosystem Status")
    eco = briefing.get("ecosystem", {})
    if "error" not in eco:
        lines.append(f"- Healthy: {eco.get('healthy', 0)}")
        lines.append(f"- Needs attention: {eco.get('attention_needed', 0)}")
        for item in eco.get("attention_items", [])[:5]:
            lines.append(f"  - {item}")
    else:
        lines.append(f"- Error: {eco.get('error')}")
    lines.append("")

    # Pending Documents
    lines.append("## Pending Documents")
    docs = briefing.get("documents", {})
    if "error" not in docs:
        lines.append(f"- PDFs: {docs.get('pending_pdfs', 0)}")
        lines.append(f"- Media: {docs.get('pending_media', 0)}")
        lines.append(f"- Needs review: {docs.get('needs_review', 0)}")
    else:
        lines.append(f"- Error: {docs.get('error')}")
    lines.append("")

    # Financial Summary (if included)
    if "financial" in briefing:
        lines.append("## Financial Summary")
        fin = briefing["financial"]
        if "error" not in fin:
            lines.append(f"- Net worth: ${fin.get('net_worth', 0):,.2f}")
            lines.append(f"- MTD Income: ${fin.get('mtd_income', 0):,.2f}")
            lines.append(f"- MTD Expenses: ${abs(fin.get('mtd_expenses', 0)):,.2f}")
        else:
            lines.append(f"- {fin.get('error')}")
            if fin.get("hint"):
                lines.append(f"- Hint: {fin.get('hint')}")
        lines.append("")

    # Automation Requests
    lines.append("## Automation Requests")
    auto = briefing.get("automation", {})
    if "error" not in auto:
        pending = auto.get("pending_count", 0)
        if pending > 0:
            lines.append(f"- {pending} request(s) pending:")
            for req in auto.get("requests", [])[:3]:
                lines.append(f"  - {req.get('name', 'Unnamed')}: {req.get('command', '')} {req.get('arguments', '')}")
        else:
            lines.append("- No pending requests")
    else:
        lines.append(f"- Error: {auto.get('error')}")
    lines.append("")

    # Calendar (if included)
    if "calendar" in briefing:
        lines.append("## Today's Events")
        cal = briefing["calendar"]
        if cal.get("available"):
            if cal.get("event_count", 0) > 0:
                for event in cal.get("events", [])[:5]:
                    time_str = event.get("time", "")
                    lines.append(f"- {time_str} {event.get('title', 'Untitled')}")
            else:
                lines.append("- No events scheduled")
        else:
            lines.append(f"- Calendar not available")
            if cal.get("hint"):
                lines.append(f"- {cal.get('hint')}")

    return "\n".join(lines)


# =============================================================================
# CLI Entry Point
# =============================================================================

def main():
    """CLI entry point for generating briefings."""
    import argparse

    parser = argparse.ArgumentParser(description="Generate daily briefing")
    parser.add_argument("--no-financial", action="store_true", help="Skip financial data")
    parser.add_argument("--no-calendar", action="store_true", help="Skip calendar data")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--quiet", "-q", action="store_true", help="Minimal output")

    args = parser.parse_args()

    # Generate briefing
    briefing = generate_briefing(
        include_financial=not args.no_financial,
        include_calendar=not args.no_calendar,
    )

    # Output
    if args.json:
        print(json.dumps(briefing, indent=2, default=str))
    elif args.quiet:
        print(briefing.get("summary", ""))
    else:
        print(format_briefing_text(briefing))


if __name__ == "__main__":
    main()
