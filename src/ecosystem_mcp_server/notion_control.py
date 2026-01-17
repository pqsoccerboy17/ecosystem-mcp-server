"""
Notion Control Plane - Automation request handling via Notion.

This module enables automation requests from anywhere (phone, tablet, etc.)
by polling a Notion database for queued requests and executing them.

Database Schema (Automation Requests):
- Request (title): What to do
- Type (select): organize, extract, sync, reconcile, custom
- Target (select): tax, media, all, treehouse, yourco, tap, personal
- Status (select): queued, running, done, failed
- Created (date): Auto timestamp
- Completed (date): When finished
- Result (text): Summary of what happened
- Error (text): If failed, why
"""

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from notion_client import Client
from notion_client.errors import APIResponseError

logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

# Environment variable for Notion token
NOTION_TOKEN_ENV = "NOTION_TOKEN"

# Database ID will be stored after creation
CONFIG_FILE = Path.home() / "Library/Application Support/ecosystem-mcp-server/notion_config.json"

# Request types and their handlers
REQUEST_TYPES = {
    "organize": "Handle file organization requests",
    "extract": "Handle tax document extraction",
    "sync": "Handle context sync requests",
    "reconcile": "Handle reconciliation checks",
    "custom": "Custom/freeform requests",
}

# Targets for requests
REQUEST_TARGETS = {
    "tax": "Tax PDF documents",
    "media": "Media files (photos, videos, audio)",
    "all": "All file types",
    "treehouse": "Treehouse LLC workspace",
    "yourco": "YourCo Consulting workspace",
    "tap": "Tap workspace",
    "personal": "Personal workspace",
}

# Status values
STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_DONE = "done"
STATUS_FAILED = "failed"


# =============================================================================
# Notion Client Setup
# =============================================================================

def get_notion_client() -> Optional[Client]:
    """Get authenticated Notion client."""
    token = os.environ.get(NOTION_TOKEN_ENV)
    if not token:
        # Try loading from ecosystem.env
        env_file = Path.home() / "scripts/ecosystem.env"
        if env_file.exists():
            with open(env_file) as f:
                for line in f:
                    if line.startswith("NOTION_TOKEN="):
                        token = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break

    if not token:
        logger.error(f"Notion token not found. Set {NOTION_TOKEN_ENV} environment variable.")
        return None

    return Client(auth=token)


def load_config() -> Dict[str, Any]:
    """Load saved configuration."""
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            return json.load(f)
    return {}


def save_config(config: Dict[str, Any]):
    """Save configuration."""
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)


# =============================================================================
# Database Creation
# =============================================================================

def create_automation_requests_database(parent_page_id: str) -> Optional[str]:
    """
    Create the Automation Requests database in Notion.

    Args:
        parent_page_id: The page ID where the database should be created

    Returns:
        Database ID if successful, None otherwise
    """
    client = get_notion_client()
    if not client:
        return None

    try:
        # Create the database with required schema
        response = client.databases.create(
            parent={"type": "page_id", "page_id": parent_page_id},
            title=[{"type": "text", "text": {"content": "Automation Requests"}}],
            properties={
                "Request": {
                    "title": {}
                },
                "Type": {
                    "select": {
                        "options": [
                            {"name": "organize", "color": "blue"},
                            {"name": "extract", "color": "green"},
                            {"name": "sync", "color": "purple"},
                            {"name": "reconcile", "color": "orange"},
                            {"name": "custom", "color": "gray"},
                        ]
                    }
                },
                "Target": {
                    "select": {
                        "options": [
                            {"name": "tax", "color": "red"},
                            {"name": "media", "color": "pink"},
                            {"name": "all", "color": "blue"},
                            {"name": "treehouse", "color": "green"},
                            {"name": "yourco", "color": "purple"},
                            {"name": "tap", "color": "orange"},
                            {"name": "personal", "color": "yellow"},
                        ]
                    }
                },
                "Status": {
                    "select": {
                        "options": [
                            {"name": "queued", "color": "yellow"},
                            {"name": "running", "color": "blue"},
                            {"name": "done", "color": "green"},
                            {"name": "failed", "color": "red"},
                        ]
                    }
                },
                "Created": {
                    "date": {}
                },
                "Completed": {
                    "date": {}
                },
                "Result": {
                    "rich_text": {}
                },
                "Error": {
                    "rich_text": {}
                },
            }
        )

        database_id = response["id"]
        logger.info(f"Created Automation Requests database: {database_id}")

        # Save to config
        config = load_config()
        config["automation_requests_db_id"] = database_id
        save_config(config)

        return database_id

    except APIResponseError as e:
        logger.error(f"Failed to create database: {e}")
        return None


# =============================================================================
# Request Handling
# =============================================================================

def get_pending_requests() -> List[Dict[str, Any]]:
    """
    Get all queued automation requests.

    Returns:
        List of pending request objects
    """
    client = get_notion_client()
    if not client:
        return []

    config = load_config()
    db_id = config.get("automation_requests_db_id")
    if not db_id:
        logger.error("Automation Requests database not configured")
        return []

    try:
        response = client.databases.query(
            database_id=db_id,
            filter={
                "property": "Status",
                "select": {"equals": STATUS_QUEUED}
            },
            sorts=[
                {"property": "Created", "direction": "ascending"}
            ]
        )

        requests = []
        for page in response.get("results", []):
            req = parse_request_page(page)
            if req:
                requests.append(req)

        return requests

    except APIResponseError as e:
        logger.error(f"Failed to query requests: {e}")
        return []


def parse_request_page(page: Dict) -> Optional[Dict[str, Any]]:
    """Parse a Notion page into a request dict."""
    try:
        props = page.get("properties", {})

        # Extract title
        title_prop = props.get("Request", {}).get("title", [])
        title = title_prop[0]["text"]["content"] if title_prop else ""

        # Extract select values
        type_prop = props.get("Type", {}).get("select")
        type_val = type_prop["name"] if type_prop else None

        target_prop = props.get("Target", {}).get("select")
        target_val = target_prop["name"] if target_prop else None

        status_prop = props.get("Status", {}).get("select")
        status_val = status_prop["name"] if status_prop else None

        # Extract dates
        created_prop = props.get("Created", {}).get("date")
        created_val = created_prop["start"] if created_prop else None

        return {
            "id": page["id"],
            "request": title,
            "type": type_val,
            "target": target_val,
            "status": status_val,
            "created": created_val,
            "url": page.get("url"),
        }
    except Exception as e:
        logger.error(f"Failed to parse request: {e}")
        return None


def update_request_status(
    request_id: str,
    status: str,
    result: Optional[str] = None,
    error: Optional[str] = None,
) -> bool:
    """
    Update the status of a request.

    Args:
        request_id: Notion page ID
        status: New status (running, done, failed)
        result: Result summary (for done status)
        error: Error message (for failed status)

    Returns:
        True if successful
    """
    client = get_notion_client()
    if not client:
        return False

    try:
        properties = {
            "Status": {"select": {"name": status}}
        }

        if status in [STATUS_DONE, STATUS_FAILED]:
            properties["Completed"] = {
                "date": {"start": datetime.now().isoformat()}
            }

        if result:
            properties["Result"] = {
                "rich_text": [{"text": {"content": result[:2000]}}]  # Notion limit
            }

        if error:
            properties["Error"] = {
                "rich_text": [{"text": {"content": error[:2000]}}]
            }

        client.pages.update(page_id=request_id, properties=properties)
        return True

    except APIResponseError as e:
        logger.error(f"Failed to update request: {e}")
        return False


# =============================================================================
# Request Execution
# =============================================================================

def execute_request(request: Dict[str, Any]) -> tuple:
    """
    Execute an automation request.

    Args:
        request: Request dict with type, target, etc.

    Returns:
        (success, result_message, error_message)
    """
    req_type = request.get("type")
    target = request.get("target")
    req_text = request.get("request", "")

    logger.info(f"Executing request: {req_type} / {target} - {req_text}")

    try:
        if req_type == "organize":
            return execute_organize(target)
        elif req_type == "extract":
            return execute_extract()
        elif req_type == "sync":
            return execute_sync(target)
        elif req_type == "reconcile":
            return execute_reconcile()
        elif req_type == "custom":
            return execute_custom(req_text, target)
        else:
            return False, None, f"Unknown request type: {req_type}"

    except Exception as e:
        logger.error(f"Request execution failed: {e}")
        return False, None, str(e)


def execute_organize(target: str) -> tuple:
    """Execute file organization request."""
    # Import here to avoid circular imports
    from . import server

    if target == "tax":
        result = server.organize_downloads("pdf", dry_run=False)
    elif target == "media":
        result = server.organize_downloads("media", dry_run=False)
    else:
        result = server.organize_downloads("all", dry_run=False)

    data = json.loads(result)
    if "error" in data:
        return False, None, data["error"]

    remaining = data.get("remaining", {})
    return True, f"Organized files. Remaining: {remaining.get('pdfs', 0)} PDFs, {remaining.get('media', 0)} media", None


def execute_extract() -> tuple:
    """Execute tax document extraction."""
    from . import server

    result = server.extract_tax_documents()
    data = json.loads(result)

    if "error" in data:
        return False, None, data["error"]

    if data.get("success"):
        processed = data.get("processed", 0)
        needs_review = data.get("needs_review", 0)
        return True, f"Extracted {processed} documents. {needs_review} need review.", None
    else:
        return False, None, data.get("error", "Extraction failed")


def execute_sync(target: str) -> tuple:
    """Execute context sync."""
    from . import server

    result = server.sync_notion_context()
    data = json.loads(result)

    if "error" in data:
        return False, None, data["error"]

    if data.get("success"):
        return True, f"Context synced. Last sync: {data.get('last_sync', 'unknown')}", None
    else:
        return False, None, data.get("error", "Sync failed")


def execute_reconcile() -> tuple:
    """Execute reconciliation check."""
    from . import server

    result = server.run_reconciliation()
    data = json.loads(result)

    if "error" in data:
        return False, None, data["error"]

    issue_count = data.get("issue_count", 0)
    status = data.get("status", "unknown")

    if issue_count == 0:
        return True, "All systems healthy. No issues found.", None
    else:
        issues = data.get("issues", [])
        return True, f"Found {issue_count} issues: {'; '.join(issues[:3])}", None


def execute_custom(request_text: str, target: str) -> tuple:
    """Execute custom request (logs for manual handling)."""
    # Custom requests are logged but not auto-executed
    # They can be picked up by Claude or handled manually
    return True, f"Custom request logged: '{request_text}' (target: {target}). Requires manual handling.", None


# =============================================================================
# Polling Service
# =============================================================================

def poll_and_process(once: bool = False, interval: int = 60):
    """
    Poll for pending requests and process them.

    Args:
        once: If True, process once and exit
        interval: Seconds between polls (default: 60)
    """
    logger.info(f"Starting Notion Control Plane polling (interval: {interval}s)")

    while True:
        try:
            requests = get_pending_requests()

            if requests:
                logger.info(f"Found {len(requests)} pending request(s)")

                for req in requests:
                    req_id = req["id"]
                    logger.info(f"Processing: {req['request']} ({req['type']}/{req['target']})")

                    # Mark as running
                    update_request_status(req_id, STATUS_RUNNING)

                    # Execute
                    success, result, error = execute_request(req)

                    # Update status
                    if success:
                        update_request_status(req_id, STATUS_DONE, result=result)
                        logger.info(f"Completed: {result}")
                    else:
                        update_request_status(req_id, STATUS_FAILED, error=error)
                        logger.error(f"Failed: {error}")

            if once:
                break

            time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("Polling stopped by user")
            break
        except Exception as e:
            logger.error(f"Polling error: {e}")
            if once:
                break
            time.sleep(interval)


# =============================================================================
# CLI Entry Point
# =============================================================================

def main():
    """CLI entry point for the polling service."""
    import argparse

    parser = argparse.ArgumentParser(description="Notion Control Plane Polling Service")
    parser.add_argument("--once", action="store_true", help="Process once and exit")
    parser.add_argument("--interval", type=int, default=60, help="Poll interval in seconds")
    parser.add_argument("--create-db", metavar="PAGE_ID", help="Create the database under this page")
    parser.add_argument("--set-db", metavar="DB_ID", help="Set the database ID manually")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    if args.create_db:
        db_id = create_automation_requests_database(args.create_db)
        if db_id:
            print(f"Created database: {db_id}")
        else:
            print("Failed to create database")
        return

    if args.set_db:
        config = load_config()
        config["automation_requests_db_id"] = args.set_db
        save_config(config)
        print(f"Database ID saved: {args.set_db}")
        return

    poll_and_process(once=args.once, interval=args.interval)


if __name__ == "__main__":
    main()
