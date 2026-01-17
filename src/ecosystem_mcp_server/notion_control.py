"""
Notion Control Plane - Automation request handling via Notion.

This module enables automation requests from anywhere (phone, tablet, etc.)
by polling a Notion database for queued requests and executing them.

Database Schema (Automation Requests):
- Name (title): Description of what to do
- Command (rich_text): organize, extract, sync, reconcile, custom
- Arguments (rich_text): Command arguments (e.g., tax, media, all)
- Status (select): queued, running, done, failed
- Created (created_time): Auto timestamp by Notion
- Processed (date): When finished
- Result (rich_text): Summary of what happened (including errors)
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

    # Use older API version that supports databases.query
    return Client(auth=token, notion_version="2022-06-28")


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

    Note: This creates a new database. If you already have a database,
    use --set-db to configure its ID instead.

    Args:
        parent_page_id: The page ID where the database should be created

    Returns:
        Database ID if successful, None otherwise
    """
    client = get_notion_client()
    if not client:
        return None

    try:
        # Create the database with schema matching existing database
        response = client.databases.create(
            parent={"type": "page_id", "page_id": parent_page_id},
            title=[{"type": "text", "text": {"content": "Automation Requests"}}],
            properties={
                "Name": {
                    "title": {}
                },
                "Command": {
                    "rich_text": {}
                },
                "Arguments": {
                    "rich_text": {}
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
                "Processed": {
                    "date": {}
                },
                "Result": {
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
        # Use direct request since library doesn't expose databases.query
        response = client.request(
            path=f"databases/{db_id}/query",
            method="POST",
            body={
                "filter": {
                    "property": "Status",
                    "select": {"equals": STATUS_QUEUED}
                },
                "sorts": [
                    {"property": "Created", "direction": "ascending"}  # created_time type
                ]
            }
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

        # Extract title (Name property)
        title_prop = props.get("Name", {}).get("title", [])
        name = title_prop[0]["text"]["content"] if title_prop else ""

        # Extract rich_text values (Command and Arguments)
        command_prop = props.get("Command", {}).get("rich_text", [])
        command = command_prop[0]["text"]["content"] if command_prop else ""

        args_prop = props.get("Arguments", {}).get("rich_text", [])
        arguments = args_prop[0]["text"]["content"] if args_prop else ""

        # Extract status
        status_prop = props.get("Status", {}).get("select")
        status_val = status_prop["name"] if status_prop else None

        # Extract created_time
        created_val = props.get("Created", {}).get("created_time")

        return {
            "id": page["id"],
            "name": name,
            "command": command.strip().lower(),
            "arguments": arguments.strip(),
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
) -> bool:
    """
    Update the status of a request.

    Args:
        request_id: Notion page ID
        status: New status (running, done, failed)
        result: Result summary or error message

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
            properties["Processed"] = {
                "date": {"start": datetime.now().isoformat()}
            }

        if result:
            properties["Result"] = {
                "rich_text": [{"text": {"content": result[:2000]}}]  # Notion limit
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
        request: Request dict with command, arguments, etc.

    Returns:
        (success, result_message)
    """
    command = request.get("command", "")
    arguments = request.get("arguments", "")
    name = request.get("name", "")

    logger.info(f"Executing: {name} (command={command}, args={arguments})")

    try:
        if command == "organize":
            return execute_organize(arguments)
        elif command == "extract":
            return execute_extract()
        elif command == "sync":
            return execute_sync(arguments)
        elif command == "reconcile":
            return execute_reconcile()
        elif command == "custom" or not command:
            return execute_custom(name, arguments)
        else:
            return False, f"Unknown command: {command}"

    except Exception as e:
        logger.error(f"Request execution failed: {e}")
        return False, f"Error: {e}"


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
        return False, f"Error: {data['error']}"

    remaining = data.get("remaining", {})
    return True, f"Organized files. Remaining: {remaining.get('pdfs', 0)} PDFs, {remaining.get('media', 0)} media"


def execute_extract() -> tuple:
    """Execute tax document extraction."""
    from . import server

    result = server.extract_tax_documents()
    data = json.loads(result)

    if "error" in data:
        return False, f"Error: {data['error']}"

    if data.get("success"):
        processed = data.get("processed", 0)
        needs_review = data.get("needs_review", 0)
        return True, f"Extracted {processed} documents. {needs_review} need review."
    else:
        return False, f"Error: {data.get('error', 'Extraction failed')}"


def execute_sync(target: str) -> tuple:
    """Execute context sync."""
    from . import server

    result = server.sync_notion_context()
    data = json.loads(result)

    if "error" in data:
        return False, f"Error: {data['error']}"

    if data.get("success"):
        return True, f"Context synced. Last sync: {data.get('last_sync', 'unknown')}"
    else:
        return False, f"Error: {data.get('error', 'Sync failed')}"


def execute_reconcile() -> tuple:
    """Execute reconciliation check."""
    from . import server

    result = server.run_reconciliation()
    data = json.loads(result)

    if "error" in data:
        return False, f"Error: {data['error']}"

    issue_count = data.get("issue_count", 0)

    if issue_count == 0:
        return True, "All systems healthy. No issues found."
    else:
        issues = data.get("issues", [])
        return True, f"Found {issue_count} issues: {'; '.join(issues[:3])}"


def execute_custom(request_text: str, arguments: str) -> tuple:
    """
    Execute custom request.

    Supports known custom commands:
    - daily-briefing: Generate and save a daily briefing to Notion

    Unknown commands are logged for manual handling.
    """
    # Check for known custom commands
    command = arguments.lower().strip() if arguments else ""

    if command == "daily-briefing":
        return execute_daily_briefing()

    # Unknown custom requests are logged for manual handling
    return True, f"Custom request logged: '{request_text}' (args: {arguments}). Requires manual handling."


def execute_daily_briefing() -> tuple:
    """Generate and save a daily briefing to Notion."""
    from . import daily_briefing

    result = daily_briefing.save_briefing_to_notion()

    if result.get("success"):
        return True, f"Briefing saved: {result.get('title')}. {result.get('summary')}"
    else:
        return False, f"Failed to save briefing: {result.get('error')}"


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
                    logger.info(f"Processing: {req['name']} (cmd={req['command']}, args={req['arguments']})")

                    # Mark as running
                    update_request_status(req_id, STATUS_RUNNING)

                    # Execute
                    success, result = execute_request(req)

                    # Update status
                    if success:
                        update_request_status(req_id, STATUS_DONE, result=result)
                        logger.info(f"Completed: {result}")
                    else:
                        update_request_status(req_id, STATUS_FAILED, result=result)
                        logger.error(f"Failed: {result}")

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
