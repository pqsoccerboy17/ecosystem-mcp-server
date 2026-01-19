"""
Monarch → Notion Transaction Sync Module

Syncs transactions from Monarch Money to Notion's Treehouse Transactions database.
Designed to be called from n8n, Claude, or as a standalone script.

Features:
- Pulls transactions for a date range
- Maps to Notion database schema
- Deduplicates based on Monarch transaction ID
- Supports dry-run mode for testing
- AI-powered categorization (optional, requires Gemini API key)

Usage:
    # From command line
    python -m ecosystem_mcp_server.monarch_sync --days 7

    # From n8n (HTTP Request node)
    POST http://localhost:5678/webhook/monarch-sync
    {"days": 7, "dry_run": false}
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add parent paths for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "monarch-mcp-server/src"))

logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

HOME = Path.home()

# Notion configuration
NOTION_TOKEN_ENV = "NOTION_TOKEN"
TREEHOUSE_TRANSACTIONS_DB = "7a8ec1ed-6ea0-4b5b-882d-f65320e8a745"

# Entity mapping (Monarch account → Business entity)
# You can customize this based on your account names
ENTITY_MAPPING = {
    # Add your account mappings here, e.g.:
    # "Chase Checking ...1234": "Treehouse LLC",
    # "Personal Savings": "Personal",
    "default": "Treehouse LLC"  # Default entity if no match
}

# Category mapping (Monarch category → Notion category)
# Maps common Monarch categories to your Notion schema
CATEGORY_MAPPING = {
    "Utilities": "Utilities",
    "Insurance": "Insurance",
    "Maintenance & Repairs": "Repairs & Maintenance",
    "Property Tax": "Taxes",
    "Mortgage": "Mortgage",
    "HOA Fees": "HOA",
    "Rental Income": "Rental Income",
    "default": "Other"
}


# =============================================================================
# Notion Client
# =============================================================================

def get_notion_token() -> Optional[str]:
    """Get Notion API token from environment or config file."""
    token = os.environ.get(NOTION_TOKEN_ENV)
    if token:
        return token

    # Try loading from ecosystem.env
    env_file = HOME / "scripts/ecosystem.env"
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                if line.startswith("NOTION_TOKEN="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")

    return None


async def create_notion_page(token: str, database_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
    """Create a page in a Notion database."""
    import aiohttp

    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    body = {
        "parent": {"database_id": database_id},
        "properties": properties
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=body) as resp:
            if resp.status != 200:
                error_text = await resp.text()
                raise Exception(f"Notion API error {resp.status}: {error_text}")
            return await resp.json()


async def query_notion_database(
    token: str,
    database_id: str,
    filter_dict: Optional[Dict] = None
) -> List[Dict]:
    """Query a Notion database with optional filter."""
    import aiohttp

    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    body = {}
    if filter_dict:
        body["filter"] = filter_dict

    results = []
    has_more = True
    start_cursor = None

    async with aiohttp.ClientSession() as session:
        while has_more:
            if start_cursor:
                body["start_cursor"] = start_cursor

            async with session.post(url, headers=headers, json=body) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    raise Exception(f"Notion API error {resp.status}: {error_text}")

                data = await resp.json()
                results.extend(data.get("results", []))
                has_more = data.get("has_more", False)
                start_cursor = data.get("next_cursor")

    return results


# =============================================================================
# Monarch Client
# =============================================================================

async def get_monarch_transactions(
    start_date: str,
    end_date: str,
    limit: int = 500
) -> List[Dict]:
    """
    Get transactions from Monarch Money.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        limit: Maximum number of transactions to retrieve

    Returns:
        List of transaction dictionaries
    """
    try:
        # Try to import from monarch-mcp-server
        from monarchmoney import MonarchMoney

        # Get session file path
        session_file = HOME / "Library/Application Support/monarch-mcp-server/mm_session.pickle"

        if not session_file.exists():
            # Try alternate location
            session_file = HOME / ".monarch-mcp/session.json"

        client = MonarchMoney(session_file=str(session_file), timeout=30)

        # Login using saved session
        await client.login(use_saved_session=True)

        # Get transactions
        response = await client.get_transactions(
            limit=limit,
            start_date=start_date,
            end_date=end_date
        )

        # Extract transactions from response
        transactions = response.get("allTransactions", {}).get("results", [])

        return transactions

    except ImportError:
        logger.error("monarchmoney library not installed. Run: pip install monarchmoney")
        return []
    except Exception as e:
        logger.error(f"Failed to get Monarch transactions: {e}")
        return []


# =============================================================================
# Transaction Mapping
# =============================================================================

def map_transaction_to_notion(tx: Dict) -> Dict[str, Any]:
    """
    Map a Monarch transaction to Notion page properties.

    Args:
        tx: Monarch transaction dictionary

    Returns:
        Notion properties dictionary
    """
    # Extract fields from Monarch transaction
    tx_id = tx.get("id", "")
    date = tx.get("date", "")
    amount = float(tx.get("amount", 0))
    # plaidName contains the original transaction description from the bank
    description = tx.get("plaidName", "") or tx.get("description", "") or tx.get("originalDescription", "")
    merchant = tx.get("merchant", {}).get("name", "") if tx.get("merchant") else ""
    category = tx.get("category", {}).get("name", "") if tx.get("category") else ""
    account_name = tx.get("account", {}).get("displayName", "") or tx.get("account", {}).get("name", "")
    is_pending = tx.get("pending", False)

    # Determine entity based on account
    entity = ENTITY_MAPPING.get(account_name, ENTITY_MAPPING.get("default", ""))

    # Map category
    notion_category = CATEGORY_MAPPING.get(category, CATEGORY_MAPPING.get("default", "Other"))

    # Build Notion properties matching Treehouse Transactions database schema
    # Title field is "Description" (the transaction description)
    # Use merchant name or plaidName for the title
    title_text = merchant if merchant else description

    properties = {
        "Description": {
            "title": [{"text": {"content": title_text[:100]}}]
        },
        "Date": {
            "date": {"start": date}
        },
        "Amount": {
            "number": amount
        },
        "Monarch ID": {
            "rich_text": [{"text": {"content": tx_id}}]
        },
    }

    # Add notes with the raw plaidName if different from title
    if description and description != title_text:
        properties["Notes"] = {
            "rich_text": [{"text": {"content": description[:2000]}}]
        }

    # Add category if available
    if category:
        properties["Category"] = {"select": {"name": notion_category}}

    # Add entity based on account tags (TH = Treehouse, PERS = Personal)
    tags = tx.get("tags", [])
    tag_names = [t.get("name", "") for t in tags]
    if "TH" in tag_names:
        properties["Entity"] = {"select": {"name": "Treehouse LLC"}}
    elif "PERS" in tag_names:
        properties["Entity"] = {"select": {"name": "Personal"}}

    return properties


# =============================================================================
# Sync Logic
# =============================================================================

async def get_existing_monarch_ids(token: str, database_id: str) -> set:
    """Get set of Monarch IDs already in Notion to prevent duplicates."""
    existing_ids = set()

    try:
        # Query for pages with Monarch ID property
        pages = await query_notion_database(token, database_id)

        for page in pages:
            props = page.get("properties", {})
            monarch_id_prop = props.get("Monarch ID", {})

            # Handle rich_text property type
            if monarch_id_prop.get("type") == "rich_text":
                texts = monarch_id_prop.get("rich_text", [])
                if texts:
                    existing_ids.add(texts[0].get("text", {}).get("content", ""))

    except Exception as e:
        logger.warning(f"Could not fetch existing Monarch IDs: {e}")

    return existing_ids


async def sync_transactions(
    days: int = 7,
    dry_run: bool = False,
    database_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Sync transactions from Monarch to Notion.

    Args:
        days: Number of days to sync (default: 7)
        dry_run: If True, don't actually create pages in Notion
        database_id: Override default database ID

    Returns:
        Summary of sync operation
    """
    result = {
        "success": False,
        "synced": 0,
        "skipped": 0,
        "errors": 0,
        "transactions": [],
        "error_details": []
    }

    # Get Notion token
    token = get_notion_token()
    if not token:
        result["error"] = "Notion token not found. Set NOTION_TOKEN environment variable."
        return result

    db_id = database_id or TREEHOUSE_TRANSACTIONS_DB

    # Calculate date range
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    logger.info(f"Syncing transactions from {start_date} to {end_date}")

    # Get transactions from Monarch
    transactions = await get_monarch_transactions(start_date, end_date)

    if not transactions:
        result["error"] = "No transactions retrieved from Monarch"
        return result

    logger.info(f"Retrieved {len(transactions)} transactions from Monarch")

    # Get existing Monarch IDs to prevent duplicates
    existing_ids = await get_existing_monarch_ids(token, db_id)
    logger.info(f"Found {len(existing_ids)} existing transactions in Notion")

    # Process each transaction
    for tx in transactions:
        tx_id = tx.get("id", "")

        # Skip if already exists
        if tx_id in existing_ids:
            result["skipped"] += 1
            continue

        # Map to Notion properties
        properties = map_transaction_to_notion(tx)

        if dry_run:
            result["transactions"].append({
                "id": tx_id,
                "description": tx.get("description", "")[:50],
                "amount": tx.get("amount"),
                "date": tx.get("date"),
                "action": "would_create"
            })
            result["synced"] += 1
        else:
            try:
                await create_notion_page(token, db_id, properties)
                result["synced"] += 1
                result["transactions"].append({
                    "id": tx_id,
                    "description": tx.get("description", "")[:50],
                    "amount": tx.get("amount"),
                    "action": "created"
                })
            except Exception as e:
                result["errors"] += 1
                result["error_details"].append({
                    "id": tx_id,
                    "error": str(e)
                })
                logger.error(f"Failed to create page for {tx_id}: {e}")

    result["success"] = result["errors"] == 0
    result["summary"] = f"Synced {result['synced']}, skipped {result['skipped']} duplicates, {result['errors']} errors"

    return result


# =============================================================================
# CLI Entry Point
# =============================================================================

def main():
    """Command-line entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Sync Monarch transactions to Notion")
    parser.add_argument("--days", type=int, default=7, help="Number of days to sync")
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating pages")
    parser.add_argument("--database-id", type=str, help="Override Notion database ID")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    result = asyncio.run(sync_transactions(
        days=args.days,
        dry_run=args.dry_run,
        database_id=args.database_id
    ))

    print(json.dumps(result, indent=2, default=str))

    return 0 if result.get("success") else 1


if __name__ == "__main__":
    sys.exit(main())
