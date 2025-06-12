"""
check-workspace-count.py

This script iterates through all organizations in a Terraform Enterprise (TFE) instance
to check for the existence of workspaces.

Features:
- Queries all or specified organizations to check for workspaces
- Supports two operational modes: count all workspaces or just check if any exist
- Generates CSV report
- Uses multithreading to process multiple organizations concurrently for improved performance
- Reports total script runtime at completion

Usage Flow:
1. Count Mode:
   Run the script with `--mode count` (default) to get exact workspace counts for each organization.

2. Empty-Only Mode:
   Run the script with `--mode empty-only` for faster processing when you only need to identify which organizations
   have no workspaces. Report will be only include TRUE/FALSE.

Usage:
    python check-workspace-count.py --config CONFIG_FILE [--orgs ORGS] [--mode MODE] [--log-level LEVEL] [--max-workers N]
    
Arguments:
    --config: Path to YAML config file (required). Config file can contain organization names under the 'organizations' key.
    --mode: Operation mode.
        - 'count': Get exact workspace counts for all organizations (default).
        - 'empty-only': Only check if organizations have any workspaces.
    --orgs: Path to a file with org names (one per line) or comma-separated list of org names. 
    --log-level: Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    --max-workers: Number of concurrent threads to use for processing organizations (default: 5).

Environment:
    - Requires an admin API token for authentication.
    - Set the token using the TFE_ADMIN_TOKEN environment variable, or you will be prompted securely.

Notes:
    - The script will detect organizations that don't exist and report them as errors
    - Everything will be logged to both the console and 'execution.log'
    - A CSV report will be written to 'workspace_report_%Y%m%d_%H%M%S.csv'
    - Organization selection precedence:
        1. If the optional --orgs flag is provided:
            - If the value is a path to a file, each line in the file is treated as an organization name.
            - Otherwise, the value is parsed as a comma-separated list of organization names (e.g, org1,org2,org3).
        2. If --orgs is not provided, but the config file (provided via --config) contains an 'organizations' key, those organizations are used.
        3. If neither of the above are provided, the script will fetch and process all organizations available in the TFE instance.
"""

import getpass
import requests
import time
import argparse
import yaml
import os
import logging
import concurrent.futures
import csv
import threading
import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

tfe_url = None 

api_prefix = "/api/v2/"
admin_token = ""
headers = {}
report_rows = []
report_lock = threading.Lock()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("execution.log"),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__)
logging.getLogger("urllib3").setLevel(logging.INFO)

# Read in config file
def load_config(config_path="config.yaml"):
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
        validate_config(config)
    return config

# Validate config file. Required keys: tfe_url
def validate_config(config):
    required = ["tfe_url"]
    for key in required:
        if key not in config:
            raise ValueError(f"Missing required config key: {key}")

# Get list of orgs with pagination
def list_orgs():
    orgs = []
    page_number = 1
    page_size = 100 # 100 is the max page size

    while True:
        try:
            url = f"{tfe_url}{api_prefix}organizations?page[number]={page_number}&page[size]={page_size}"
            response = session.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
              
            # Build list of org dicts with id and created-at
            page_orgs = [
                {
                    "id": org["id"],
                    "created_at": org["attributes"].get("created-at")
                }
                for org in data["data"]
            ]

            if not page_orgs:
                break

            orgs.extend(page_orgs)
            logger.info(f"Retrieved {len(page_orgs)} orgs from page {page_number}")

            # Stop if no more pages
            if not data.get("links", {}).get("next"):
                break

            page_number += 1

        except requests.exceptions.RequestException as e:
            logger.error(f"Error listing orgs on page {page_number}: {e}")
            break

    return orgs

def process_org(org, mode, created_at=None):
    """
    Process an organization to check if it has any workspaces.
    
    Args:
        org: The organization name to check
        mode: The operation mode ('count' or 'empty-only')
        
    Returns:
        A tuple of (org_name, workspace_count, has_workspaces)
    """
    logger.info(f"Checking workspaces for org: {org}")
    
    try:
        # For empty-only mode, we just need to check if there's any data
        if mode == "empty-only":
            url = f"{tfe_url}{api_prefix}organizations/{org}/workspaces?page[number]=1&page[size]=1"
        else:
            url = f"{tfe_url}{api_prefix}organizations/{org}/workspaces?page[number]=1&page[size]=20"
            
        response = session.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # Check if there are any workspaces
        if mode == "empty-only":
            # Just check if data array is empty for faster processing
            has_workspaces = len(data.get("data", [])) > 0
            workspace_count = 0 if not has_workspaces else 1  # We don't know exact count in this mode
        else:
            # Get exact workspace count
            workspace_count = data["meta"]["pagination"]["total-count"]
            has_workspaces = workspace_count > 0
        
        # Log the result
        if has_workspaces:
            if mode == "empty-only":
                logger.info(f"Organization {org} has workspaces")
            else:
                logger.info(f"Organization {org} has {workspace_count} workspaces")
        else:
            logger.info(f"Organization {org} has no workspaces")
        
        # Add to report
        with report_lock:
            report_row = {
                "org": org,
                "created_at": created_at,
                "has_workspaces": has_workspaces
            }
            
            if mode == "count":
                report_row["workspace_count"] = workspace_count
            
            report_rows.append(report_row)
            
        return org, workspace_count if mode == "count" else 0, has_workspaces
        
    except requests.exceptions.RequestException as e:
        error_message = str(e)
        
        # Check if this is a 404 error (organization not found)
        if "404" in error_message and "Not Found" in error_message:
            friendly_error = f"Organization does not exist: {error_message}"
        else:
            friendly_error = error_message
            
        logger.error(f"Error checking workspaces for org {org}: {friendly_error}")
        
        with report_lock:
            report_row = {
                "org": org,
                "created_at": created_at,
                "has_workspaces": None,
                "error": friendly_error
            }
            
            if mode == "count":
                report_row["workspace_count"] = -1  # Only add this field in count mode
            
            report_rows.append(report_row)
        return org, -1, None

# Gets metadata for an org when its passed as argument or from config file.
# For now, will grab the created-at date
def fetch_org_metadata(org_id):
    url = f"{tfe_url}{api_prefix}organizations/{org_id}"
    try:
        response = session.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        attributes = data["data"]["attributes"]
        return {
            "id": org_id,
            "created_at": attributes.get("created-at")
        }
    except Exception as e:
        logger.warning(f"Could not fetch metadata for org '{org_id}': {e}")
        return {
            "id": org_id,
            "created_at": None
        }

# Create a requests session with retries (max 6 retries, exponential backoff)
# Covers 429 for rate limits
def get_requests_session_with_retries(retries=6, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504)):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# Create a global session object
session = get_requests_session_with_retries()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect count or empty workspaces from all organizations in TFE.")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument("--orgs", help="Comma-separated list of org names or path to a file with org names (one per line)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set the logging level (default: INFO)")
    parser.add_argument("--max-workers", type=int, default=5, help="Number of concurrent threads (default: 5)")
    parser.add_argument("--mode", choices=["count", "empty-only"], default="count",
                   help="Operation mode: 'count' gets workspace counts for all orgs, 'empty-only' only identifies if an org has any workspaces (default: count)")
    args = parser.parse_args()

    logging.getLogger().setLevel(args.log_level.upper())
    # Load config file
    config = load_config(args.config)
    tfe_url = config["tfe_url"]

    # Set admin token from environment variable or prompt user
    admin_token = os.getenv("TFE_ADMIN_TOKEN") or getpass.getpass("Enter your admin token: ")
    
    start_time = time.time()

    headers = {
        "Authorization": f"Bearer {admin_token}"
    }

    # Determine organizations to process
    organizations = None
    if args.orgs:
        if os.path.isfile(args.orgs):
            with open(args.orgs, "r") as f:
                organizations = [line.strip() for line in f if line.strip()]
        else:
            organizations = [org.strip() for org in args.orgs.split(",") if org.strip()]
        # Fetch metadata for each org
        organizations = [fetch_org_metadata(org) for org in organizations]
    elif "organizations" in config:
        organizations = [fetch_org_metadata(org) for org in config["organizations"]]
    else:
        organizations = list_orgs()

    logger.info(f"Found {len(organizations)} orgs")
    logger.info(f"Orgs: {[org['id'] for org in organizations]}")

    # Process each organization in parallel, max 5 threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = [
            executor.submit(process_org, org["id"], args.mode, org.get("created_at"))
            for org in organizations
        ]
        for i, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            try:
                future.result()
                logger.info(f"[{i}/{len(organizations)}] Finished processing org")
            except Exception as exc:
                logger.error(f"Error processing org: {exc}")

    # Write CSV report
    if report_rows:
        report_filename = f"workspace_report_{datetime.datetime.now():%Y%m%d_%H%M%S}.csv"
        with open(report_filename, "w", newline="") as csvfile:
            # Use different fieldnames based on mode
            if args.mode == "count":
                fieldnames = ["org", "created_at", "workspace_count", "has_workspaces", "error"]
            else:
                fieldnames = ["org", "created_at", "has_workspaces", "error"]
                
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in report_rows:
                writer.writerow(row)
        logger.info(f"CSV report written to {report_filename}")

        # Log summary
        empty_orgs = [row["org"] for row in report_rows if row.get("has_workspaces") is False]
        error_orgs = [row["org"] for row in report_rows if "error" in row]

        # Calculate percentages and totals
        total_orgs = len(organizations)
        empty_count = len(empty_orgs)
        error_count = len(error_orgs)
        active_count = total_orgs - empty_count - error_count
        
        empty_percent = (empty_count / total_orgs) * 100 if total_orgs > 0 else 0
        
        # Print enhanced summary with mode information
        logger.info(f"\n==== WORKSPACE SUMMARY ({args.mode.upper()} MODE) ====")
        logger.info(f"Total organizations processed: {total_orgs}")
        logger.info(f"Organizations with workspaces: {active_count} ({active_count/total_orgs*100:.1f}%)")
        logger.info(f"Organizations with NO workspaces: {empty_count} ({empty_percent:.1f}%)")
        logger.info(f"Organizations with errors: {error_count} ({(error_count/total_orgs*100) if total_orgs > 0 else 0:.1f}%)")


    end_time = time.time()
    runtime_seconds = end_time - start_time
    logger.info(f"\nTotal script runtime: {runtime_seconds:.2f} seconds ({runtime_seconds / 60:.2f} minutes)")
