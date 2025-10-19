#!/usr/bin/env python3
"""
Trigger Lambda function to collect team rankings data for a specific date.

Usage:
    python scripts/trigger_lambda.py 2025-10-07
"""

import sys
import json
import os
from dotenv import load_dotenv
import boto3

load_dotenv()

def trigger_lambda(date_str, function_name=None):
    """Trigger the data collection Lambda for a specific date."""

    # Get function name from environment or use default
    if not function_name:
        # Try to get from environment or search for it
        lambda_client = boto3.client('lambda', region_name='us-east-2')
        response = lambda_client.list_functions()

        # Find function with 'nfl' or 'data' in name
        for func in response['Functions']:
            if 'nfl' in func['FunctionName'].lower() or 'redaptive' in func['FunctionName'].lower():
                function_name = func['FunctionName']
                print(f"Found Lambda function: {function_name}")
                break

    if not function_name:
        print("ERROR: Could not find Lambda function. Please specify function name.")
        return

    # Create event payload
    event = {
        "collectors_to_run": ["team_rankings_data_collector"],
        "date": date_str
    }

    print(f"\nInvoking Lambda: {function_name}")
    print(f"Event: {json.dumps(event, indent=2)}\n")

    # Invoke Lambda asynchronously (don't wait for response)
    lambda_client = boto3.client('lambda', region_name='us-east-2')
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='Event',  # Asynchronous - don't wait
        Payload=json.dumps(event)
    )

    # Parse response
    status_code = response['StatusCode']

    print(f"Status Code: {status_code}")

    if status_code == 202:  # 202 = Accepted (async invocation)
        print(f"\n✓ Successfully triggered collection for {date_str}")
        print(f"  Lambda is running asynchronously (will take ~7-8 minutes)")
        print(f"  Check CloudWatch logs or S3 to verify completion")
    else:
        print(f"\n✗ Error triggering Lambda")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/trigger_lambda.py <date>")
        print("Example: python scripts/trigger_lambda.py 2025-10-07")
        sys.exit(1)

    date = sys.argv[1]
    trigger_lambda(date)
