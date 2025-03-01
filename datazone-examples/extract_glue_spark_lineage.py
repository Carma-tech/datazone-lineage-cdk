#!/usr/bin/env python3

import argparse
import json
import sys
from datetime import datetime, timezone
from time import sleep, time

import boto3

GLUE_LOG_GROUP_NAME = "/aws-glue/jobs/error"
CONSOLE_TRANSPORT_TEXT = "INFO [spark-listener-group-shared] transports.ConsoleTransport "
JSON_BEGINS = "{"
JSON_ENDS = "}"
WAIT_TIME_SECONDS = 15

def post_run_event(datazone_client, domain_identifier, run_event, parsed_run_event):
    print("\n  Posting data lineage for:")
    print(f"    Run ID:     {parsed_run_event['run']['runId']}")
    print(f"    Event type: {parsed_run_event['eventType']}")
    print(f"    Event time: {parsed_run_event['eventTime']}")
    print(f"    Job name:   {parsed_run_event['job']['name']}")
    try:
        datazone_client.post_lineage_event(domainIdentifier=domain_identifier, event=run_event)
        print("  Succeeded.")
    except KeyboardInterrupt:
        raise
    except Exception:
        print(f"\n   Error calling PostLineageEvent with data lineage:\n{run_event}\n")
        raise

def process_partial_run_event(logs_client, datazone_client, domain_identifier, log_event, partial_run_event):
    """Process a partial run event by reassembling fragmented JSON from CloudWatch logs."""
    run_event_parts = [partial_run_event]
    paginator = logs_client.get_paginator("filter_log_events")

    # Attempt to locate log lines from the same approximate timestamp,
    # in case CloudWatch truncated the JSON across multiple lines.
    page_iterator = paginator.paginate(
        logGroupName=GLUE_LOG_GROUP_NAME,
        startTime=log_event["timestamp"],
        endTime=log_event["timestamp"] + 100,
    )

    after_first_part_index = None
    for page in page_iterator:
        events = page["events"]
        events_count = len(events)
        
        # Find the starting event to skip it
        if after_first_part_index is None:
            for i in range(events_count):
                if log_event["eventId"] == events[i]["eventId"]:
                    after_first_part_index = i + 1
                    break
        else:
            after_first_part_index = 0
        
        if after_first_part_index is not None:
            for i in range(after_first_part_index, events_count):
                console_msgs = events[i]["message"].split("\n")
                for console_msg in console_msgs:
                    run_event_text_pos = console_msg.find(CONSOLE_TRANSPORT_TEXT)
                    if run_event_text_pos == -1:
                        continue
                    run_event_pos = console_msg.find(JSON_BEGINS, run_event_text_pos + len(CONSOLE_TRANSPORT_TEXT))
                    if run_event_pos == -1:
                        continue
                    run_event_part = console_msg[run_event_pos:]
                    run_event_parts.append(run_event_part)

    # Attempt to reassemble the full JSON
    full_run_event = "".join(run_event_parts)
    print("Reassembled partial run event: ", full_run_event)

    try:
        parsed_run_event = json.loads(full_run_event)
        post_run_event(
            datazone_client=datazone_client,
            domain_identifier=domain_identifier,
            run_event=full_run_event,
            parsed_run_event=parsed_run_event,
        )
    except json.JSONDecodeError as e:
        print(f"Failed to parse reassembled JSON: {e}")
        print("Problematic JSON: ", full_run_event)
        # Skip this event instead of raising an error
        return

def process_log_event(logs_client, datazone_client, domain_identifier, log_event):
    """Process a log event and extract OpenLineage data."""
    console_msgs = log_event["message"].split("\n")
    print("log console msgs: ", console_msgs)

    for console_msg in console_msgs:
        run_event_text_pos = console_msg.find(CONSOLE_TRANSPORT_TEXT)
        if run_event_text_pos == -1:
            continue
        run_event_pos = console_msg.find(JSON_BEGINS, run_event_text_pos + len(CONSOLE_TRANSPORT_TEXT))
        if run_event_pos == -1:
            continue
        run_event = console_msg[run_event_pos:]

        try:
            print("log run event: ", run_event)
            parsed_run_event = json.loads(run_event)
            post_run_event(
                datazone_client=datazone_client,
                domain_identifier=domain_identifier,
                run_event=run_event,
                parsed_run_event=parsed_run_event,
            )
        except json.JSONDecodeError:
            # If JSON parsing fails, assume it's a partial event and attempt reassembly
            process_partial_run_event(
                logs_client=logs_client,
                datazone_client=datazone_client,
                domain_identifier=domain_identifier,
                log_event=log_event,
                partial_run_event=run_event,
            )

def start_time_to_iso_format(timestamp):
    # Returns the ISO format with milliseconds (3 digits)
    # (e.g. "2025-02-26T00:12:40.123+00:00")
    return datetime.fromtimestamp(int(timestamp * 1_000) / 1_000, timezone.utc).isoformat(timespec="milliseconds")

def extract_and_post_lineage(session, datazone_endpoint_url, domain_identifier, start_time, max_seconds=180):
    """
    Poll CloudWatch logs for lineage events up to max_seconds, then exit.
    """
    logs_client = session.client(service_name="logs")
    datazone_client = session.client(service_name="datazone", endpoint_url=datazone_endpoint_url)

    start_time_seconds = datetime.fromisoformat(start_time).timestamp()
    start_run_time = time()  # NEW: Track when we started polling

    try:
        while True:
            now = time()
            if now - start_run_time > max_seconds:  # NEW
                print(f"\nReached maximum wait time of {max_seconds} seconds. Exiting.")
                break

            print("\nSearching for data lineage...")
            polling_time_seconds = time()

            paginator = logs_client.get_paginator("filter_log_events")
            page_iterator = paginator.paginate(
                logGroupName=GLUE_LOG_GROUP_NAME,
                filterPattern=f'"{CONSOLE_TRANSPORT_TEXT}"',
                startTime=int(start_time_seconds * 1_000),
            )

            events_found = False
            for page in page_iterator:
                log_events = page["events"]
                if log_events:
                    print(f"\nFound {len(log_events)} log events that contain data lineage.")
                    events_found = True
                    # Start the next search after the last log event.
                    start_time_seconds = (log_events[-1]["timestamp"] + 1) / 1_000
                    for log_event in log_events:
                        process_log_event(
                            logs_client=logs_client,
                            datazone_client=datazone_client,
                            domain_identifier=domain_identifier,
                            log_event=log_event,
                        )
            if not events_found:
                # No events found - start the next search from the last polling time.
                print("No data lineage found.")
                start_time_seconds = polling_time_seconds
                print(f"\nPausing for {WAIT_TIME_SECONDS} seconds (CTRL-C to quit)...")
                sleep(WAIT_TIME_SECONDS)
    except KeyboardInterrupt:
        print(f"\nExiting {sys.argv[0]}")
        sys.exit(1)

def print_identity(session):
    # Print info about the caller's identity (if permissions allow).
    try:
        iam_client = session.client(service_name="iam")
        account_alias = iam_client.list_account_aliases()["AccountAliases"][0]
    except Exception:
        account_alias = "-"
    try:
        sts_client = session.client(service_name="sts")
        caller_identity = sts_client.get_caller_identity()
        account_id = caller_identity["Account"]
        user_id = caller_identity["UserId"]
        user_arn = caller_identity["Arn"]
    except Exception:
        account_id = user_id = user_arn = "-"
    print("  IAM identity:\n")
    print(f"    Account alias: {account_alias}")
    print(f"    Account Id:    {account_id}")
    print(f"    User Id:       {user_id}")
    print(f"    ARN:           {user_arn}")

def verify_identity_and_settings(session, datazone_endpoint_url, domain_identifier, start_time, max_seconds):
    if session.region_name is None:
        print(f"\n{sys.argv[0]}: error: the following arguments are required: -r/--region")
        exit(1)

    print("\nPlease review the settings for this session.\n")
    print(f"  Profile: {session.profile_name}")
    print(f"  Region:  {session.region_name}\n")

    print_identity(session)

    print("\n  Extracting AWS Glue Spark data lineage from:\n")
    print(f"    Log group:  {GLUE_LOG_GROUP_NAME}")
    start_time_iso = start_time_to_iso_format(datetime.fromisoformat(start_time).timestamp())
    print(f"    Start time: {start_time_iso}")

    print("\n  Posting data lineage to Amazon DataZone:\n")
    print(
        f"    Endpoint:  {session.client(service_name='datazone', endpoint_url=datazone_endpoint_url).meta.endpoint_url}"
    )
    print(f"    Domain Id: {domain_identifier}")
    print(f"    Max seconds to poll: {max_seconds} seconds")

    user_input = input("\nAre the settings above correct? (yes/no): ")
    if not user_input.lower() == "yes":
        print(f'Exiting. You entered "{user_input}", enter "yes" to continue.')
        exit(0)

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Extract data lineage from AWS Glue Spark job runs and post it to Amazon DataZone."
    )
    parser.add_argument(
        "-p",
        "--profile",
        help="Use a specific profile from your credential file.",
    )
    parser.add_argument(
        "-r",
        "--region",
        help="The region to use. Overrides config/env settings.",
    )
    parser.add_argument(
        "-e",
        "--datazone-endpoint-url",
        help="The Amazon DataZone endpoint URL to use. Overrides the default endpoint URL for the region.",
    )
    parser.add_argument(
        "-i",
        "--domain-identifier",
        help="The identifier for the Amazon DataZone domain where data lineage is stored.",
        required=True,
    )
    default_start_time = start_time_to_iso_format(time())
    parser.add_argument(
        "-s",
        "--start-time",
        help="The start time for searching the logs in ISO 8601 format. "
             f"The default start time is 'now': {default_start_time}.",
        default=default_start_time,
    )
    # NEW: Add a max-seconds argument to limit how long we poll
    parser.add_argument(
        "--max-seconds",
        type=int,
        default=180,
        help="Maximum total time in seconds to search for lineage logs before exiting. Default is 180."
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    session = boto3.Session(profile_name=args.profile, region_name=args.region)

    verify_identity_and_settings(
        session=session,
        datazone_endpoint_url=args.datazone_endpoint_url,
        domain_identifier=args.domain_identifier,
        start_time=args.start_time,
        max_seconds=args.max_seconds
    )

    extract_and_post_lineage(
        session=session,
        datazone_endpoint_url=args.datazone_endpoint_url,
        domain_identifier=args.domain_identifier,
        start_time=args.start_time,
        max_seconds=args.max_seconds  # pass new param
    )
