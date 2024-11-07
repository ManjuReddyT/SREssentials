import pandas as pd
import json
import os
import re
from collections import defaultdict
from statistics import mean


# Function to prompt for file paths
def get_file_path(prompt):
    return input(prompt)


# Function to normalize queries
def normalize_query(query):
    # Replace values in the query filter section with a placeholder
    # Adjust the regex to handle numbers, strings, and dates
    normalized_query = re.sub(r'(:\s*["\']?[^,{}\[\]]+["\']?\s*(?=[,}]))', ':<value>', query)
    return normalized_query


# Prompt for input and output file paths
input_file_path = get_file_path('Enter the input log file path: ')
output_file_path = get_file_path('Enter the output Excel file path: ')

# Check if input file exists
if not os.path.isfile(input_file_path):
    print(f"Error: The file '{input_file_path}' does not exist.")
    exit(1)

try:
    # Read the log file
    with open(input_file_path, 'r') as file:
        lines = file.readlines()
except Exception as e:
    print(f"Error reading the input file: {e}")
    exit(1)

# Define the columns for the output Excel file
output_columns = ['Command', 'Collection', 'AppName', 'Duration(ms)', 'KeysExamined', 'DocsExamined', 'numYields',
                  'nreturned', 'Filter', 'Plan', 'timestamp']
error_columns = ['Line', 'msg', 'error', 'errmsg', 'totalCount', 'SampleLine']

# Initialize an empty list to store the extracted data
data = []
non_slow_query_data = []
error_data = []

# Initialize dictionaries to store query and error statistics
query_stats = defaultdict(lambda: {"count": 0, "durations": [], "sample_query": ""})
error_stats = defaultdict(lambda: {"totalCount": 0, "SampleLine": ""})

# Iterate over each line in the log file
for index, line in enumerate(lines):
    try:
        # Parse the JSON payload
        json_payload = json.loads(line)

        if "Slow query" in line:
            # Extract the timestamp from the nested structure
            timestamp = json_payload.get('t', {}).get('$date', '')

            # Extract the required fields from the payload
            attr = json_payload.get('attr', {})
            command = attr.get('command', {})
            app_name = attr.get('ns', '').split('.')[0]
            collection = attr.get('ns', '').split('.')[1]
            duration = attr.get('durationMillis', 0)
            keys_examined = attr.get('keysExamined', 0)
            docs_examined = attr.get('docsExamined', 0)
            num_yields = attr.get('numYields', 0)
            nreturned = attr.get('nreturned', 0)

            # Handle different scenarios for the filter_
            if 'pipeline' in command:  # For aggregate commands
                filter_ = command.get('pipeline', [])[0].get('$match', {})
            elif 'filter' in command:  # For find commands or other queries
                filter_ = command.get('filter', {})
            else:
                filter_ = {}  # Default to empty dictionary

            plan = attr.get('planSummary', '')

            # Append the extracted data to the list
            data.append([command, collection, app_name, duration, keys_examined, docs_examined, num_yields, nreturned,
                         json.dumps(filter_), plan, timestamp])

            # Normalize the query and update query stats
            normalized_query = normalize_query(json.dumps(command))
            query_stats[normalized_query]["count"] += 1  # type: ignore
            query_stats[normalized_query]["durations"].append(duration)  # type: ignore
            query_stats[normalized_query]["sample_query"] = json.dumps(command)  # Store a sample query
        else:
            # Append non-slow query data
            non_slow_query_data.append(line)

            # Check for error messages
            if 'msg' in json_payload and 'error' in json_payload.get('attr', {}):
                msg = json_payload.get('msg', '')
                error = json_payload['attr'].get('error', {}).get('codeName', '')
                errmsg = json_payload['attr'].get('error', {}).get('errmsg', '')

                error_stats[msg]["totalCount"] += 1  # type: ignore
                error_stats[msg]["SampleLine"] = line

                # Append error data to the list
                error_data.append(
                    [index, msg, error, errmsg, error_stats[msg]["totalCount"], error_stats[msg]["SampleLine"]])

    except json.JSONDecodeError:
        print(f"Error: Invalid JSON payload in line {index}. Skipping line.")
    except Exception as e:
        print(f"Error processing line {index}: {e}. Skipping line.")

# Create DataFrames for the output data
output_df = pd.DataFrame(data, columns=output_columns)
non_slow_query_df = pd.DataFrame(non_slow_query_data, columns=['Line'])
error_df = pd.DataFrame(error_data, columns=error_columns)

# Create a DataFrame for the query statistics
query_stats_data = []
for query, stats in query_stats.items():
    durations = stats["durations"]
    query_stats_data.append({
        "Query": query,
        "Executions": stats["count"],
        "Min Duration(ms)": min(durations),  # type: ignore
        "Max Duration(ms)": max(durations),  # type: ignore
        "Avg Duration(ms)": mean(durations),  # type: ignore
        "Sample Query": stats["sample_query"]
    })

query_stats_df = pd.DataFrame(query_stats_data)

try:
    # Write the output DataFrames to an Excel file
    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        output_df.to_excel(writer, sheet_name='Detailed Metrics', index=False)
        query_stats_df.to_excel(writer, sheet_name='Query Stats', index=False)
        non_slow_query_df.to_excel(writer, sheet_name='Non-Slow Queries', index=False)
        error_df.to_excel(writer, sheet_name='Error Stats', index=False)
    print(f"Output successfully written to '{output_file_path}'")
except Exception as e:
    print(f"Error writing the output file: {e}")
    exit(1)
