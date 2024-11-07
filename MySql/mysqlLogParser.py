import re
import pandas as pd

# Function to normalize queries by removing specific values
def normalize_query(query):
    # Remove specific values (e.g., literals, numbers)
    normalized_query = re.sub(r"(\b\d+\b)|('[^']*')", "?", query)
    # Convert to uppercase for consistency
    normalized_query = normalized_query.upper()
    return normalized_query

# Function to parse the log file and extract the required metrics
def parse_mysql_log(input_file_path, output_file_path):
    # Regular expressions to extract the required fields
    time_pattern = re.compile(r'# Time: (.*)')
    #user_host_pattern = re.compile(r'# User@Host: (.*?) Id:')
    user_host_pattern = re.compile(r'# User@Host: (.*?) thread_id:')
    query_time_pattern = re.compile(r'# Query_time: (.*?) Lock_time:')
    lock_time_pattern = re.compile(r'Lock_time: (.*?) Rows_sent:')
    rows_sent_pattern = re.compile(r'Rows_sent: (.*?) Rows_examined:')
    rows_examined_pattern = re.compile(r'Rows_examined: (.*?)\n')
    query_pattern = re.compile(r'SET timestamp=.*?;\n(.*?);\n# Time:', re.DOTALL)

    # Lists to store the extracted data
    time_list = []
    user_host_list = []
    query_time_list = []
    lock_time_list = []
    rows_sent_list = []
    rows_examined_list = []
    query_list = []
    normalized_query_list = []

    with open(input_file_path, 'r') as log_file:
        log_content = log_file.read()

    # Split log content by "# Time" for individual entries
    log_entries = log_content.split('# Time')[1:]

    for entry in log_entries:
        entry = '# Time' + entry  # Adding back the delimiter for the regex to work
        time_match = time_pattern.search(entry)
        user_host_match = user_host_pattern.search(entry)
        query_time_match = query_time_pattern.search(entry)
        lock_time_match = lock_time_pattern.search(entry)
        rows_sent_match = rows_sent_pattern.search(entry)
        rows_examined_match = rows_examined_pattern.search(entry)
        query_match = query_pattern.search(entry + '# Time:')  # Adding back the delimiter for the regex to work

        if time_match and user_host_match and query_time_match and lock_time_match and rows_sent_match and rows_examined_match and query_match:
            time_list.append(time_match.group(1))
            user_host_list.append(user_host_match.group(1))
            query_time_list.append(float(query_time_match.group(1)) * 1000)  # Convert to ms
            lock_time_list.append(lock_time_match.group(1))
            rows_sent_list.append(rows_sent_match.group(1))
            rows_examined_list.append(rows_examined_match.group(1))
            query = query_match.group(1).strip()
            query_list.append(query)
            normalized_query_list.append(normalize_query(query))

    # Create a DataFrame from the lists
    df = pd.DataFrame({
        'Time': time_list,
        'User@Host': user_host_list,
        'Query_time (ms)': query_time_list,
        'Lock_time': lock_time_list,
        'Rows_sent': rows_sent_list,
        'Rows_examined': rows_examined_list,
        'Query': query_list,
        'Normalized_Query': normalized_query_list
    })

    # Calculate aggregate results
    aggregate_df = df.groupby('Normalized_Query').agg(
        Executions=('Normalized_Query', 'count'),
        Min_Query_time=('Query_time (ms)', 'min'),
        Max_Query_time=('Query_time (ms)', 'max'),
        Avg_Query_time=('Query_time (ms)', 'mean'),
        Sample_Query=('Query', 'first')
    ).reset_index()

    # Save DataFrame to an Excel file with two sheets
    with pd.ExcelWriter(output_file_path) as writer:
        df.to_excel(writer, sheet_name='Detailed Metrics', index=False)
        aggregate_df.to_excel(writer, sheet_name='Aggregate Results', index=False)

# Prompt for the input and output file paths
input_file_path = input('Enter the path to your MySQL log file: ')
output_file_path = input('Enter the path to save the Excel file: ')

# Parse the log file and save the metrics to an Excel file
parse_mysql_log(input_file_path, output_file_path)