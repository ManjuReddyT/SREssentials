import re
import pandas as pd
import streamlit as st
from io import StringIO, BytesIO
import argparse

# Function to normalize queries by removing specific values
def normalize_query(query):
    # Remove specific values (e.g., literals, numbers)
    normalized_query = re.sub(r"(\b\d+\b)|('[^']*')", "?", query)
    # Convert to uppercase for consistency
    normalized_query = normalized_query.upper()
    return normalized_query

# Function to parse the log content and extract the required metrics
def parse_mysql_log_content(log_content_string):
    # Regular expressions to extract the required fields
    time_pattern = re.compile(r'# Time: (.*)')
    user_host_pattern = re.compile(r'# User@Host: (.*?) thread_id:') # Kept original, assuming it's correct for target logs
    query_time_pattern = re.compile(r'# Query_time: (.*?) Lock_time:')
    lock_time_pattern = re.compile(r'Lock_time: (.*?) Rows_sent:')
    rows_sent_pattern = re.compile(r'Rows_sent: (.*?) Rows_examined:')
    rows_examined_pattern = re.compile(r'Rows_examined: (.*?)\n')
    # query_pattern looks for a query that might span multiple lines, starting after SET timestamp=...;
    # and ending before the next # Time:
    # It assumes there's always a SET timestamp before the actual query of interest.
    query_pattern = re.compile(r'SET timestamp=.*?;\n(.*?)(?=\n# Time:|\Z)', re.DOTALL)


    # Lists to store the extracted data
    time_list = []
    user_host_list = []
    query_time_list = []
    lock_time_list = []
    rows_sent_list = []
    rows_examined_list = []
    query_list = []
    normalized_query_list = []
    parse_warnings = []

    # Split log content by "# Time" for individual entries
    # The first element after split will be empty if log starts with # Time, or contain pre-amble.
    # We only care about sections starting with # Time that represent a query block.
    log_entries = log_content_string.split('# Time: ')
    if not log_entries:
        parse_warnings.append("Log content seems empty or not structured as expected (missing '# Time: ' delimiters).")
        return pd.DataFrame(), pd.DataFrame(), parse_warnings

    # The first item in log_entries might be a header or empty if the file starts with # Time.
    # We iterate from the second item, prepending '# Time: ' to reconstruct the entry.
    processed_entries = 0
    for i, entry_content in enumerate(log_entries[1:]): # Skip the part before the first "# Time: "
        full_entry = '# Time: ' + entry_content # Reconstruct the full log entry segment

        time_match = time_pattern.search(full_entry) # Time is already extracted by split, but good for consistency check
        user_host_match = user_host_pattern.search(full_entry)
        query_time_match = query_time_pattern.search(full_entry)
        lock_time_match = lock_time_pattern.search(full_entry)
        rows_sent_match = rows_sent_pattern.search(full_entry)
        rows_examined_match = rows_examined_pattern.search(full_entry)
        
        # For query_match, we need to ensure it operates on a segment that potentially includes a following # Time
        # to correctly delimit the query. We search within full_entry.
        # The regex uses a positive lookahead `(?=\n# Time:|\Z)` to find the end of the query.
        query_match = query_pattern.search(full_entry)

        if time_match and user_host_match and query_time_match and lock_time_match and rows_sent_match and rows_examined_match and query_match:
            time_list.append(time_match.group(1).strip())
            user_host_list.append(user_host_match.group(1).strip())
            try:
                query_time_list.append(float(query_time_match.group(1).strip()) * 1000)  # Convert to ms
            except ValueError:
                parse_warnings.append(f"Could not parse Query_time: '{query_time_match.group(1)}' in entry {i+1}. Skipping field.")
                query_time_list.append(0.0) # Default value
            
            lock_time_list.append(lock_time_match.group(1).strip())
            rows_sent_list.append(rows_sent_match.group(1).strip())
            rows_examined_list.append(rows_examined_match.group(1).strip())
            
            query = query_match.group(1).strip()
            # If the query is empty (e.g. just "COMMIT" or "ROLLBACK" that might not be captured by query_pattern)
            # or if query_pattern fails, we might want to log a warning or skip.
            if not query:
                parse_warnings.append(f"Empty query string found in entry {i+1}. It might be a non-SELECT/INSERT/UPDATE/DELETE statement or a parsing issue.")
                # Decide if to append empty or skip entry
                query_list.append("N/A (Query not captured)")
                normalized_query_list.append("N/A (Query not captured)")
            else:
                query_list.append(query)
                normalized_query_list.append(normalize_query(query))
            processed_entries +=1
        else:
            # This warning helps identify which entries are not fully matching.
            # It could be due to variations in log format or incomplete entries.
            details = f"T:{bool(time_match)}, UH:{bool(user_host_match)}, QT:{bool(query_time_match)}, LT:{bool(lock_time_match)}, RS:{bool(rows_sent_match)}, RE:{bool(rows_examined_match)}, Q:{bool(query_match)}"
            parse_warnings.append(f"Skipped log entry {i+1} due to missing fields. Details: {details}. Content snippet: {full_entry[:200]}...")


    if not processed_entries and not time_list: # check if any data was actually processed
        parse_warnings.append("No valid log entries were parsed. The log might be in an unexpected format or empty.")
        return pd.DataFrame(), pd.DataFrame(), parse_warnings

    # Create a DataFrame from the lists
    df_detailed = pd.DataFrame({
        'Time': time_list,
        'User@Host': user_host_list,
        'Query_time (ms)': query_time_list,
        'Lock_time': lock_time_list,
        'Rows_sent': rows_sent_list,
        'Rows_examined': rows_examined_list,
        'Query': query_list,
        'Normalized_Query': normalized_query_list
    })

    if df_detailed.empty:
        parse_warnings.append("Detailed metrics DataFrame is empty after processing.")
        return df_detailed, pd.DataFrame(), parse_warnings

    # Calculate aggregate results
    try:
        aggregate_df = df_detailed.groupby('Normalized_Query').agg(
            Executions=('Normalized_Query', 'count'),
            Min_Query_time_ms=('Query_time (ms)', 'min'), # Added ms for clarity
            Max_Query_time_ms=('Query_time (ms)', 'max'), # Added ms for clarity
            Avg_Query_time_ms=('Query_time (ms)', 'mean'), # Added ms for clarity
            Sample_Query=('Query', 'first')
        ).reset_index()
        if not aggregate_df.empty:
             aggregate_df['Avg_Query_time_ms'] = aggregate_df['Avg_Query_time_ms'].round(2)
    except Exception as e:
        parse_warnings.append(f"Error during aggregation: {str(e)}")
        aggregate_df = pd.DataFrame() # Return empty if aggregation fails

    return df_detailed, aggregate_df, parse_warnings

# Function to save DataFrames to an Excel file
def save_to_excel(df_detailed, df_aggregated, output_filepath_or_buffer):
    try:
        with pd.ExcelWriter(output_filepath_or_buffer, engine='xlsxwriter') as writer:
            df_detailed.to_excel(writer, sheet_name='Detailed Metrics', index=False)
            df_aggregated.to_excel(writer, sheet_name='Aggregate Results', index=False)
        return True, None
    except Exception as e:
        return False, str(e)

# --- Streamlit App Function ---
def run_streamlit_app():
    st.set_page_config(page_title="MySQL Log Parser", layout="wide")
    st.title("MySQL Log Parser & Analyzer")

    uploaded_file = st.file_uploader(
        "Upload your MySQL log file:",
        type=["log", "txt"] # MySQL logs can be .log or .txt
    )

    if uploaded_file is not None:
        stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
        log_content_string = stringio.read() # Read the whole content as a single string

        df_detailed, df_aggregated, parse_warnings = parse_mysql_log_content(log_content_string)

        if parse_warnings:
            for warning in parse_warnings:
                st.warning(warning)

        if not df_detailed.empty:
            st.subheader("Detailed Metrics")
            st.dataframe(df_detailed)
        else:
            st.info("No detailed metrics were generated. Check warnings above if any.")

        if not df_aggregated.empty:
            st.subheader("Aggregate Results")
            st.dataframe(df_aggregated)
        else:
            st.info("No aggregate results were generated. Check warnings above if any.")

        if not df_detailed.empty or not df_aggregated.empty:
            excel_buffer = BytesIO()
            success, error_msg = save_to_excel(df_detailed, df_aggregated, excel_buffer)
            if success:
                excel_buffer.seek(0)
                st.download_button(
                    label="Download Excel Report",
                    data=excel_buffer,
                    file_name="mysql_log_report.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.error(f"Failed to generate Excel report for download: {error_msg}")
        else:
            st.info("No data available to download.")
            
    else:
        st.info("Please upload a MySQL log file to begin analysis.")

# --- Main Execution Logic ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="MySQL Log Parser & Analyzer.",
        epilog="If no arguments are provided, the script will run in interactive Streamlit mode."
    )
    parser.add_argument(
        "-i", "--input",
        help="Path to the input MySQL log file (e.g., mysql-slow.log)."
    )
    parser.add_argument(
        "-o", "--output",
        help="Path to save the generated Excel report (e.g., mysql_report.xlsx)."
    )

    args = parser.parse_args()

    if args.input and args.output:
        # CLI Mode
        print(f"CLI Mode: Parsing file '{args.input}' and saving report to '{args.output}'...")
        try:
            with open(args.input, 'r', encoding='utf-8') as f:
                log_content_string = f.read()
            
            if not log_content_string.strip():
                print(f"Warning: Input file '{args.input}' is empty or contains only whitespace.")
                # save_to_excel can handle empty dataframes if parse_mysql_log_content returns them
                df_detailed, df_aggregated, parse_warnings = pd.DataFrame(), pd.DataFrame(), ["Input file is empty."]
            else:
                df_detailed, df_aggregated, parse_warnings = parse_mysql_log_content(log_content_string)

            if parse_warnings:
                for warning in parse_warnings:
                    print(f"Parsing Warning: {warning}")
            
            success, error_msg = save_to_excel(df_detailed, df_aggregated, args.output)
            if success:
                print(f"Successfully parsed '{args.input}' and saved Excel report to '{args.output}'")
                if df_detailed.empty and df_aggregated.empty:
                    print("Note: The log file did not contain any parsable query entries matching the defined patterns.")
            else:
                print(f"Error saving Excel file: {error_msg}")

        except FileNotFoundError:
            print(f"Error: Input file '{args.input}' not found.")
        except Exception as e:
            print(f"An unexpected error occurred during CLI processing: {e}")
    elif args.input or args.output:
        # User provided one argument but not the other
        print("Error: Both --input and --output arguments are required for CLI mode.")
        print("To run in interactive Streamlit mode, please provide no arguments.")
        parser.print_help()
    else:
        # Streamlit Mode
        run_streamlit_app()

def main():
    parser = argparse.ArgumentParser(
        description="MySQL Log Parser & Analyzer.",
        epilog="If no arguments are provided, the script will run in interactive Streamlit mode."
    )
    parser.add_argument(
        "-i", "--input",
        help="Path to the input MySQL log file (e.g., mysql-slow.log)."
    )
    parser.add_argument(
        "-o", "--output",
        help="Path to save the generated Excel report (e.g., mysql_report.xlsx)."
    )

    args = parser.parse_args()

    if args.input and args.output:
        # CLI Mode
        print(f"CLI Mode: Parsing file '{args.input}' and saving report to '{args.output}'...")
        try:
            with open(args.input, 'r', encoding='utf-8') as f:
                log_content_string = f.read()
            
            if not log_content_string.strip():
                print(f"Warning: Input file '{args.input}' is empty or contains only whitespace.")
                # save_to_excel can handle empty dataframes if parse_mysql_log_content returns them
                df_detailed, df_aggregated, parse_warnings = pd.DataFrame(), pd.DataFrame(), ["Input file is empty."]
            else:
                df_detailed, df_aggregated, parse_warnings = parse_mysql_log_content(log_content_string)

            if parse_warnings:
                for warning in parse_warnings:
                    print(f"Parsing Warning: {warning}")
            
            success, error_msg = save_to_excel(df_detailed, df_aggregated, args.output)
            if success:
                print(f"Successfully parsed '{args.input}' and saved Excel report to '{args.output}'")
                if df_detailed.empty and df_aggregated.empty:
                    print("Note: The log file did not contain any parsable query entries matching the defined patterns.")
            else:
                print(f"Error saving Excel file: {error_msg}")

        except FileNotFoundError:
            print(f"Error: Input file '{args.input}' not found.")
        except Exception as e:
            print(f"An unexpected error occurred during CLI processing: {e}")
    elif args.input or args.output:
        # User provided one argument but not the other
        print("Error: Both --input and --output arguments are required for CLI mode.")
        print("To run in interactive Streamlit mode, please provide no arguments.")
        parser.print_help()
    else:
        # Streamlit Mode
        run_streamlit_app()

if __name__ == "__main__":
    main()