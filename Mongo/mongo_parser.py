import streamlit as st
import pandas as pd
import json
import os
import re
from collections import defaultdict
from statistics import mean
from io import StringIO, BytesIO
import argparse # Added import

# --- Helper Functions ---
def normalize_query(query):
    normalized_query = re.sub(r'(:\s*["\']?[^,{}\[\]]+["\']?\s*(?=[,}]))', ':<value>', query)
    return normalized_query

# --- Core Parsing Logic ---
def parse_log_lines(lines):
    output_columns = ['Command', 'Collection', 'AppName', 'Duration(ms)', 'KeysExamined', 'DocsExamined', 'numYields',
                    'nreturned', 'Filter', 'Plan', 'timestamp']
    error_columns = ['OriginalLineNumber', 'msg', 'error', 'errmsg', 'totalCount', 'SampleLine'] # Adjusted error_columns

    data = []
    non_slow_query_data = []
    error_summary_map = defaultdict(lambda: {"totalCount": 0, "SampleLine": "", "msg": "", "error": "", "errmsg": "", "lines": []})
    query_stats = defaultdict(lambda: {"count": 0, "durations": [], "sample_query": ""})
    parse_errors = [] # To collect errors for CLI/logging

    for index, line in enumerate(lines):
        try:
            json_payload = json.loads(line)
            if "Slow query" in line: # Heuristic for slow query log lines
                timestamp = json_payload.get('t', {}).get('$date', '')
                attr = json_payload.get('attr', {})
                command_obj = attr.get('command', {}) # Keep command as obj for now
                ns_split = attr.get('ns', '').split('.')
                app_name = ns_split[0] if len(ns_split) > 0 else 'N/A'
                collection = ns_split[1] if len(ns_split) > 1 else 'N/A'
                duration = attr.get('durationMillis', 0)
                keys_examined = attr.get('keysExamined', 0)
                docs_examined = attr.get('docsExamined', 0)
                num_yields = attr.get('numYields', 0)
                nreturned = attr.get('nreturned', 0)

                filter_ = {}
                if 'pipeline' in command_obj: # Handle aggregate queries
                    # Try to extract $match from the first stage of a pipeline
                    pipeline = command_obj.get('pipeline', [])
                    if pipeline and isinstance(pipeline, list) and pipeline[0] and '$match' in pipeline[0]:
                        filter_ = pipeline[0]['$match']
                    else: # Fallback for complex pipelines or other structures
                        filter_ = {'pipeline_info': 'Complex pipeline, see full command'}
                elif 'filter' in command_obj: # Handle find queries
                    filter_ = command_obj.get('filter', {})
                
                plan = attr.get('planSummary', '')

                data.append([
                    json.dumps(command_obj), collection, app_name, duration, keys_examined, docs_examined, num_yields, nreturned,
                    json.dumps(filter_), plan, timestamp
                ])

                # For query stats, normalize the command structure
                normalized_query_key_str = normalize_query(json.dumps(command_obj))
                query_stats[normalized_query_key_str]["count"] += 1
                query_stats[normalized_query_key_str]["durations"].append(duration)
                if not query_stats[normalized_query_key_str]["sample_query"]: # Store first encountered full query as sample
                    query_stats[normalized_query_key_str]["sample_query"] = json.dumps(command_obj)
            
            # Check for errors in any line, not just non-slow query lines
            # This captures errors that might be reported on lines that also get parsed as slow queries (though less common)
            # or on lines that are neither slow queries nor typical app messages.
            if 'msg' in json_payload and json_payload.get('s', '') == 'E' and 'attr' in json_payload and 'error' in json_payload['attr']:
                msg = json_payload.get('msg', 'N/A')
                error_details = json_payload['attr'].get('error', {})
                err_code_name = error_details.get('codeName', 'N/A')
                errmsg_text = error_details.get('errmsg', 'N/A')
                
                error_key = f"{msg}|{err_code_name}|{errmsg_text}" # Create a unique key for error aggregation
                
                error_summary_map[error_key]["totalCount"] += 1
                if not error_summary_map[error_key]["SampleLine"]: # Store first sample line
                     error_summary_map[error_key]["SampleLine"] = line.strip()
                error_summary_map[error_key]["msg"] = msg
                error_summary_map[error_key]["error"] = err_code_name
                error_summary_map[error_key]["errmsg"] = errmsg_text
                error_summary_map[error_key]["lines"].append(index + 1)

            elif "Slow query" not in line : # Store other non-slow, non-error lines
                non_slow_query_data.append(line.strip())

        except json.JSONDecodeError:
            parse_errors.append(f"Line {index + 1}: Invalid JSON. Skipped.")
        except Exception as e:
            parse_errors.append(f"Line {index + 1}: Error parsing line: {e}. Skipped.")

    output_df = pd.DataFrame(data, columns=output_columns)
    non_slow_query_df = pd.DataFrame(non_slow_query_data, columns=['LogLine'])
    
    error_data_for_df = []
    for key, err_info in error_summary_map.items():
        # Use the first line number for 'OriginalLineNumber' for now, or consider how to represent multiple lines
        first_line_num = err_info["lines"][0] if err_info["lines"] else "N/A"
        error_data_for_df.append([
            first_line_num,
            err_info["msg"], 
            err_info["error"], 
            err_info["errmsg"], 
            err_info["totalCount"], 
            err_info["SampleLine"]
        ])
    error_df = pd.DataFrame(error_data_for_df, columns=error_columns)

    query_stats_data = []
    for query, stats in query_stats.items():
        durations = stats["durations"]
        if durations:
            query_stats_data.append({
                "Query Pattern": query, # Renamed for clarity
                "Executions": stats["count"],
                "Min Duration(ms)": min(durations),
                "Max Duration(ms)": max(durations),
                "Avg Duration(ms)": round(mean(durations), 2), # Rounded Average
                "Sample Full Query": stats["sample_query"] # Renamed for clarity
            })
    query_stats_df = pd.DataFrame(query_stats_data)
    
    # Sort by executions and then by average duration
    if not query_stats_df.empty:
        query_stats_df = query_stats_df.sort_values(by=['Executions', 'Avg Duration(ms)'], ascending=[False, False])
    
    return output_df, query_stats_df, non_slow_query_df, error_df, parse_errors

# --- Excel Saving Logic ---
def save_to_excel(output_df, query_stats_df, non_slow_query_df, error_df, output_filepath):
    try:
        with pd.ExcelWriter(output_filepath, engine='xlsxwriter') as writer:
            output_df.to_excel(writer, sheet_name='Detailed Metrics', index=False)
            query_stats_df.to_excel(writer, sheet_name='Query Stats', index=False)
            non_slow_query_df.to_excel(writer, sheet_name='Non-Slow Queries', index=False)
            error_df.to_excel(writer, sheet_name='Error Stats', index=False)
        return True, None # Success, no error message
    except Exception as e:
        return False, str(e) # Failure, error message

# --- Streamlit UI ---
def run_streamlit_app():
    st.set_page_config(page_title="MongoDB Log Parser", layout="wide") 
    st.title("MongoDB Log Parser & Analyzer") 

    # Local file uploader for Streamlit mode
    local_uploaded_file = st.file_uploader(
        "Upload your MongoDB log file (one JSON log per line):",
        type=["log", "txt", "json"]
    )
    
    if local_uploaded_file is not None:
        stringio = StringIO(local_uploaded_file.getvalue().decode("utf-8"))
        lines = stringio.readlines()
        
        output_df, query_stats_df, non_slow_query_df, error_df, parse_errors = parse_log_lines(lines)

        for err in parse_errors: # Display parsing errors in Streamlit UI
            st.warning(err)

        st.subheader("Detailed Metrics")
        st.dataframe(output_df)

        st.subheader("Query Stats")
        st.dataframe(query_stats_df)

        st.subheader("Non-Slow Queries")
        st.dataframe(non_slow_query_df)

        st.subheader("Error Stats")
        st.dataframe(error_df)

        output_excel_bytes = BytesIO()
        success, error_msg = save_to_excel(output_df, query_stats_df, non_slow_query_df, error_df, output_excel_bytes)
        
        if success:
            output_excel_bytes.seek(0)
            st.download_button(
                label="Download Excel report",
                data=output_excel_bytes,
                file_name="mongo_log_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.error(f"Failed to generate Excel file: {error_msg}")
            
    else:
        st.info("Please upload a MongoDB log file to get started.")

# --- Main Execution Logic ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="MongoDB Log Parser & Analyzer. Processes MongoDB log files to extract slow queries, errors, and other statistics.",
        epilog="If no arguments are provided, the script will run in interactive Streamlit mode."
    )
    parser.add_argument(
        "-i", "--input", 
        help="Path to the input MongoDB log file (e.g., mongod.log)."
    )
    parser.add_argument(
        "-o", "--output", 
        help="Path to save the generated Excel report (e.g., report.xlsx)."
    )
    
    args = parser.parse_args()

    if args.input and args.output:
        # CLI Mode
        print(f"CLI Mode: Parsing file '{args.input}' and saving report to '{args.output}'...")
        try:
            with open(args.input, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            if not lines:
                print(f"Warning: Input file '{args.input}' is empty.")
                # Create empty dataframes or handle as appropriate
                output_df, query_stats_df, non_slow_query_df, error_df = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
                parse_errors = ["Input file is empty."]
            else:
                output_df, query_stats_df, non_slow_query_df, error_df, parse_errors = parse_log_lines(lines)
            
            if parse_errors:
                for err in parse_errors:
                    print(f"Parsing Warning: {err}")
            
            success, error_msg = save_to_excel(output_df, query_stats_df, non_slow_query_df, error_df, args.output)
            if success:
                print(f"Successfully parsed '{args.input}' and saved Excel report to '{args.output}'")
                if output_df.empty and query_stats_df.empty and error_df.empty:
                     print("Note: The log file did not contain any parsable slow queries or errors matching the defined patterns.")
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
        # Streamlit mode doesn't need any specific print statements here now,
        # as the run_streamlit_app function handles its own UI.
        run_streamlit_app()

def main():
    parser = argparse.ArgumentParser(
        description="MongoDB Log Parser & Analyzer. Processes MongoDB log files to extract slow queries, errors, and other statistics.",
        epilog="If no arguments are provided, the script will run in interactive Streamlit mode."
    )
    parser.add_argument(
        "-i", "--input", 
        help="Path to the input MongoDB log file (e.g., mongod.log)."
    )
    parser.add_argument(
        "-o", "--output", 
        help="Path to save the generated Excel report (e.g., report.xlsx)."
    )
    
    args = parser.parse_args()

    if args.input and args.output:
        # CLI Mode
        print(f"CLI Mode: Parsing file '{args.input}' and saving report to '{args.output}'...")
        try:
            with open(args.input, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            if not lines:
                print(f"Warning: Input file '{args.input}' is empty.")
                # Create empty dataframes or handle as appropriate
                output_df, query_stats_df, non_slow_query_df, error_df = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
                parse_errors = ["Input file is empty."]
            else:
                output_df, query_stats_df, non_slow_query_df, error_df, parse_errors = parse_log_lines(lines)
            
            if parse_errors:
                for err in parse_errors:
                    print(f"Parsing Warning: {err}")
            
            success, error_msg = save_to_excel(output_df, query_stats_df, non_slow_query_df, error_df, args.output)
            if success:
                print(f"Successfully parsed '{args.input}' and saved Excel report to '{args.output}'")
                if output_df.empty and query_stats_df.empty and error_df.empty:
                     print("Note: The log file did not contain any parsable slow queries or errors matching the defined patterns.")
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
