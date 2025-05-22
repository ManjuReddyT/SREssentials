import streamlit as st
import pandas as pd
import json
import os
import re
from collections import defaultdict
from statistics import mean
from io import StringIO, BytesIO

st.set_page_config(page_title="MongoDB Log Parser", layout="wide")
st.title("MongoDB Log Parser & Analyzer")

def normalize_query(query):
    normalized_query = re.sub(r'(:\s*["\']?[^,{}\[\]]+["\']?\s*(?=[,}]))', ':<value>', query)
    return normalized_query

# File uploader
uploaded_file = st.file_uploader(
    "Upload your MongoDB log file (one JSON log per line):",
    type=["log", "txt", "json"]
)

if uploaded_file is not None:
    # Read uploaded file
    stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
    lines = stringio.readlines()

    output_columns = ['Command', 'Collection', 'AppName', 'Duration(ms)', 'KeysExamined', 'DocsExamined', 'numYields',
                    'nreturned', 'Filter', 'Plan', 'timestamp']
    error_columns = ['Line', 'msg', 'error', 'errmsg', 'totalCount', 'SampleLine']

    data = []
    non_slow_query_data = []
    error_data = []
    query_stats = defaultdict(lambda: {"count": 0, "durations": [], "sample_query": ""})
    error_stats = defaultdict(lambda: {"totalCount": 0, "SampleLine": ""})

    for index, line in enumerate(lines):
        try:
            json_payload = json.loads(line)
            if "Slow query" in line:
                timestamp = json_payload.get('t', {}).get('$date', '')
                attr = json_payload.get('attr', {})
                command = attr.get('command', {})
                ns_split = attr.get('ns', '').split('.')
                app_name = ns_split[0] if len(ns_split) > 0 else ''
                collection = ns_split[1] if len(ns_split) > 1 else ''
                duration = attr.get('durationMillis', 0)
                keys_examined = attr.get('keysExamined', 0)
                docs_examined = attr.get('docsExamined', 0)
                num_yields = attr.get('numYields', 0)
                nreturned = attr.get('nreturned', 0)

                if 'pipeline' in command:
                    filter_ = command.get('pipeline', [{}])[0].get('$match', {})
                elif 'filter' in command:
                    filter_ = command.get('filter', {})
                else:
                    filter_ = {}

                plan = attr.get('planSummary', '')

                data.append([
                    command, collection, app_name, duration, keys_examined, docs_examined, num_yields, nreturned,
                    json.dumps(filter_), plan, timestamp
                ])

                normalized_query = normalize_query(json.dumps(command))
                query_stats[normalized_query]["count"] += 1
                query_stats[normalized_query]["durations"].append(duration)
                query_stats[normalized_query]["sample_query"] = json.dumps(command)
            else:
                non_slow_query_data.append(line)
                if 'msg' in json_payload and 'error' in json_payload.get('attr', {}):
                    msg = json_payload.get('msg', '')
                    error = json_payload['attr'].get('error', {}).get('codeName', '')
                    errmsg = json_payload['attr'].get('error', {}).get('errmsg', '')

                    error_stats[msg]["totalCount"] += 1
                    error_stats[msg]["SampleLine"] = line

                    error_data.append(
                        [index, msg, error, errmsg, error_stats[msg]["totalCount"], error_stats[msg]["SampleLine"]]
                    )
        except json.JSONDecodeError:
            st.warning(f"Line {index}: Invalid JSON. Skipped.")
        except Exception as e:
            st.warning(f"Line {index}: {e}. Skipped.")

    output_df = pd.DataFrame(data, columns=output_columns)
    non_slow_query_df = pd.DataFrame(non_slow_query_data, columns=['Line'])
    error_df = pd.DataFrame(error_data, columns=error_columns)

    query_stats_data = []
    for query, stats in query_stats.items():
        durations = stats["durations"]
        if durations:  # Prevent min/max/mean errors
            query_stats_data.append({
                "Query": query,
                "Executions": stats["count"],
                "Min Duration(ms)": min(durations),
                "Max Duration(ms)": max(durations),
                "Avg Duration(ms)": mean(durations),
                "Sample Query": stats["sample_query"]
            })
    query_stats_df = pd.DataFrame(query_stats_data)

    # Show dataframes on the page
    st.subheader("Detailed Metrics")
    st.dataframe(output_df)

    st.subheader("Query Stats")
    st.dataframe(query_stats_df)

    st.subheader("Non-Slow Queries")
    st.dataframe(non_slow_query_df)

    st.subheader("Error Stats")
    st.dataframe(error_df)

    # Prepare Excel for download
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        output_df.to_excel(writer, sheet_name='Detailed Metrics', index=False)
        query_stats_df.to_excel(writer, sheet_name='Query Stats', index=False)
        non_slow_query_df.to_excel(writer, sheet_name='Non-Slow Queries', index=False)
        error_df.to_excel(writer, sheet_name='Error Stats', index=False)
    output.seek(0)

    st.download_button(
        label="Download Excel report",
        data=output,
        file_name="mongo_log_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.info("Please upload a MongoDB log file to get started.")
