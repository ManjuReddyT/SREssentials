# SREssentials
SREssentials is a comprehensive toolkit designed to support Site Reliability Engineers (SREs) in managing, monitoring, and optimizing system reliability and performance. It contains a collection of scripts and tools that streamline essential SRE tasks, including log parsing, error tracking, database query analysis, and automated monitoring. With a focus on observability and efficiency, SREssentials helps engineers gain deeper insights into system behavior, detect and troubleshoot issues faster, and maintain stable, high-performing services.

## Key Features:
- **Log Parsing**: Tools to parse and analyze logs from various sources like MongoDB and MySQL, helping to identify slow queries, errors, and other key metrics.
- **Error Tracking**: Modules that gather and structure error logs, making it easier to diagnose and address issues across complex systems.
- **Performance Monitoring**: Scripts that analyze system and application performance, highlighting bottlenecks and optimizing resource usage.
- **Automation Scripts**: Essential scripts to automate repetitive tasks, reducing manual workload and ensuring consistent reliability practices.
- **Report Generation**: Generates structured reports (e.g., Excel, JSON) for easy sharing and analysis across teams.
- **Dual Interface**: Both MongoDB and MySQL parsers now support a command-line interface (CLI) for batch processing and an interactive Streamlit web interface for ease of use.

Whether you’re managing logs, analyzing metrics, or tracking down errors, SREssentials provides the foundational tools to help you maintain reliability and improve the operational efficiency of large-scale systems.


# MongoDB Log Parser

This MongoDB Log Parser script extracts, normalizes, and analyzes information from MongoDB log files, specifically targeting slow queries, general query metrics, and error statistics. The output is saved to an Excel file, providing structured insights for efficient database monitoring and troubleshooting. It can be run via a command-line interface or an interactive Streamlit web UI.

## Features

- **Slow Query Analysis**: Identifies and extracts key details from slow queries in MongoDB logs, including query duration, keys examined, and documents examined.
- **Query Normalization**: Normalizes queries by replacing specific values with placeholders, allowing for consistent tracking of query patterns.
- **Error Detection**: Captures error messages and relevant details, helping database administrators quickly pinpoint issues.
- **Excel Output**: Saves detailed logs, query statistics, and error information to an Excel file for easy review and analysis.
- **Dual Mode Operation**: Supports both CLI for automated processing and a Streamlit web UI for interactive analysis.

## Requirements

- **Python** (3.8 or above)
- **Pandas** (for data manipulation)
- **XlsxWriter** (for Excel output)
- **Streamlit** (for the web interface)

Install dependencies with:
```bash
pip install pandas xlsxwriter streamlit
```

## Usage

1.  **Clone the Repository** (if you haven't already):
    ```bash
    git clone https://github.com/ManjuReddyT/SREssentials.git
    cd SREssentials 
    ```
    *(Note: The original README mentioned `cd parser`, ensure your script paths are correct relative to the repo root, e.g., `Mongo/mongo_parser.py`)*

2.  **Choose your mode of operation**:

    *   **Streamlit Web Interface (Recommended for interactive use)**:
        To use the web interface, run the script without any command-line arguments:
        ```bash
        python Mongo/mongo_parser.py
        ```
        This will launch a web application in your browser. You can then:
        1.  Upload your MongoDB log file using the file uploader.
        2.  View the parsed dataframes for Detailed Metrics, Query Stats, Non-Slow Queries, and Error Stats directly in the app.
        3.  Download the complete report as an Excel file (`mongo_log_report.xlsx`).

    *   **Command-Line Interface (CLI) (For batch processing or automation)**:
        To use the CLI, you need to provide the input log file path and the desired output Excel file path.
        ```bash
        python Mongo/mongo_parser.py --input /path/to/your/mongod.log --output /path/to/your/report.xlsx
        ```
        Replace `/path/to/your/mongod.log` with the actual path to your MongoDB log file and `/path/to/your/report.xlsx` with where you want to save the Excel report.

        **CLI Arguments**:
        *   `-i, --input FILE_PATH`: Path to the input MongoDB log file.
        *   `-o, --output FILE_PATH`: Path to save the output Excel report.

# MySQL Log Parser

This script is designed to parse MySQL log files (particularly slow query logs) and extract detailed performance metrics. It provides both individual query metrics and aggregated analysis. The output is saved as an Excel file with two sheets: "Detailed Metrics" and "Aggregate Results." It now features both a Streamlit web interface and an updated command-line interface.

## Features

- **Extracts Key Metrics**: Captures execution time, lock time, rows sent, rows examined, and user/host information from log entries.
- **Query Normalization**: Replaces specific literals and numbers with placeholders ('?') to group similar queries for effective aggregation.
- **Aggregate Analysis**: Summarizes executions of each normalized query, providing count, min, max, and average execution times, along with a sample query.
- **Dual Mode Operation**: Offers a user-friendly Streamlit web interface for interactive analysis and a command-line interface (CLI) for batch processing.
- **Excel Output**: Generates a report in Excel format with two sheets: "Detailed Metrics" and "Aggregate Results".

## Requirements

- Python 3.7+
- `pandas` library for data manipulation.
- `xlsxwriter` library for Excel file writing (Note: changed from `openpyxl` for consistency).
- `streamlit` library for the web interface.

You can install the dependencies via pip:

```bash
pip install pandas xlsxwriter streamlit
```

## Usage

1.  **Clone the Repository** (if you haven't already):
    ```bash
    git clone https://github.com/ManjuReddyT/SREssentials.git
    cd SREssentials
    ```

2.  **Ensure your MySQL log file is available**:
    The parser expects log entries typically found in slow query logs, where entries are often marked by `# Time:` and include `SET timestamp=...;` before the query.

3.  **Choose your mode of operation**:

    *   **Streamlit Web Interface (Recommended for interactive use)**:
        To launch the web UI, run the script without any command-line arguments:
        ```bash
        python MySql/mysqlLogParser.py
        ```
        This will open a web application in your browser. In the UI, you can:
        1.  Upload your MySQL log file (e.g., `mysql-slow.log`, `.txt`).
        2.  View parsing warnings, detailed metrics, and aggregate results directly on the page.
        3.  Download the generated report as an Excel file (`mysql_log_report.xlsx`).

    *   **Command-Line Interface (CLI) (For batch processing or automation)**:
        The script uses `argparse` for CLI arguments. You need to provide the input log file path and the output Excel file path.
        ```bash
        python MySql/mysqlLogParser.py --input /path/to/your/mysql-slow.log --output /path/to/your/mysql_report.xlsx
        ```
        Replace `/path/to/your/mysql-slow.log` with the path to your MySQL log file and `/path/to/your/mysql_report.xlsx` with your desired output file name.

        **CLI Arguments**:
        *   `-i, --input FILE_PATH`: Path to the input MySQL log file.
        *   `-o, --output FILE_PATH`: Path to save the output Excel report.

4.  **View Output**:
    Open the generated Excel file. It will contain two sheets:
    *   **Detailed Metrics**: Shows raw parsed data for each query entry, including Time, User@Host, Query_time (ms), Lock_time, Rows_sent, Rows_examined, the original Query, and its Normalized_Query.
    *   **Aggregate Results**: Provides a summary grouped by `Normalized_Query`, showing `Executions`, `Min_Query_time_ms`, `Max_Query_time_ms`, `Avg_Query_time_ms`, and a `Sample_Query`.