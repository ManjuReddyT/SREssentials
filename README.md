# SREssentials
SREssentials is a comprehensive toolkit designed to support Site Reliability Engineers (SREs) in managing, monitoring, and optimizing system reliability and performance. It contains a collection of scripts and tools that streamline essential SRE tasks, including log parsing, error tracking, database query analysis, and automated monitoring. With a focus on observability and efficiency, SREssentials helps engineers gain deeper insights into system behavior, detect and troubleshoot issues faster, and maintain stable, high-performing services.

## Key Features:
- **Log Parsing**: Tools to parse and analyze logs from various sources like MongoDB and MySQL, helping to identify slow queries, errors, and other key metrics.
- **Error Tracking**: Modules that gather and structure error logs, making it easier to diagnose and address issues across complex systems.
- **Performance Monitoring**: Scripts that analyze system and application performance, highlighting bottlenecks and optimizing resource usage.
- **Automation Scripts**: Essential scripts to automate repetitive tasks, reducing manual workload and ensuring consistent reliability practices.
- **Report Generation**: Generates structured reports (e.g., Excel, JSON) for easy sharing and analysis across teams.
Whether you’re managing logs, analyzing metrics, or tracking down errors, SREssentials provides the foundational tools to help you maintain reliability and improve the operational efficiency of large-scale systems.


# MongoDB Log Parser

This MongoDB Log Parser script extracts, normalizes, and analyzes information from MongoDB log files, specifically targeting slow queries, general query metrics, and error statistics. The output is saved to an Excel file, providing structured insights for efficient database monitoring and troubleshooting.

## Features

- **Slow Query Analysis**: Identifies and extracts key details from slow queries in MongoDB logs, including query duration, keys examined, and documents examined.
- **Query Normalization**: Normalizes queries by replacing specific values with placeholders, allowing for consistent tracking of query patterns.
- **Error Detection**: Captures error messages and relevant details, helping database administrators quickly pinpoint issues.
- **Excel Output**: Saves detailed logs, query statistics, and error information to an Excel file for easy review and analysis.

## Requirements

- **Python** (3.8 or above)
- **Pandas** (for data manipulation)
- **XlsxWriter** (for Excel output)

Install dependencies with:
```bash
pip install pandas xlsxwriter
```

## Usage

1. **Clone the Repository**
   ```bash
   git clone https://github.com/ManjuReddyT/SREssentials.git
   cd parser
   ```
2. Run the Script Execute the script and follow the prompts to provide the paths for the MongoDB log file and the output Excel file.
   ```bash
   python mongo_parser.py
   ```
3. Command-Line Prompts
    - Log File Path: Provide the full path to your MongoDB log file.
    - Output File Path: Specify the path where you want the output Excel file saved.

