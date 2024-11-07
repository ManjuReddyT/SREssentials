# parser
Parser is a powerful tool designed to parse and analyze log files from MongoDB and MySQL databases. It provides structured insights from raw logs, helping developers and database administrators troubleshoot and monitor database activity efficiently.

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
   git clone https://github.com/yourusername/parser.git
   cd parser
   ```
2. Run the Script Execute the script and follow the prompts to provide the paths for the MongoDB log file and the output Excel file.
   ```bash
   python mongo_parser.py
   ```
3. Command-Line Prompts
    - Log File Path: Provide the full path to your MongoDB log file.
    - Output File Path: Specify the path where you want the output Excel file saved.

