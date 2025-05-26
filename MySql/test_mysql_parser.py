import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from io import BytesIO

# Assuming mysqlLogParser.py is in the same directory or accessible via PYTHONPATH
from MySql.mysqlLogParser import normalize_query, parse_mysql_log_content, save_to_excel

class TestMySqlParser(unittest.TestCase):

    def test_normalize_query(self):
        query1 = "SELECT * FROM users WHERE id = 123 AND name = 'Test User';"
        expected1 = "SELECT * FROM USERS WHERE ID = ? AND NAME = ?;"
        self.assertEqual(normalize_query(query1), expected1)

        query2 = "INSERT INTO logs (message, level) VALUES ('Error occurred', 5);"
        expected2 = "INSERT INTO LOGS (MESSAGE, LEVEL) VALUES (?, ?);"
        self.assertEqual(normalize_query(query2), expected2)

        query3 = "UPDATE products SET price = 19.99, stock = 100 WHERE sku = 'ABC-123';"
        expected3 = "UPDATE PRODUCTS SET PRICE = ?, STOCK = ? WHERE SKU = ?;"
        self.assertEqual(normalize_query(query3), expected3)
        
        query4 = "SELECT name from Customers where city = 'New York'"
        expected4 = "SELECT NAME FROM CUSTOMERS WHERE CITY = ?"
        self.assertEqual(normalize_query(query4), expected4)

    sample_log_content_simple = """
# Time: 231026 10:00:00
# User@Host: root[root] @ localhost [] thread_id: 1234 exec_time: 0.000222 lock_time: 0.000000 rows_sent: 1 rows_examined: 0
SET timestamp=1698300000;
SELECT * FROM table1 WHERE id = 1;
# Time: 231026 10:01:00
# User@Host: user1[user1] @ 192.168.1.5 [] thread_id: 1235 exec_time: 0.005000 lock_time: 0.000100 rows_sent: 10 rows_examined: 200
# Query_time: 0.004500 Lock_time: 0.000050 Rows_sent: 5 Rows_examined: 100
SET timestamp=1698300060;
SELECT name, email FROM users WHERE status = 'active' AND age > 30;
"""
    # Note: The parser expects Query_time, Lock_time etc. on a separate line for some fields based on original regex.
    # The sample_log_content_simple might need adjustment if the regexes are very specific.
    # The current regexes in mysqlLogParser.py:
    # query_time_pattern = re.compile(r'# Query_time: (.*?) Lock_time:')
    # lock_time_pattern = re.compile(r'Lock_time: (.*?) Rows_sent:')
    # rows_sent_pattern = re.compile(r'Rows_sent: (.*?) Rows_examined:')
    # rows_examined_pattern = re.compile(r'Rows_examined: (.*?)\n')
    # This implies that User@Host line might have exec_time, but Query_time, Lock_time etc are on a line starting with # Query_time
    
    # Adjusted sample log based on regex expectations
    sample_log_content_adjusted = """
# Time: 231026 10:00:00
# User@Host: root[root] @ localhost [] thread_id: 1234 exec_time: 0.000222 lock_time: 0.000000 rows_sent: 1 rows_examined: 0
# Query_time: 0.000200 Lock_time: 0.000010 Rows_sent: 1 Rows_examined: 1
SET timestamp=1698300000;
SELECT * FROM table1 WHERE id = 1;
# Time: 231026 10:01:00
# User@Host: user1[user1] @ 192.168.1.5 [] thread_id: 1235 exec_time: 0.005000 lock_time: 0.000100 rows_sent: 10 rows_examined: 200
# Query_time: 0.004500 Lock_time: 0.000050 Rows_sent: 5 Rows_examined: 100
SET timestamp=1698300060;
SELECT name, email FROM users WHERE status = 'active' AND age > 30;
# Time: 231026 10:02:00
# User@Host: root[root] @ localhost [] thread_id: 1236 exec_time: 0.000300 lock_time: 0.000000 rows_sent: 1 rows_examined: 0
# Query_time: 0.000250 Lock_time: 0.000000 Rows_sent: 0 Rows_examined: 0
SET timestamp=1698300120;
COMMIT;
# Time: 231026 10:03:00
# User@Host: user2[user2] @ client-host.com [] thread_id: 1237
# Query_time: 0.120000 Lock_time: 0.000500 Rows_sent: 50 Rows_examined: 5000
SET timestamp=1698300180;
SELECT
    product_id,
    product_name,
    COUNT(*) AS orders
FROM
    order_items
WHERE
    order_date >= '2023-01-01'
GROUP BY
    product_id, product_name
ORDER BY
    orders DESC;
# Time: 231026 10:00:00
# User@Host: root[root] @ localhost [] thread_id: 1238 exec_time: 0.000222 lock_time: 0.000000 rows_sent: 1 rows_examined: 0
# Query_time: 0.000200 Lock_time: 0.000010 Rows_sent: 1 Rows_examined: 1
SET timestamp=1698300000;
SELECT * FROM table1 WHERE id = 1;
""" # Added a duplicate query for aggregation testing

    def test_parse_mysql_log_content_valid(self):
        df_detailed, df_aggregated, parse_warnings = parse_mysql_log_content(self.sample_log_content_adjusted)

        # Check for unexpected parsing warnings
        # Depending on how "COMMIT;" is handled, there might be a warning.
        # The current regex `SET timestamp=.*?;\n(.*?)(?=\n# Time:|\Z)` might not capture "COMMIT;" as a query
        # if it's not preceded by "SET timestamp". Let's assume COMMIT is not captured.
        # Expected warnings: One for COMMIT if it's not parsed as a query.
        # The query_pattern `SET timestamp=.*?;\n(.*?)(?=\n# Time:|\Z)` requires "SET timestamp"
        # The COMMIT entry does not have SET timestamp, so it will be skipped by the main loop's `if time_match and ... and query_match:`
        # So, one "Skipped log entry" warning is expected for the COMMIT block.
        self.assertEqual(len(parse_warnings), 1, f"Expected 1 parsing warning, got {len(parse_warnings)}: {parse_warnings}")
        self.assertTrue("Skipped log entry 3" in parse_warnings[0])


        # Detailed DataFrame assertions
        self.assertEqual(len(df_detailed), 4) # 3 unique queries + 1 duplicate = 4 entries
        
        # First entry
        self.assertEqual(df_detailed.iloc[0]['Time'], '231026 10:00:00')
        self.assertEqual(df_detailed.iloc[0]['User@Host'], 'root[root] @ localhost []')
        self.assertAlmostEqual(df_detailed.iloc[0]['Query_time (ms)'], 0.200) # 0.000200s * 1000
        self.assertEqual(df_detailed.iloc[0]['Lock_time'], '0.000010')
        self.assertEqual(df_detailed.iloc[0]['Rows_sent'], '1')
        self.assertEqual(df_detailed.iloc[0]['Rows_examined'], '1')
        self.assertEqual(df_detailed.iloc[0]['Query'], 'SELECT * FROM table1 WHERE id = 1;')
        self.assertEqual(df_detailed.iloc[0]['Normalized_Query'], 'SELECT * FROM TABLE1 WHERE ID = ?;')

        # Third entry (multi-line)
        expected_multiline_query = """SELECT
    product_id,
    product_name,
    COUNT(*) AS orders
FROM
    order_items
WHERE
    order_date >= '2023-01-01'
GROUP BY
    product_id, product_name
ORDER BY
    orders DESC;"""
        self.assertEqual(df_detailed.iloc[2]['Query'], expected_multiline_query.strip())
        self.assertAlmostEqual(df_detailed.iloc[2]['Query_time (ms)'], 120.0)

        # Aggregated DataFrame assertions
        self.assertEqual(len(df_aggregated), 3) # 3 unique normalized queries

        # Stats for "SELECT * FROM TABLE1 WHERE ID = ?;" (executed twice)
        agg_row1 = df_aggregated[df_aggregated['Normalized_Query'] == 'SELECT * FROM TABLE1 WHERE ID = ?;'].iloc[0]
        self.assertEqual(agg_row1['Executions'], 2)
        self.assertAlmostEqual(agg_row1['Min_Query_time_ms'], 0.200)
        self.assertAlmostEqual(agg_row1['Max_Query_time_ms'], 0.200)
        self.assertAlmostEqual(agg_row1['Avg_Query_time_ms'], 0.200)
        self.assertEqual(agg_row1['Sample_Query'], 'SELECT * FROM table1 WHERE id = 1;')

        # Stats for "SELECT NAME, EMAIL FROM USERS WHERE STATUS = ? AND AGE > ?;"
        agg_row2 = df_aggregated[df_aggregated['Normalized_Query'] == "SELECT NAME, EMAIL FROM USERS WHERE STATUS = ? AND AGE > ?;"].iloc[0]
        self.assertEqual(agg_row2['Executions'], 1)
        self.assertAlmostEqual(agg_row2['Avg_Query_time_ms'], 4.500)


    def test_parse_mysql_log_content_empty(self):
        df_detailed, df_aggregated, parse_warnings = parse_mysql_log_content("")
        self.assertTrue(df_detailed.empty)
        self.assertTrue(df_aggregated.empty)
        self.assertTrue(len(parse_warnings) > 0)
        self.assertIn("Log content seems empty or not structured as expected", parse_warnings[0])

    def test_parse_mysql_log_content_no_valid_entries(self):
        log_content = """
# This is some header
# Not a valid log entry structure
Another line of text
"""
        df_detailed, df_aggregated, parse_warnings = parse_mysql_log_content(log_content)
        self.assertTrue(df_detailed.empty)
        self.assertTrue(df_aggregated.empty)
        self.assertTrue(len(parse_warnings) > 0)
        # This will actually have "Log content seems empty or not structured as expected" if split results in few parts
        # or "No valid log entries were parsed" if it passes the initial split check.
        # Current logic: split by '# Time: '. If `log_entries` (after split) is not empty, it proceeds.
        # If `log_entries[1:]` is empty, it means no entries started with `# Time: ` after the first line.
        # Then `processed_entries` remains 0, leading to "No valid log entries were parsed."
        self.assertIn("No valid log entries were parsed", parse_warnings[-1])


    def test_save_to_excel_empty_dfs(self):
        df_detailed = pd.DataFrame()
        df_aggregated = pd.DataFrame()
        
        output_buffer = BytesIO()
        success, error_msg = save_to_excel(df_detailed, df_aggregated, output_buffer)
        
        self.assertTrue(success)
        self.assertIsNone(error_msg)
        output_buffer.seek(0)
        self.assertTrue(len(output_buffer.getvalue()) > 0) # Excel file has some structure even with empty sheets

    def test_save_to_excel_with_data(self):
        df_detailed_data = {'Time': ['231026 10:00:00'], 'User@Host': ['root'], 'Query_time (ms)': [0.2], 'Lock_time': ['0.00001'], 'Rows_sent': ['1'], 'Rows_examined': ['1'], 'Query': ['SELECT 1'], 'Normalized_Query': ['SELECT ?']}
        df_detailed = pd.DataFrame(df_detailed_data)
        
        df_aggregated_data = {'Normalized_Query': ['SELECT ?'], 'Executions': [1], 'Min_Query_time_ms': [0.2], 'Max_Query_time_ms': [0.2], 'Avg_Query_time_ms': [0.2], 'Sample_Query': ['SELECT 1']}
        df_aggregated = pd.DataFrame(df_aggregated_data)

        output_buffer = BytesIO()
        success, error_msg = save_to_excel(df_detailed, df_aggregated, output_buffer)

        self.assertTrue(success)
        self.assertIsNone(error_msg)
        output_buffer.seek(0)
        self.assertTrue(len(output_buffer.getvalue()) > 0)
        # Further checks could involve reading with pd.read_excel as in mongo tests


if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)

# To run these tests from the SREssentials root directory:
# python -m unittest MySql.test_mysql_parser
# or
# python -m unittest discover -s MySql -p "test_*.py"

    # --- CLI Tests ---
    @patch('MySql.mysqlLogParser.run_streamlit_app')
    @patch('MySql.mysqlLogParser.save_to_excel')
    @patch('MySql.mysqlLogParser.parse_mysql_log_content')
    @patch('builtins.open', new_callable=unittest.mock.mock_open, read_data="# Time: ...\nSET timestamp=...;\nSELECT 1;")
    @patch('builtins.print') # Suppress print statements
    def test_main_cli_mode_success(self, mock_print, mock_open, mock_parse_content, mock_save_to_excel, mock_run_streamlit_app):
        mock_parse_content.return_value = (pd.DataFrame(), pd.DataFrame(), []) # df_detailed, df_aggregated, warnings
        mock_save_to_excel.return_value = (True, None) # success, error_msg

        with patch('sys.argv', ['mysqlLogParser.py', '--input', 'dummy.log', '--output', 'out.xlsx']):
            from MySql.mysqlLogParser import main as mysql_main
            mysql_main()

        mock_open.assert_called_once_with('dummy.log', 'r', encoding='utf-8')
        mock_parse_content.assert_called_once_with("# Time: ...\nSET timestamp=...;\nSELECT 1;")
        mock_save_to_excel.assert_called_once_with(
            mock_parse_content.return_value[0], # df_detailed
            mock_parse_content.return_value[1], # df_aggregated
            'out.xlsx'
        )
        mock_run_streamlit_app.assert_not_called()
        self.assertTrue(any("Successfully parsed" in call.args[0] for call in mock_print.call_args_list if call.args))

    @patch('MySql.mysqlLogParser.run_streamlit_app')
    @patch('builtins.open', side_effect=FileNotFoundError("File not found"))
    @patch('builtins.print')
    def test_main_cli_mode_file_not_found(self, mock_print, mock_open_file, mock_run_streamlit_app):
        with patch('sys.argv', ['mysqlLogParser.py', '--input', 'nonexistent.log', '--output', 'out.xlsx']):
            from MySql.mysqlLogParser import main as mysql_main
            mysql_main()

        mock_open_file.assert_called_once_with('nonexistent.log', 'r', encoding='utf-8')
        mock_run_streamlit_app.assert_not_called()
        self.assertTrue(any("Error: Input file 'nonexistent.log' not found." in call.args[0] for call in mock_print.call_args_list if call.args))

    @patch('MySql.mysqlLogParser.run_streamlit_app')
    @patch('MySql.mysqlLogParser.save_to_excel')
    @patch('MySql.mysqlLogParser.parse_mysql_log_content')
    @patch('builtins.print')
    def test_main_streamlit_mode(self, mock_print, mock_parse, mock_save, mock_run_streamlit_app):
        with patch('sys.argv', ['mysqlLogParser.py']):
            from MySql.mysqlLogParser import main as mysql_main
            mysql_main()
        
        mock_run_streamlit_app.assert_called_once()
        mock_parse.assert_not_called()
        mock_save.assert_not_called()

    @patch('MySql.mysqlLogParser.run_streamlit_app')
    @patch('argparse.ArgumentParser.print_help')
    @patch('builtins.print')
    def test_main_cli_mode_missing_output_arg(self, mock_print, mock_print_help, mock_run_streamlit_app):
        with patch('sys.argv', ['mysqlLogParser.py', '--input', 'dummy.log']):
            from MySql.mysqlLogParser import main as mysql_main
            mysql_main()

        mock_run_streamlit_app.assert_not_called()
        self.assertTrue(any("Both --input and --output arguments are required" in call.args[0] for call in mock_print.call_args_list if call.args))
        mock_print_help.assert_called_once()

    @patch('MySql.mysqlLogParser.run_streamlit_app')
    @patch('argparse.ArgumentParser.print_help')
    @patch('builtins.print')
    def test_main_cli_mode_missing_input_arg(self, mock_print, mock_print_help, mock_run_streamlit_app):
        with patch('sys.argv', ['mysqlLogParser.py', '--output', 'out.xlsx']):
            from MySql.mysqlLogParser import main as mysql_main
            mysql_main()

        mock_run_streamlit_app.assert_not_called()
        self.assertTrue(any("Both --input and --output arguments are required" in call.args[0] for call in mock_print.call_args_list if call.args))
        mock_print_help.assert_called_once()
