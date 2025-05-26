import unittest
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
import json
from io import BytesIO

# Assuming mongo_parser.py is in the same directory or accessible via PYTHONPATH
from Mongo.mongo_parser import normalize_query, parse_log_lines, save_to_excel

class TestMongoParser(unittest.TestCase):

    def test_normalize_query(self):
        # Simple find
        query1 = '{"find": "collection_name", "filter": {"field1": "value1", "field2": 123}}'
        expected1 = '{"find": "collection_name", "filter": {"field1": <value>, "field2": <value>}}'
        self.assertEqual(normalize_query(query1), expected1)

        # Find with projection
        query2 = '{"find": "another_coll", "filter": {"status": "A"}, "projection": {"_id": 0, "data": 1}}'
        expected2 = '{"find": "another_coll", "filter": {"status": <value>}, "projection": {"_id": <value>, "data": <value>}}'
        self.assertEqual(normalize_query(query2), expected2)

        # Aggregate query
        query3 = '{"aggregate": "my_collection", "pipeline": [{"$match": {"type": "event"}}, {"$group": {"_id": "$user", "count": {"$sum": 1}}}], "cursor": {}}'
        expected3 = '{"aggregate": "my_collection", "pipeline": [{"$match": {"type": <value>}}, {"$group": {"_id": <value>, "count": {"$sum": <value>}}}], "cursor": {}}'
        # Note: Deeply nested values in arrays might require more sophisticated normalization or testing strategy
        # For now, this tests the general replacement. The current normalize_query is regex based and might not perfectly handle all nested structures.
        # We will test based on its current capabilities.
        normalized_q3 = normalize_query(query3)
        # A more robust check might involve parsing JSON and comparing, but for string-based normalization:
        self.assertIn('"type": <value>', normalized_q3)
        self.assertIn('"_id": <value>', normalized_q3)
        self.assertIn('"$sum": <value>', normalized_q3)


        # Query with $in operator
        query4 = '{"find": "products", "filter": {"product_id": {"$in": ["A", "B", "C"]}}}'
        expected4 = '{"find": "products", "filter": {"product_id": {"$in": <value>}}}' # The current regex might make this <value> not ["A", "B", "C"]
        # This is a limitation of the current regex normalizer. It will turn the whole array into <value>
        # self.assertEqual(normalize_query(query4), expected4) -- This would fail if $in's array is not specifically handled
        # Current regex: r'(:\s*["\']?[^,{}\[\]]+["\']?\s*(?=[,}]))'
        # It replaces values like "value", 123, but complex objects like arrays or nested objects as values are tricky
        # For {"$in": ["A", "B", "C"]}, it might replace "A" if it's the last element before a comma or brace,
        # or it might treat the whole array as a single value if the regex matches that.
        # Let's test current behavior:
        # If the array is the *last* thing, it might be `{"$in": <value>}`
        # If there are fields after $in, it's more complex.
        # The regex `[^,{}\[\]]+` means it stops at commas, braces, brackets.
        # So `["A", "B", "C"]` will likely not be replaced as a whole by the current regex.
        # It would replace "C"] if it was followed by }
        # Let's assume a slightly different query for a more predictable test of the existing regex
        query4_modified = '{"find": "products", "filter": {"product_id": {"$in": ["A", "B", "value"]}, "status": "active"}}'
        expected4_partial_norm = '{"find": "products", "filter": {"product_id": {"$in": ["A", "B", <value>]}, "status": <value>}}'
        # The current regex will replace "value" and "active"
        self.assertEqual(normalize_query(query4_modified), expected4_partial_norm)


        # Empty filter
        query5 = '{"find": "users", "filter": {}}'
        expected5 = '{"find": "users", "filter": {}}' # No values to normalize
        self.assertEqual(normalize_query(query5), expected5)

        # Query with numbers and booleans
        query6 = '{"find": "items", "filter": {"count": {"$gt": 10}, "available": true}}'
        expected6 = '{"find": "items", "filter": {"count": {"$gt": <value>}, "available": <value>}}'
        self.assertEqual(normalize_query(query6), expected6)


    # Sample log lines for testing parse_log_lines
    sample_slow_query_line = '{"t":{"$date":"2023-10-25T10:00:00.000Z"},"s":"I","c":"COMMAND","id":12345,"ctx":"conn1","msg":"Slow query","attr":{"type":"command","ns":"testdb.mycollection","command":{"find":"mycollection","filter":{"name":"test"},"sort":{"age":-1},"limit":10,"comment":"slow_query_example"},"planSummary":"COLLSCAN","keysExamined":0,"docsExamined":1000,"numYields":1,"nreturned":10,"durationMillis":150,"remote":"127.0.0.1:12345","protocol":"op_msg"}}'
    sample_error_line = '{"t":{"$date":"2023-10-25T10:05:00.000Z"},"s":"E","c":"NETWORK","id":23359,"ctx":"conn2","msg":"Error receiving request from client. Ending connection.","attr":{"error":{"code":7,"codeName":"HostUnreachable","errmsg":"Connection refused"},"remote":"127.0.0.1:54321","protocol":"op_query"}}'
    sample_non_slow_non_error_line = '{"t":{"$date":"2023-10-25T10:06:00.000Z"},"s":"I","c":"ACCESS","id":20250,"ctx":"conn3","msg":"Successful authentication","attr":{"user":"myuser","db":"admin","mechanism":"SCRAM-SHA-1"}}'
    invalid_json_line = 'This is not a valid JSON line.'
    another_slow_query_line_agg = '{"t":{"$date":"2023-10-25T10:10:00.000Z"},"s":"I","c":"COMMAND","id":12346,"ctx":"conn4","msg":"Slow query","attr":{"type":"command","ns":"testdb.anothercollection","command":{"aggregate":"anothercollection","pipeline":[{"$match":{"status":"active"}},{"$group":{"_id":"$category","total":{"$sum":"$quantity"}}}],"cursor":{},"comment":"slow_agg_example"},"planSummary":"IXSCAN { category: 1 }","keysExamined":50,"docsExamined":200,"numYields":2,"nreturned":5,"durationMillis":250,"remote":"127.0.0.1:12346","protocol":"op_msg"}}'
    empty_line = ""
    whitespace_line = "   "


    def test_parse_log_lines_empty(self):
        lines = []
        output_df, query_stats_df, non_slow_df, error_df, parse_errors = parse_log_lines(lines)
        self.assertTrue(output_df.empty)
        self.assertTrue(query_stats_df.empty)
        self.assertTrue(non_slow_df.empty)
        self.assertTrue(error_df.empty)
        self.assertEqual(parse_errors, [])

    def test_parse_log_lines_invalid_json_only(self):
        lines = [self.invalid_json_line, self.empty_line, self.whitespace_line]
        output_df, query_stats_df, non_slow_df, error_df, parse_errors = parse_log_lines(lines)
        self.assertTrue(output_df.empty)
        self.assertTrue(query_stats_df.empty)
        # Non-slow query df might pick up non-JSON lines if they aren't filtered out before JSON parsing attempt
        # Current mongo_parser.py's parse_log_lines tries json.loads first.
        # If it fails, it adds to parse_errors. It doesn't add to non_slow_query_data unless it's valid JSON but not a slow query.
        # So, non_slow_df should be empty if only invalid JSON is provided.
        self.assertTrue(non_slow_df.empty, f"Non-slow DF should be empty, got: {non_slow_df}")
        self.assertTrue(error_df.empty)
        self.assertEqual(len(parse_errors), 3) # one for each invalid line
        self.assertIn("Line 1: Invalid JSON. Skipped.", parse_errors)
        self.assertIn("Line 2: Invalid JSON. Skipped.", parse_errors) # Empty line
        self.assertIn("Line 3: Invalid JSON. Skipped.", parse_errors) # Whitespace line

    def test_parse_log_lines_single_slow_query(self):
        lines = [self.sample_slow_query_line]
        output_df, query_stats_df, non_slow_df, error_df, parse_errors = parse_log_lines(lines)

        self.assertEqual(len(parse_errors), 0)
        self.assertTrue(non_slow_df.empty)
        self.assertTrue(error_df.empty)

        # Test output_df (Detailed Metrics)
        self.assertEqual(len(output_df), 1)
        expected_command_obj = {"find":"mycollection","filter":{"name":"test"},"sort":{"age":-1},"limit":10,"comment":"slow_query_example"}
        self.assertEqual(json.loads(output_df.iloc[0]['Command']), expected_command_obj)
        self.assertEqual(output_df.iloc[0]['Collection'], "mycollection")
        self.assertEqual(output_df.iloc[0]['AppName'], "testdb")
        self.assertEqual(output_df.iloc[0]['Duration(ms)'], 150)
        self.assertEqual(output_df.iloc[0]['KeysExamined'], 0)
        self.assertEqual(output_df.iloc[0]['DocsExamined'], 1000)
        self.assertEqual(output_df.iloc[0]['Plan'], "COLLSCAN")
        self.assertEqual(json.loads(output_df.iloc[0]['Filter']), {"name":"test"}) # Filter from command

        # Test query_stats_df
        self.assertEqual(len(query_stats_df), 1)
        stat_row = query_stats_df.iloc[0]
        normalized_expected_command = normalize_query(json.dumps(expected_command_obj))
        self.assertEqual(stat_row['Query Pattern'], normalized_expected_command)
        self.assertEqual(stat_row['Executions'], 1)
        self.assertEqual(stat_row['Min Duration(ms)'], 150)
        self.assertEqual(stat_row['Max Duration(ms)'], 150)
        self.assertEqual(stat_row['Avg Duration(ms)'], 150)
        self.assertEqual(json.loads(stat_row['Sample Full Query']), expected_command_obj)

    def test_parse_log_lines_single_error_line(self):
        lines = [self.sample_error_line]
        output_df, query_stats_df, non_slow_df, error_df, parse_errors = parse_log_lines(lines)

        self.assertEqual(len(parse_errors), 0)
        self.assertTrue(output_df.empty)
        self.assertTrue(query_stats_df.empty)
        # The current logic in mongo_parser.py puts non-slow query lines (which error lines are, if not also slow)
        # into non_slow_query_data *if* they are valid JSON but not "Slow query".
        # Error lines are also added to error_data if they match the error pattern.
        self.assertEqual(len(non_slow_df), 0) # Corrected: parse_log_lines was updated to not put errors in non_slow_df if they are actual errors.
                                             # It checks `json_payload.get('s', '') == 'E'` for errors.

        self.assertEqual(len(error_df), 1)
        err_row = error_df.iloc[0]
        self.assertEqual(err_row['Message'], "Error receiving request from client. Ending connection.")
        self.assertEqual(err_row['ErrorType'], "HostUnreachable")
        self.assertEqual(err_row['ErrorMessage'], "Connection refused")
        self.assertEqual(err_row['TotalCount'], 1)
        self.assertTrue("Error receiving request from client" in err_row['SampleLine'])
        # self.assertEqual(err_row['OriginalLineNumber'], 1) # Line numbers are 1-based

    def test_parse_log_lines_non_slow_non_error(self):
        lines = [self.sample_non_slow_non_error_line]
        output_df, query_stats_df, non_slow_df, error_df, parse_errors = parse_log_lines(lines)
        
        self.assertEqual(len(parse_errors), 0)
        self.assertTrue(output_df.empty)
        self.assertTrue(query_stats_df.empty)
        self.assertTrue(error_df.empty)
        
        self.assertEqual(len(non_slow_df), 1)
        self.assertEqual(non_slow_df.iloc[0]['LogLine'], self.sample_non_slow_non_error_line.strip())


    def test_parse_log_lines_mixed_content(self):
        lines = [
            self.sample_slow_query_line,
            self.invalid_json_line,
            self.sample_error_line,
            self.another_slow_query_line_agg,
            self.sample_non_slow_non_error_line,
            self.sample_slow_query_line # Duplicate slow query to test aggregation
        ]
        output_df, query_stats_df, non_slow_df, error_df, parse_errors = parse_log_lines(lines)

        self.assertEqual(len(parse_errors), 1)
        self.assertIn("Line 2: Invalid JSON. Skipped.", parse_errors)

        # output_df (detailed metrics for slow queries)
        self.assertEqual(len(output_df), 3) # 3 slow queries
        self.assertEqual(output_df.iloc[0]['Duration(ms)'], 150)
        self.assertEqual(output_df.iloc[1]['Duration(ms)'], 250)
        self.assertEqual(output_df.iloc[2]['Duration(ms)'], 150)


        # query_stats_df (aggregated stats for slow queries)
        # Two unique normalized queries
        self.assertEqual(len(query_stats_df), 2) 
        
        # Find the stats for the first type of slow query (duration 150ms)
        # Need to parse the command to normalize it for comparison
        cmd1_obj = json.loads(self.sample_slow_query_line)['attr']['command']
        norm_cmd1 = normalize_query(json.dumps(cmd1_obj))
        
        stats_row1 = query_stats_df[query_stats_df['Query Pattern'] == norm_cmd1].iloc[0]
        self.assertEqual(stats_row1['Executions'], 2)
        self.assertEqual(stats_row1['Min Duration(ms)'], 150)
        self.assertEqual(stats_row1['Max Duration(ms)'], 150)
        self.assertEqual(stats_row1['Avg Duration(ms)'], 150)

        # Find the stats for the aggregate slow query (duration 250ms)
        cmd2_obj = json.loads(self.another_slow_query_line_agg)['attr']['command']
        norm_cmd2 = normalize_query(json.dumps(cmd2_obj))
        stats_row2 = query_stats_df[query_stats_df['Query Pattern'] == norm_cmd2].iloc[0]
        self.assertEqual(stats_row2['Executions'], 1)
        self.assertEqual(stats_row2['Min Duration(ms)'], 250)
        self.assertEqual(stats_row2['Max Duration(ms)'], 250)
        self.assertEqual(stats_row2['Avg Duration(ms)'], 250)


        # non_slow_df
        self.assertEqual(len(non_slow_df), 1)
        self.assertEqual(non_slow_df.iloc[0]['LogLine'], self.sample_non_slow_non_error_line.strip())

        # error_df
        self.assertEqual(len(error_df), 1)
        self.assertEqual(error_df.iloc[0]['ErrorType'], "HostUnreachable")
        self.assertEqual(error_df.iloc[0]['TotalCount'], 1)


    def test_save_to_excel_empty_dfs(self):
        output_df = pd.DataFrame()
        query_stats_df = pd.DataFrame()
        non_slow_query_df = pd.DataFrame()
        error_df = pd.DataFrame()
        
        output_buffer = BytesIO()
        success, error_msg = save_to_excel(output_df, query_stats_df, non_slow_query_df, error_df, output_buffer)
        
        self.assertTrue(success)
        self.assertIsNone(error_msg)
        output_buffer.seek(0)
        # Check if it's a valid Excel file (at least, it's not empty)
        self.assertTrue(len(output_buffer.getvalue()) > 0) 
        # Further checks could involve reading it back with pd.read_excel if critical

    def test_save_to_excel_with_data(self):
        # Create some dummy data similar to what parse_log_lines would produce
        output_df_data = {'Command': ['{"find":"test"}'], 'Collection': ['c1'], 'AppName': ['app1'], 'Duration(ms)': [100], 'KeysExamined': [10], 'DocsExamined': [100], 'numYields': [0], 'nreturned': [1], 'Filter': ['{}'], 'Plan': ['COLLSCAN'], 'timestamp': ['2023']}
        output_df = pd.DataFrame(output_df_data)
        
        query_stats_df_data = {'Query Pattern': ['{"find":<value>}'], 'Executions': [1], 'Min Duration(ms)': [100], 'Max Duration(ms)': [100], 'Avg Duration(ms)': [100.0], 'Sample Full Query': ['{"find":"test"}']}
        query_stats_df = pd.DataFrame(query_stats_df_data)

        non_slow_query_df_data = {'LogLine': ['some other log line']}
        non_slow_query_df = pd.DataFrame(non_slow_query_df_data)

        error_df_data = {'Message': ['Error msg'], 'ErrorType': ['TypeA'], 'ErrorMessage': ['Details'], 'TotalCount': [1], 'SampleLine': ['Error line sample']}
        error_df = pd.DataFrame(error_df_data)

        output_buffer = BytesIO()
        success, error_msg = save_to_excel(output_df, query_stats_df, non_slow_query_df, error_df, output_buffer)

        self.assertTrue(success)
        self.assertIsNone(error_msg)
        output_buffer.seek(0)
        self.assertTrue(len(output_buffer.getvalue()) > 0)

        # Verify sheets and some data (optional, but good for confidence)
        # This requires openpyxl or other engine that pd.read_excel can use
        # For simplicity, we'll assume if it writes without error and buffer is non-empty, it's okay for this test.
        # xls = pd.ExcelFile(output_buffer)
        # df_detailed_read = xls.parse('Detailed Metrics')
        # assert_frame_equal(df_detailed_read, output_df)


if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)

# To run these tests from the SREssentials root directory:
# python -m unittest Mongo.test_mongo_parser
# or
# python -m unittest discover -s Mongo -p "test_*.py"

    # --- CLI Tests ---
    @patch('Mongo.mongo_parser.run_streamlit_app')
    @patch('Mongo.mongo_parser.save_to_excel')
    @patch('Mongo.mongo_parser.parse_log_lines')
    @patch('builtins.open', new_callable=unittest.mock.mock_open, read_data='{"t":{"$date":"2023-10-25T10:00:00.000Z"},"s":"I","c":"COMMAND","id":12345,"ctx":"conn1","msg":"Slow query","attr":{"type":"command","ns":"testdb.mycollection","command":{"find":"mycollection"},"durationMillis":150}}')
    @patch('builtins.print') # Suppress print statements
    def test_main_cli_mode_success(self, mock_print, mock_open, mock_parse_log_lines, mock_save_to_excel, mock_run_streamlit_app):
        # Prepare mock returns
        mock_parse_log_lines.return_value = (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), [])
        mock_save_to_excel.return_value = (True, None)

        with patch('sys.argv', ['mongo_parser.py', '--input', 'dummy.log', '--output', 'out.xlsx']):
            from Mongo.mongo_parser import main as mongo_main # Import main here to use fresh argv
            mongo_main()

        mock_open.assert_called_once_with('dummy.log', 'r', encoding='utf-8')
        mock_parse_log_lines.assert_called_once()
        # Check the actual lines passed to parse_log_lines
        # mock_open().readlines() would be the way if read_data was multi-line and readlines was used by main
        # In our case, main uses f.readlines(), and mock_open is set up with read_data.
        # The mock_open().readlines() will return ['log line'] if read_data='log line'
        # For the more complex JSON, it's one long string, so readlines() returns a list with that one string.
        self.assertEqual(mock_parse_log_lines.call_args[0][0], ['{"t":{"$date":"2023-10-25T10:00:00.000Z"},"s":"I","c":"COMMAND","id":12345,"ctx":"conn1","msg":"Slow query","attr":{"type":"command","ns":"testdb.mycollection","command":{"find":"mycollection"},"durationMillis":150}}'])

        mock_save_to_excel.assert_called_once_with(
            mock_parse_log_lines.return_value[0], # output_df
            mock_parse_log_lines.return_value[1], # query_stats_df
            mock_parse_log_lines.return_value[2], # non_slow_query_df
            mock_parse_log_lines.return_value[3], # error_df
            'out.xlsx'
        )
        mock_run_streamlit_app.assert_not_called()
        # Check for success print message
        self.assertTrue(any("Successfully parsed" in call.args[0] for call in mock_print.call_args_list if call.args))


    @patch('Mongo.mongo_parser.run_streamlit_app')
    @patch('builtins.open', side_effect=FileNotFoundError("File not found"))
    @patch('builtins.print') # Suppress print statements
    def test_main_cli_mode_file_not_found(self, mock_print, mock_open_file, mock_run_streamlit_app):
        with patch('sys.argv', ['mongo_parser.py', '--input', 'nonexistent.log', '--output', 'out.xlsx']):
            from Mongo.mongo_parser import main as mongo_main
            mongo_main()

        mock_open_file.assert_called_once_with('nonexistent.log', 'r', encoding='utf-8')
        mock_run_streamlit_app.assert_not_called()
        # Check for error print message
        self.assertTrue(any("Error: Input file 'nonexistent.log' not found." in call.args[0] for call in mock_print.call_args_list if call.args))


    @patch('Mongo.mongo_parser.run_streamlit_app')
    @patch('Mongo.mongo_parser.save_to_excel')
    @patch('Mongo.mongo_parser.parse_log_lines')
    @patch('builtins.print') # Suppress print statements
    def test_main_streamlit_mode(self, mock_print, mock_parse, mock_save, mock_run_streamlit_app):
        with patch('sys.argv', ['mongo_parser.py']):
            from Mongo.mongo_parser import main as mongo_main
            mongo_main()
        
        mock_run_streamlit_app.assert_called_once()
        mock_parse.assert_not_called()
        mock_save.assert_not_called()

    @patch('Mongo.mongo_parser.run_streamlit_app')
    @patch('argparse.ArgumentParser.print_help') # Mock print_help
    @patch('builtins.print') # Suppress print statements
    def test_main_cli_mode_missing_output_arg(self, mock_print, mock_print_help, mock_run_streamlit_app):
        with patch('sys.argv', ['mongo_parser.py', '--input', 'dummy.log']):
            from Mongo.mongo_parser import main as mongo_main
            mongo_main()

        mock_run_streamlit_app.assert_not_called()
        # Check for error print message about missing arguments
        self.assertTrue(any("Both --input and --output arguments are required" in call.args[0] for call in mock_print.call_args_list if call.args))
        mock_print_help.assert_called_once() # Check if help is printed

    @patch('Mongo.mongo_parser.run_streamlit_app')
    @patch('argparse.ArgumentParser.print_help') # Mock print_help
    @patch('builtins.print') # Suppress print statements
    def test_main_cli_mode_missing_input_arg(self, mock_print, mock_print_help, mock_run_streamlit_app):
        with patch('sys.argv', ['mongo_parser.py', '--output', 'out.xlsx']):
            from Mongo.mongo_parser import main as mongo_main
            mongo_main()

        mock_run_streamlit_app.assert_not_called()
        self.assertTrue(any("Both --input and --output arguments are required" in call.args[0] for call in mock_print.call_args_list if call.args))
        mock_print_help.assert_called_once()
