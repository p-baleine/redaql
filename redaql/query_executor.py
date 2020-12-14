from redash_py.client import RedashAPIClient
from prettytable import PrettyTable


class QueryExecutor:

    def __init__(self, redaql_instance, query_string: str, datasource_name: str, pivot_result: bool):
        self.redaql_instance = redaql_instance
        self.query_string = query_string
        self.datasource_name = datasource_name
        self.pivot_result = pivot_result

    def execute_query(self):
        client: RedashAPIClient = self.redaql_instance.client
        result = client.get_adhoc_query_result(
            query=self.query_string,
            data_source_name=self.datasource_name,
            retry_count=1000,
        )
        query_result = result['query_result']
        rows = query_result['data']['rows']
        columns = query_result['data']['columns']
        column_names = [col['name'] for col in columns]
        if rows:
            if self.pivot_result:
                return self._get_pivot_report(rows, column_names)

            else:
                return self._get_pretty_report(rows, column_names)
        return 'no results.\n'

    def _get_pretty_report(self, base_data, columns):
        table = PrettyTable(columns)
        for row in base_data:
            row_data = [row[col] for col in columns]
            table.add_row(row_data)
        return table

    def _get_pivot_report(self, base_data, columns):
        result = ''
        max_col_name_length = max([len(col) for col in columns])

        for row in base_data:
            result += '-' * max_col_name_length + '\n'
            for col in columns:
                result += f'{col.ljust(max_col_name_length)}: {row[col]}\n'
        return result
