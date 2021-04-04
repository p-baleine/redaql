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
            retry_count=100000,
            max_age=0,
        )
        query_result = result['query_result']
        rows = query_result['data']['rows']
        columns = query_result['data']['columns']
        column_names = [col['name'] for col in columns]
        runtime = query_result['runtime']
        results = []
        if rows:
            if self.pivot_result:
                results = self._get_pivot_report(rows, column_names)

            else:
                results = self._get_pretty_report(rows, column_names)
        if not results:
            return 'no rows returned.\n'

        return_message = f'{len(rows)} rows returned.'
        if len(rows) == 1:
            return_message = f'1 row returned.'
        runtime_message = f'Time: {round(runtime, 4)}s'
        return f'\n{results} \n\n{return_message}\n{runtime_message}\n'

    def _get_pretty_report(self, base_data, columns):
        table = PrettyTable(columns)
        for row in base_data:
            row_data = [row[col] for col in columns]
            table.add_row(row_data)
        return table.get_string()

    def _get_pivot_report(self, base_data, columns):
        result = ''
        max_col_name_length = max([len(col) for col in columns])

        for row in base_data:
            result += '-' * max_col_name_length + '\n'
            for col in columns:
                result += f'{col.ljust(max_col_name_length)}: {row[col]}\n'
        return result
