from redash_py.client import RedashAPIClient


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
        """
        インデント等がきれいに揃った形で結果を戻す

        :param list[dict] base_data: 元データ
        :param list[str] columns: 元データのうち表示する列のリスト
        :return:
        """

        ret_str = ""
        # 各列の最大長のDictを作る
        max_length_dict = {x: self.get_max_str_length(base_data, x) for x in columns}

        # まずはヘッダーを作る
        header_str = "  ".join([x.rjust(max_length_dict[x]) for x in columns])
        # ヘッダーの下の---- --- みたいなところを作る
        header_line = "  ".join(["-".rjust(max_length_dict[x], "-") for x in columns])
        ret_str += "{}\n".format(header_str)
        ret_str += "{}\n".format(header_line)

        # データ部分を追加していく
        for row_data in base_data:
            row_data_list = []
            for col in columns:
                col_data = row_data[col]
                # 文字列でない場合はstr()でキャストする
                if hasattr(col_data, "rjust"):
                    if col_data:
                        row_data_list.append(col_data.rjust(max_length_dict[col]))
                else:
                    # Noneは0にしてしまう
                    if col_data:
                        row_data_list.append(str(col_data).rjust(max_length_dict[col]))
                    else:
                        row_data_list.append(str(0).rjust(max_length_dict[col]))
            # 1行分の組み立て
            ret_str += "{}\n".format("  ".join(row_data_list))

        return ret_str

    def _get_pivot_report(self, base_data, columns):
        result = ''
        max_col_name_length = max([len(col) for col in columns])

        for row in base_data:
            for col in columns:
                result += f'{col.ljust(max_col_name_length)}: {row[col]}\n'
        return result

    @staticmethod
    def get_max_str_length(raw_list_dict_data, column_name):
        """
        指定したlist[dict]について指定した各列を取得し、その列の最大長を戻す

        :param list[dict] raw_list_dict_data:
        :param str column_name:
        :return:
        """
        max_length = max([len(str(dict_[column_name])) for dict_ in raw_list_dict_data])
        # カラム名も結局ヘッダに使うのでそちらのほうが長いかもチェック
        if len(column_name) > max_length:
            return len(column_name)
        return max_length
