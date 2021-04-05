import sys
import fnmatch
import itertools
from functools import lru_cache

from abc import ABC, abstractmethod
from redash_py.client import RedashAPIClient
from redaql.exceptions import NotFoundDataSourceException, FutureFeatureException
from .query_executor import QueryExecutor


class Executor(ABC):

    def __init__(self, redaql_instance, *args):
        """
        :param redaql.command.Redaql redaql_instance:
        :param args:
        """
        self.redaql_instance = redaql_instance
        self.args = args

    @staticmethod
    @abstractmethod
    def help_text():
        raise NotImplemented()

    @abstractmethod
    def execute(self):
        raise NotImplemented()


class HelpExecutor(Executor):

    @staticmethod
    def help_text():
        return 'HELP SP COMMANDS.'

    def execute(self):
        for command, executor in SP_COMMANDS.items():
            print(f'\\{command}: {executor.help_text()}')


class PivotExecutor(Executor):

    @staticmethod
    def help_text():
        return 'query result toggle pivot.'

    def execute(self):
        self.redaql_instance.pivot_result = not self.redaql_instance.pivot_result
        if self.redaql_instance.pivot_result:
            print('set pivot format')
        else:
            print('set normal format')


class ExitExecutor(Executor):

    @staticmethod
    def help_text():
        return 'exit.'

    def execute(self):
        print('Bye.')
        sys.exit(0)


class ConnectionExecutor(Executor):

    @staticmethod
    def help_text():
        return 'SELECT DATASOURCE.'

    def execute(self):
        client: RedashAPIClient = self.redaql_instance.client
        if not self.args:
            # show datasource name
            ds_names = []
            message = ''
            for ds in client.get_data_sources():
                message += f'{ds["name"]}:{ds["type"]}\n'
                ds_names.append(ds['name'])
            self.redaql_instance.complete_sources += ds_names
            return message
        else:
            # set datasource name
            input_ds_name = self.args[0]
            ds_names = [ds['name'] for ds in client.get_data_sources()]
            if input_ds_name not in ds_names:
                raise NotFoundDataSourceException(f'{input_ds_name} is not exists.')
            self.redaql_instance.data_source_name = input_ds_name
            self.redaql_instance.reset_completer()
            res = client.get_data_source_schema(input_ds_name)
            if 'schema' not in res:
                return
            schemas = [schema['name'] for schema in res['schema']]
            columns = list(
                itertools.chain.from_iterable([schema['columns'] for schema in res['schema']])
            )
            meta_dict = {s: 'table' for s in schemas}
            meta_dict.update(
                {c: 'column' for c in columns}
            )
            self.redaql_instance.set_query_mode_completer(
                schema=schemas + columns,
                meta_dict=meta_dict
            )


class DescExecutor(Executor):

    @staticmethod
    def help_text():
        return 'describe table.'

    def execute(self):
        if not self.args:
            schemas = self._get_schemas()
            messages = ''
            for schema in schemas:
                messages += f'{schema["name"]}\n'
            return messages
        else:
            table_name = self.args[0]
            schemas = self._get_schemas()
            is_notfound = True
            messages = ''
            for schema in schemas:
                if fnmatch.fnmatch(schema['name'], table_name):
                    messages += f'## {schema["name"]}\n'
                    messages += ('\n'.join(
                        [f'- {c}'for c in schema['columns']]
                    ))
                    messages += '\n'
                    is_notfound = False
            if is_notfound:
                messages += f'No Such table {table_name}'
            return messages

    @lru_cache()
    def _get_schemas(self):
        client: RedashAPIClient = self.redaql_instance.client
        schemas = client.get_data_source_schema(
            self.redaql_instance.data_source_name
        )
        if 'schema' not in schemas:
            return []
        return schemas['schema']


class LoadExecutor(Executor):

    @staticmethod
    def help_text():
        return 'Load Query from Redash.'

    def execute(self):
        client: RedashAPIClient = self.redaql_instance.client
        args = self.args
        messages = ''
        if not args or not args[0].isdigit():
            messages += 'need query_id'
            return messages
        query_id = int(args[0])
        query = client.get_query_by_id(query_id)
        sql = query['query']
        data_source_id = query['data_source_id']
        options = query['options']
        # TODO need fix
        if len(options['parameters']) > 0:
            raise FutureFeatureException(f'Query ID {query_id} need some parameters. Cannot execute.')

        data_sources = client.get_data_sources()
        data_source_name = [ds['name'] for ds in data_sources if ds['id'] == data_source_id][0]

        self.redaql_instance.history.append_string(sql)
        executor = QueryExecutor(
            redaql_instance=self.redaql_instance,
            query_string=sql,
            pivot_result=self.redaql_instance.pivot_result,
            datasource_name=data_source_name,
        )
        return executor.execute_query()


SP_COMMANDS = {
    'c': ConnectionExecutor,
    'q': ExitExecutor,
    'd': DescExecutor,
    'x': PivotExecutor,
    'l': LoadExecutor,
    '?': HelpExecutor,
}
