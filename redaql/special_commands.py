import sys

from abc import ABC, abstractmethod
from redash_py.client import RedashAPIClient
from exceptions import NotFoundDataSourceException


class Executor(ABC):

    def __init__(self, redaql_instance, *args):
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
            for ds in client.get_data_sources():
                print(f'{ds["name"]}:{ds["type"]}')
                ds_names.append(ds['name'])
            self.redaql_instance.complete_sources += ds_names

        else:
            # set datasource name
            input_ds_name = self.args[0]
            ds_names = [ds['name'] for ds in client.get_data_sources()]
            if input_ds_name not in ds_names:
                raise NotFoundDataSourceException(f'{input_ds_name} is not exists.')
            self.redaql_instance.data_source_name = input_ds_name
            self.redaql_instance.reset_completer()
            res = client.get_data_source_schema(input_ds_name)
            schemas = [schema['name'] for schema in res['schema']]
            self.redaql_instance.set_query_mode_completer(schemas)


SP_COMMANDS = {
    'c': ConnectionExecutor,
    'q': ExitExecutor,
    'x': PivotExecutor,
    '?': HelpExecutor,
}
