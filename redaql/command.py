import sys
import re
import traceback

from os.path import expanduser

from redaql import utils
from redaql import exceptions
from redaql import special_commands
from redaql import constants
from redaql.query_executor import QueryExecutor

from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.completion import FuzzyWordCompleter

from redash_py.client import RedashAPIClient
from redash_py.exceptions import RedashPyException


class SpecialCommandHandler:
    def __init__(self, redaql_instance, command):
        commands = re.split(' +', command.strip())
        self.redaql_instance = redaql_instance
        self.sp_command = commands[0].split('\\')[1]
        self.option = []
        if len(commands) > 1:
            self.option = commands[1:]

    def execute(self):
        if self.sp_command not in special_commands.SP_COMMANDS:
            raise exceptions.InvalidSpCommandException(
                f'{self.sp_command} is not a valid special command.'
            )
        executor = special_commands.SP_COMMANDS[self.sp_command]
        executor(self.redaql_instance, *self.option).execute()


class Redaql:
    def __init__(self):
        self.client = RedashAPIClient(
            api_key=None,
            host=None,
            proxy=None,
            timeout=None,
        )
        self.data_source_name = None
        self.pivot_result = False
        self.buffer = []
        self.complete_sources = []
        self.complete_meta_dict = {}
        self.history = FileHistory(f'{expanduser("~")}/.redaql.hist')
        self.init()

    def init(self):
        version = self.client.get_server_version()
        print(f'\nsuccess connect server version {version}\n')
        if self.data_source_name is None:
            self.complete_sources += [d['name'] for d in self.client.get_data_sources()]

    def loop(self):
        try:
            answer = prompt(
                self._get_prompt,
                history=self.history,
                completer=self._get_completer(),
            )
            self.handle(answer)
        except (exceptions.RedaqlException, RedashPyException) as e:
            print(e)
            self.buffer = []
        except KeyboardInterrupt as e:
            print('if want to exit, use \\q')
            self.buffer = []
        except EOFError as e:
            print('Bye.')
            sys.exit(0)

    def handle(self, text):
        if text == '':
            return

        if utils.is_special_command(text):
            self.execute_special_command(text)
            self.buffer = []
            return

        if not self.data_source_name:
            print('select datasource via \\c')
            return

        self.buffer.append(text)
        if utils.is_end(text):
            self.execute_query()
            self.buffer = []
            return

    def execute_query(self):
        executor = QueryExecutor(
            redaql_instance=self,
            query_string=' '.join(self.buffer),
            datasource_name=self.data_source_name,
            pivot_result=self.pivot_result
        )
        results = executor.execute_query()
        print(results)

    def execute_special_command(self, command_string):
        spc_handler = SpecialCommandHandler(self, command_string)
        spc_handler.execute()

    def reset_completer(self):
        self.complete_sources = []
        self.complete_meta_dict = {}

    def set_query_mode_completer(self, schema, meta_dict=None):
        self.complete_sources += constants.SQL_KEYWORDS
        self.complete_sources += schema
        base_meta_dict = {k: 'keyword' for k in constants.SQL_KEYWORDS}
        if meta_dict:
            base_meta_dict.update(meta_dict)
        self.complete_meta_dict = base_meta_dict

    def _get_prompt(self):
        data_source_name = self.data_source_name if self.data_source_name else '(No DataSource)'
        if self.buffer:
            return f'{data_source_name}-# '
        return f'{data_source_name}=# '

    def _get_completer(self):
        self.complete_sources = list(set(self.complete_sources))
        return FuzzyWordCompleter(
            words=self.complete_sources,
            meta_dict=self.complete_meta_dict
        )


def main():
    redaql = Redaql()
    while True:
        try:
            redaql.loop()
        except Exception as e:
            traceback.print_exc()
            sys.exit(1)


if __name__ in '__main__':
    main()


