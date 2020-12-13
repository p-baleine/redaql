import sys
import re
from os.path import expanduser

import utils
import exceptions
import special_commands
import constants
from query_executor import QueryExecutor

from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.completion import FuzzyWordCompleter

from redash_py.client import RedashAPIClient


class SpecialCommandHandler:
    def __init__(self, redaql_instance, command):
        commands = re.split(' +', command)
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
        self.client = RedashAPIClient()
        self.data_source_name = None
        self.buffer = []
        self.complete_sources = []
        self.init()

    def init(self):
        version = self.client.get_server_version()
        print(f'\nsuccess connect server version {version}\n')

    def loop(self):
        answer = prompt(
            self._get_prompt,
            history=FileHistory(f'{expanduser("~")}/.redaql.hist'),
            completer=self._get_completer(),
        )
        self.handle(answer)

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
            datasource_name=self.data_source_name
        )
        results = executor.execute_query()
        print(results)

    def execute_special_command(self, command_string):
        spc_handler = SpecialCommandHandler(self, command_string)
        spc_handler.execute()

    def reset_completer(self):
        self.complete_sources = []

    def set_query_mode_completer(self, schema):
        self.complete_sources += constants.SQL_KEYWORDS
        self.complete_sources += schema

    def _get_prompt(self):
        data_source_name = self.data_source_name if self.data_source_name else ''
        if self.buffer:
            return f'{data_source_name}-# '
        return f'{data_source_name}=# '

    def _get_completer(self):
        self.complete_sources = list(set(self.complete_sources))
        return FuzzyWordCompleter(self.complete_sources)


def main():
    redaql = Redaql()
    try:
        while True:
            redaql.loop()
    except (exceptions.RedaqlException, KeyboardInterrupt) as e:
        print(e)
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ in '__main__':
    main()


