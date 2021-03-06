import argparse
import sys
import re
import traceback
import dataclasses

from typing import Optional
from os.path import expanduser
from textwrap import dedent

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
from redaql.__version__ import __VERSION__


@dataclasses.dataclass(frozen=True)
class Args:
    api_key: Optional[str]
    host: Optional[str]
    proxy: Optional[str]
    initial_data_source_name: Optional[str]

    def to_dict(self):
        return dataclasses.asdict(self)


class Redaql:
    def __init__(
        self,
        api_key=None,
        host=None,
        proxy=None,
        initial_data_source_name=None,
    ):
        self.client = RedashAPIClient(
            api_key=api_key,
            host=host,
            proxy=proxy,
            timeout=None,
        )
        self.data_source_name = initial_data_source_name
        self.pivot_result = False
        self.buffer = []
        self.complete_sources = []
        self.complete_meta_dict = {}
        self.history = FileHistory(f'{expanduser("~")}/.redaql.hist')
        self.init()

    def init(self):
        version = self.client.get_server_version()
        print(dedent(f"""
           ___         __          __
          / _ \___ ___/ /__ ____ _/ /
         / , _/ -_) _  / _ `/ _ `/ / 
        /_/|_|\__/\_,_/\_,_/\_, /_/  
                             /_/     
          - redash query cli tool -

        SUCCESS CONNECT
        - server version {version}
        - client version {__VERSION__}

        """))

        if self.data_source_name is None:
            self.complete_sources += [d['name'] for d in self.client.get_data_sources()]
        else:
            # need completer
            self.execute_special_command(f'\\c {self.data_source_name}')

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


def init():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-k',
        '--api-key',
        help='redash api key',
        default=None,
    )
    parser.add_argument(
        '-s',
        '--server-host',
        help='redash host i.e) https://your-redash-server/',
        default=None,
    )
    parser.add_argument(
        '-p',
        '--proxy',
        help=dedent("""
        if you need proxy connection, set them
        i.e) http://user:pass@proxy-host:proxy-port'
        """),
        default=None,
    )
    parser.add_argument(
        '-i',
        '--initial-data-source-name',
        help=dedent("""
        initial datasource name.
        if not set, no datasource selected.
        """),
        default=None,
    )
    args = parser.parse_args()
    return Args(
        api_key=args.api_key,
        host=args.server_host,
        proxy=args.proxy,
        initial_data_source_name=args.initial_data_source_name,
    )


def main():
    args = init()
    try:
        redaql = Redaql(**args.to_dict())
    except exceptions.RedaqlException as e:
        print(f'[ERROR] {e}\n')
        sys.exit(1)

    while True:
        try:
            redaql.loop()
        except exceptions.RedaqlException as e:
            print(e)
        except Exception as e:
            traceback.print_exc()
            sys.exit(1)


if __name__ in '__main__':
    main()


