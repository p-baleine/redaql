import re

COMMENT_CHR = '#'


def is_special_command(text: str):
    return re.match(r'^ *\\', text) is not None


def is_end(text: str):
    cleaned_text = _remove_comment(text)
    cleaned_text = _remove_empty_lines(cleaned_text)
    return re.match('.*; *', cleaned_text.split('\n')[-1]) is not None


def _remove_comment(text: str):
    return text.split(COMMENT_CHR)[0]


def _remove_empty_lines(text: str):
    lines = text.splitlines()
    target = 0
    for i, line in enumerate(lines[::-1]):
        # found not empty line
        if not re.match('^[ \t\s]*$', line):
            target = i
            break
    return '\n'.join(lines[:-target])
