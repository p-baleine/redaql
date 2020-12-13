import re

COMMENT_CHR = '#'


def is_special_command(text: str):
    return re.match(r'^ *\\', text) is not None


def is_end(text: str):
    cleaned_text = _remove_comment(text)
    return re.match('.*; *', cleaned_text) is not None


def _remove_comment(text):
    return text.split(COMMENT_CHR)[0]
