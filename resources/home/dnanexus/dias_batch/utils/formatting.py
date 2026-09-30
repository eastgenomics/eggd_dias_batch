"""
Generic formatting/string helpers used across other utils modules
"""
from datetime import datetime
import json
import re


def time_stamp() -> str:
    """
    Returns string of date & time formatted as YYMMDD_HHMM

    Returns
    -------
    str
        String of current date and time as YYMMDD_HHMM
    """
    return datetime.now().strftime("%y%m%d_%H%M")


def prettier_print(thing) -> None:
    """
    Pretty print for nicer viewing in the logs since pprint does not
    do an amazing job visualising big dicts and long strings

    Bonus: we're indenting using the Braille Pattern Blank U+2800
    unicode character since the new DNAnexus UI (as of Dec. 2023)
    strips leading tabs and spaces in the logs, which makes viewing
    the pretty dicts terrible. Luckily they don't strip other
    whitespace characters, so we can get around them yet again making
    their UI worse.

    Parameters
    ----------
    thing : anything json dumpable
        thing to print
    """
    print(json.dumps(thing, indent='⠀⠀'))


def make_path(*path) -> str:
    """
    Generate path-like string (i.e. for searching in DNAnexus) or
    when setting output folder for running an app

    Parameters
    ----------
    path : iterable
        strings of path parts to join

    Returns
    -------
    str
        nicely formatted path with leading and trailing forward slash
    """
    path = '/'.join([
        re.sub(r"project-[\d\w]+:", "", x).lstrip('/').rstrip('/')
        for x in path if x
    ])

    return f"/{path}/"
