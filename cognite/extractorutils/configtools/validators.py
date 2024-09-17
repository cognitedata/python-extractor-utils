import logging
import re
from typing import List, Union

from cognite.extractorutils.configtools.elements import IgnorePattern

_logger = logging.getLogger(__name__)

def matches_patterns(patterns: list[Union[str, re.Pattern[str]]], string: str) -> bool:
    """
    Check string against list of RegExp patterns.

    Args:
        patterns: A list of (re) patterns to match string against.
        string: String to which we match the pattern.

    Returns:
        boolean value indicating whether string matches any of the patterns.
    """
    return any([matches_pattern(pattern, string) for pattern in patterns])


def matches_pattern(pattern: Union[str, re.Pattern[str]], string: str) -> bool:
    """
    Match pattern against a string.

    Args:
        pattern: (re) Pattern to match against a string.
        string: String to which we match the pattern.

    Returns:
        boolean value indicating a match or otherwise.
    """
    try:
        return re.search(pattern, string) is not None
    except re.error as e:
        _logger.warning(f"Could not apply RegExp: {pattern}\nReason: {e}")
        return False


def compile_patterns(ignore_patterns: List[Union[str, IgnorePattern]]) -> list[re.Pattern[str]]:
    """
    List of patterns to compile

    Args:
        ignore_patterns: A list of strings or IgnorePattern to be compiled.

    Returns:
        A list of compiled RegExp patterns.
    """
    compiled = []
    for p in ignore_patterns:
        if isinstance(p, IgnorePattern):
            compiled.append(re.compile(p.compile()))
        else:
            compiled.append(re.compile(p))
    return compiled
