import re
from pathlib import Path

CONTEXTS = []


def REGISTER(regex):
    """Registers the given regex to the CONTEXT array"""
    global CONTEXTS
    CONTEXTS.append(re.compile(regex))


def extract_pdo_path_context(path):
    """
    Extracts registered regexes from the provided path.
    All named capture groups will update the return context
    dictionary in the order the regex was registered.
    """
    found_contexts = {}
    for regex in CONTEXTS:
        result = regex.search(str(path))
        if result:
            found_contexts.update(result.groupdict())
    return found_contexts


# Register basic pdo contexts
REGISTER(r"orbit-(?P<orbit_number>[0-9]+)")
REGISTER(r"sector-(?P<sector>[0-9]+)")
REGISTER(r"cam(?P<camera>[1-4])")
REGISTER(r"ccd(?P<ccd>[1-4])")
REGISTER(r"[^0-9]?(?P<tic_id>\d+)\.h5$")


def get_parent_dir(path):
    return Path(path).parts[-1]
