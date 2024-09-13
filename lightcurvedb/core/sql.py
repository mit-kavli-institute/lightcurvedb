def psql_safe_str(string):
    """
    Replace NULL char in a string with \uFFFD. A copy of a string will
    be returned.

    Parameters
    ----------
    string: str
        The string to replace

    Returns
    -------
    str
        The psql safe string.
    """
    if isinstance(string, str):
        return string.replace("\x00", "\uFFFD")
    return string
