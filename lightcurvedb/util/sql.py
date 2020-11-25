import operator

# All following token set names are relative to:
# LHS TOKEN_SET_NAME RHS

EQUIVALENT = {"==", "=", "equal"}

NOT_EQUIVALENT = {"!=", "notequal", "not-equal"}

GREATER_THAN = {
    ">",
    "greater",
    "greaterthan",
    "greater-than",
}

GREATER_EQ_THAN = {
    ">=",
    "greatereq",
    "greater-eq",
}

LESS_THAN = {"<", "less", "lessthan", "less-than"}

LESS_EQ_THAN = {"<=", "lesseq", "less-eq"}


def get_operator_f(op_string):
    if op_string in EQUIVALENT:
        return operator.eq
    if op_string in NOT_EQUIVALENT:
        return operator.ne
    if op_string in GREATER_THAN:
        return operator.gt
    if op_string in GREATER_EQ_THAN:
        return operator.ge
    if op_string in LESS_THAN:
        return operator.lt
    if op_string in LESS_EQ_THAN:
        return operator.le
    raise ValueError("token {0} is not defined!".format(op_string))
