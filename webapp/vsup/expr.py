"""The expression language of `when:` in rules-mode schemes.

Parsed here and never handed to eval(). The whole language:

    p90 < 0.1                      quantile names p0 ... p100, and `deterministic`
    p10 > 1 and p90 < 2            and / or / not, and binds tighter than or
    not (p25 > 10 or p75 < 3)      parentheses
    1 < p50 <= 2                   chained comparisons, as in Python
    p10 >= -5                      numbers, including negative and decimal

Comparisons are < <= > >=. There is no == on purpose: forecast values are floats.
"""
import re
from dataclasses import dataclass

from core.model import quantile_level

DETERMINISTIC = "deterministic"
_KEYWORDS = {"and", "or", "not"}


class ExprError(ValueError):
    def __init__(self, message, column):
        super().__init__(message)
        self.message = message
        self.column = column  # 1-based position in the expression

    def __str__(self):
        return f"column {self.column}: {self.message}"


@dataclass(frozen=True)
class Num:
    value: float


@dataclass(frozen=True)
class Ref:
    name: str


@dataclass(frozen=True)
class Compare:
    left: object
    op: str
    right: object


@dataclass(frozen=True)
class And:
    terms: tuple


@dataclass(frozen=True)
class Or:
    terms: tuple


@dataclass(frozen=True)
class Not:
    term: object


_TOKEN = re.compile(
    r"(?P<num>-?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?)"
    r"|(?P<op><=|>=|<|>)"
    r"|(?P<paren>[()])"
    r"|(?P<word>[A-Za-z_][A-Za-z_0-9]*)"
    r"|(?P<space>\s+)"
)


def _tokenize(text):
    tokens, pos = [], 0
    while pos < len(text):
        match = _TOKEN.match(text, pos)
        if match is None:
            bad = text[pos : pos + 2]
            hint = " (use < <= > >=; there is no == or !=)" if bad[:1] in "=!" else ""
            raise ExprError(f"unexpected {bad[:1]!r}{hint}", pos + 1)
        if match.lastgroup != "space":
            tokens.append((match.lastgroup, match.group(), pos + 1))
        pos = match.end()
    tokens.append(("end", "", len(text) + 1))
    return tokens


class _Parser:
    def __init__(self, text):
        self.tokens = _tokenize(text)
        self.i = 0

    def peek(self):
        return self.tokens[self.i]

    def take(self):
        token = self.tokens[self.i]
        self.i += 1
        return token

    def expect(self, kind, value):
        token = self.take()
        if token[:2] != (kind, value):
            raise ExprError(f"expected {value!r}, found {_describe(token)}", token[2])
        return token

    def parse(self):
        node = self.disjunction()
        token = self.peek()
        if token[0] != "end":
            raise ExprError(f"unexpected {_describe(token)}", token[2])
        return node

    def disjunction(self):
        terms = [self.conjunction()]
        while self.peek()[:2] == ("word", "or"):
            self.take()
            terms.append(self.conjunction())
        return terms[0] if len(terms) == 1 else Or(tuple(terms))

    def conjunction(self):
        terms = [self.negation()]
        while self.peek()[:2] == ("word", "and"):
            self.take()
            terms.append(self.negation())
        return terms[0] if len(terms) == 1 else And(tuple(terms))

    def negation(self):
        if self.peek()[:2] == ("word", "not"):
            self.take()
            return Not(self.negation())
        if self.peek()[:2] == ("paren", "("):
            self.take()
            node = self.disjunction()
            self.expect("paren", ")")
            return node
        return self.comparison()

    def comparison(self):
        start = self.peek()[2]
        operands = [self.operand()]
        ops = []
        while self.peek()[0] == "op":
            ops.append(self.take()[1])
            operands.append(self.operand())
        if not ops:
            token = self.peek()
            raise ExprError(f"expected a comparison (< <= > >=), found {_describe(token)}", token[2])
        if not any(isinstance(o, Ref) for o in operands):
            raise ExprError("compares numbers with numbers; name a quantile such as p90", start)
        pairs = [Compare(a, op, b) for a, op, b in zip(operands, ops, operands[1:])]
        return pairs[0] if len(pairs) == 1 else And(tuple(pairs))

    def operand(self):
        kind, value, column = self.take()
        if kind == "num":
            return Num(float(value))
        if kind == "word" and value not in _KEYWORDS:
            if value != DETERMINISTIC:
                try:
                    quantile_level(value)
                except ValueError:
                    raise ExprError(
                        f"unknown name {value!r}; expected a quantile such as p10 or p90, "
                        f"or {DETERMINISTIC!r}",
                        column,
                    ) from None
            return Ref(value)
        raise ExprError(f"expected a quantile or a number, found {_describe((kind, value, column))}", column)


def _describe(token):
    kind, value, _ = token
    return "the end of the expression" if kind == "end" else repr(value)


def parse(text):
    """Parse `text` into a tree of Num/Ref/Compare/And/Or/Not, or raise ExprError."""
    if not isinstance(text, str) or not text.strip():
        raise ExprError("the expression is empty", 1)
    return _Parser(text).parse()


_OPS = {
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
}


def evaluate(node, values):
    """Evaluate against {"p10": 1.2, ..., "deterministic": 0.4}."""
    if isinstance(node, Compare):
        return _OPS[node.op](_value(node.left, values), _value(node.right, values))
    if isinstance(node, And):
        return all(evaluate(term, values) for term in node.terms)
    if isinstance(node, Or):
        return any(evaluate(term, values) for term in node.terms)
    if isinstance(node, Not):
        return not evaluate(node.term, values)
    raise TypeError(f"not an expression node: {node!r}")


def _value(operand, values):
    return operand.value if isinstance(operand, Num) else values[operand.name]


def refs(node):
    """Every name the expression reads."""
    if isinstance(node, Ref):
        return {node.name}
    if isinstance(node, Num):
        return set()
    if isinstance(node, Compare):
        return refs(node.left) | refs(node.right)
    if isinstance(node, (And, Or)):
        return set().union(*(refs(term) for term in node.terms))
    return refs(node.term)


def map_numbers(node, fn):
    """A copy with every number replaced by fn(number). Used to convert the
    thresholds of a scheme written in another unit, once, at load time."""
    if isinstance(node, Num):
        return Num(fn(node.value))
    if isinstance(node, Ref):
        return node
    if isinstance(node, Compare):
        return Compare(map_numbers(node.left, fn), node.op, map_numbers(node.right, fn))
    if isinstance(node, (And, Or)):
        return type(node)(tuple(map_numbers(term, fn) for term in node.terms))
    return Not(map_numbers(node.term, fn))
