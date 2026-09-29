"""The `when:` expression language: grammar, evaluation, and what it refuses."""
import pytest

from vsup.expr import And, Compare, ExprError, Not, Num, Or, Ref, evaluate, map_numbers, parse, refs

Q = {"p10": 1.0, "p25": 2.0, "p50": 3.0, "p75": 4.0, "p90": 5.0, "deterministic": 2.5}


@pytest.mark.parametrize(
    "text,expected",
    [
        ("p90 < 5", False),
        ("p90 <= 5", True),
        ("p10 > 0.5", True),
        ("p10 >= 1", True),
        ("5 > p50", True),
        ("p10 > -5", True),
        ("p10 > 1e-3", True),
        ("p10 > .5", True),
        ("p10 > 0 and p90 < 5", False),
        ("p10 > 0 and p90 <= 5", True),
        ("p10 > 9 or p90 > 4", True),
        ("not p10 > 9", True),
        ("not (p10 > 0 or p90 > 4)", False),
        ("2 < p50 <= 3", True),
        ("2 < p50 < 3", False),
        ("deterministic > p25", True),
        ("p10>0.5 and(p90<9)", True),
    ],
)
def test_evaluates(text, expected):
    assert evaluate(parse(text), Q) is expected


def test_and_binds_tighter_than_or():
    assert parse("p10 > 1 or p50 > 2 and p90 > 3") == Or((
        Compare(Ref("p10"), ">", Num(1.0)),
        And((Compare(Ref("p50"), ">", Num(2.0)), Compare(Ref("p90"), ">", Num(3.0)))),
    ))


def test_not_binds_tighter_than_and():
    assert parse("not p10 > 1 and p90 > 3") == And((
        Not(Compare(Ref("p10"), ">", Num(1.0))),
        Compare(Ref("p90"), ">", Num(3.0)),
    ))


def test_chained_comparison_is_a_conjunction():
    assert parse("1 < p50 < 2") == And((
        Compare(Num(1.0), "<", Ref("p50")),
        Compare(Ref("p50"), "<", Num(2.0)),
    ))


def test_refs():
    assert refs(parse("p10 > 1 and (p90 < 2 or not deterministic > 3)")) == {"p10", "p90", "deterministic"}


def test_map_numbers_leaves_names_alone():
    node = map_numbers(parse("p10 > 3.6 and p90 < 36"), lambda v: v / 3.6)
    assert node == parse("p10 > 1 and p90 < 10")


@pytest.mark.parametrize(
    "text,column,message",
    [
        ("", 1, "empty"),
        ("   ", 1, "empty"),
        ("p90", 4, "expected a comparison"),
        ("p90 < ", 7, "expected a quantile or a number"),
        ("p90 == 1", 5, "no == or !="),
        ("p90 != 1", 5, "no == or !="),
        ("p90 < 1 and", 12, "expected a quantile or a number"),
        ("(p90 < 1", 9, "expected ')'"),
        ("p90 < 1)", 8, "unexpected ')'"),
        ("p90 < 1 p10 > 2", 9, "unexpected 'p10'"),
        ("ninety < 1", 1, "unknown name 'ninety'"),
        ("p101 < 1", 1, "unknown name 'p101'"),
        ("1 < 2", 1, "compares numbers with numbers"),
        ("p90 + 1 < 2", 5, "unexpected '+'"),
        ("__import__('os').system('true') < 1", 12, "unexpected \"'\""),
    ],
)
def test_refuses(text, column, message):
    with pytest.raises(ExprError) as info:
        parse(text)
    assert message in info.value.message
    assert info.value.column == column, f"reported column {info.value.column}"


def test_is_never_evaluated_by_python():
    """Whatever the text, it goes through the parser or nowhere."""
    with pytest.raises(ExprError):
        parse("p90 < 1 or __import__('os')")
