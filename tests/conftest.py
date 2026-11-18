"""
Shared pytest configuration for all test suites.

Puts the project on ``sys.path`` the same way the code is run in the repository
(README: ``PYTHONPATH=.``; Airflow DAG: ``sys.path.append(".../src/big_data_analytics")``):

Also hosts a small plugin that generates the tabular test-case documentation:
``pytest tests --test-doc PREFIX`` writes ``PREFIX.md``, ``PREFIX.tex`` (a booktabs
``longtable`` to ``\\input``) and ``PREFIX_summary.tex`` (counts per level and type)
from the test docstrings and the outcomes of that run. Expected docstring format::

    One-line objective.

    Steps:
    1. ...
    Expected:
    - ...
"""

import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PACKAGE = REPO_ROOT / "src" / "big_data_analytics"
TESTS_DIR = Path(__file__).resolve().parent

for path in (str(SRC_PACKAGE), str(REPO_ROOT)):
    if path in sys.path:
        sys.path.remove(path)
    sys.path.insert(0, path)


# --------------------------------------------------------------------------------
# Test-case documentation generator (--test-doc PREFIX)
# --------------------------------------------------------------------------------

DASH = "\u2014"
OUTCOMES = ("passed", "failed", "skipped", "xfailed", "xpassed", "not run")
MAX_DETAIL = 200
SECTION = re.compile(r"^(steps|expected(?: results?)?)\s*:\s*(.*)$", re.IGNORECASE)
ITEM = re.compile(r"^(?:\d+[.)]|[-*\u2022])\s+(.*)$")
COLUMNS = (
    # header, share of the table width
    ("ID", 0.05),
    ("Module", 0.10),
    ("Level", 0.08),
    ("Type", 0.08),
    ("Objective", 0.16),
    ("Steps", 0.21),
    ("Expected result", 0.20),
    ("Actual result", 0.12),
)
LATEX_CHARS = {
    "\\": r"\textbackslash{}",
    "&": r"\&",
    "%": r"\%",
    "$": r"\$",
    "#": r"\#",
    "_": r"\_\allowbreak{}",
    "{": r"\{",
    "}": r"\}",
    "~": r"\textasciitilde{}",
    "^": r"\textasciicircum{}",
    "<": r"\textless{}",
    ">": r"\textgreater{}",
    "|": r"\textbar{}",
    "\u2014": r"\textemdash{}",
    "\u2013": r"\textendash{}",
    "\u2212": r"$-$",
    "\u2264": r"$\leq$",
    "\u2265": r"$\geq$",
    "\u2260": r"$\neq$",
    "\u2248": r"$\approx$",
    "\u00b1": r"$\pm$",
    "\u00d7": r"$\times$",
    "\u00b7": r"$\cdot$",
    "\u2192": r"$\rightarrow$",
    "\u2026": r"\ldots{}",
    "\u00a0": "~",
}
LATEX_TABLE = str.maketrans(LATEX_CHARS)


@dataclass
class DocCase:
    """One documented test function with the outcomes of all its cases."""

    nodeid: str
    module: str
    level: str
    kind: str
    objective: str
    steps: list[str]
    expected: list[str]
    case_ids: list[str] = field(default_factory=list)
    doc_id: str = ""


def parse_docstring(doc: str | None) -> tuple[str, list[str], list[str]]:
    """Split a docstring into objective, steps and expected results."""
    objective: list[str] = []
    sections: dict[str, list[str]] = {"steps": [], "expected": []}
    current: list[str] | None = None
    in_objective = True
    for raw in (doc or "").strip().splitlines():
        line = raw.strip()
        header = SECTION.match(line)
        if header:
            current = sections[header.group(1).lower().split()[0]]
            in_objective = False
            line = header.group(2)
            if not line:
                continue
        if not line:
            in_objective = in_objective and not objective
            continue
        if current is None:
            if in_objective:
                objective.append(line)
            continue
        item = ITEM.match(line)
        if item or not current:
            current.append(item.group(1) if item else line)
        else:  # continuation of a wrapped line
            current[-1] = f"{current[-1]} {line}"
    return " ".join(objective), sections["steps"], sections["expected"]


def humanise(name: str) -> str:
    words = re.sub(r"^test_?", "", name).replace("_", " ").strip()
    return (words[:1].upper() + words[1:]) if words else name


def first_line(text: str) -> str:
    lines = [line.strip() for line in str(text).strip().splitlines() if line.strip()]
    line = lines[0] if lines else ""
    return line if len(line) <= MAX_DETAIL else line[: MAX_DETAIL - 3] + "..."


def failure_message(report: pytest.TestReport) -> str:
    crash = getattr(report.longrepr, "reprcrash", None)
    message = first_line(crash.message if crash else report.longreprtext)
    return message if report.when == "call" else f"{report.when}: {message}"


def classify(report: pytest.TestReport) -> tuple[str, str] | None:
    """Map one phase report to (outcome, detail); None when it adds nothing."""
    if hasattr(report, "wasxfail"):
        reason = re.sub(r"^reason: ", "", report.wasxfail)
        return ("xfailed" if report.skipped else "xpassed"), first_line(reason)
    if report.failed:
        return "failed", failure_message(report)
    if report.skipped:
        reason = report.longrepr[2] if isinstance(report.longrepr, tuple) else ""
        return "skipped", first_line(re.sub(r"^Skipped: ", "", str(reason)))
    if report.when == "call":
        return "passed", ""
    return None


def actual_result(results: list[tuple[str, str]]) -> str:
    """Aggregate the outcomes of all cases of one test function."""
    counts = Counter(outcome for outcome, _ in results)
    details = {}
    for outcome, detail in results:
        if detail:
            details.setdefault(outcome, detail)
    present = [outcome for outcome in OUTCOMES if counts[outcome]]
    if len(results) == 1:
        outcome = present[0]
        return f"{outcome}: {details[outcome]}" if outcome in details else outcome
    summary = ", ".join(f"{counts[outcome]} {outcome}" for outcome in present)
    summary = f"{summary} ({len(results)} cases)"
    notes = [
        f"{outcome}: {details[outcome]}" for outcome in present if outcome in details
    ]
    return "; ".join([summary, *notes])


def latex_escape(text: str) -> str:
    # "--" would otherwise be typeset as an en dash (e.g. in "--test-doc")
    return re.sub(r"-(?=-)", "-{}", text.translate(LATEX_TABLE))


def markdown_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("|", "\\|")


def numbered(items: list[str], ordered: bool) -> list[str]:
    if not items:
        return [DASH]
    if len(items) == 1 and not ordered:
        return items
    return [
        f"{i}. {item}" if ordered else f"- {item}" for i, item in enumerate(items, 1)
    ]


class DocPlugin:
    def __init__(self, prefix: str):
        self.prefix = Path(prefix)
        self.cases: dict[str, DocCase] = {}
        self.results: dict[str, tuple[str, str]] = {}
        self.collection_errors: list[str] = []

    # -- collection ----------------------------------------------------------------

    def pytest_collectreport(self, report):
        if report.failed:
            self.collection_errors.append(report.nodeid)

    def pytest_collection_finish(self, session):
        self.cases.clear()
        numbers: Counter[str] = Counter()
        for item in session.items:
            name = getattr(item, "originalname", None) or item.name
            key = f"{item.parent.nodeid}::{name}"
            case = self.cases.get(key)
            if case is None:
                try:
                    level = item.path.relative_to(TESTS_DIR).parts[0]
                except ValueError:
                    level = "other"
                level = level if (TESTS_DIR / level).is_dir() else "other"
                doc = getattr(getattr(item, "obj", None), "__doc__", None)
                objective, steps, expected = parse_docstring(doc)
                numbers[level] += 1
                case = self.cases[key] = DocCase(
                    nodeid=key,
                    module=re.sub(r"^test_", "", item.path.stem),
                    level=level,
                    kind="quality"
                    if item.get_closest_marker("quality")
                    else "functional",
                    objective=objective or humanise(name),
                    steps=steps,
                    expected=expected,
                    doc_id=f"{level[0].upper()}-{numbers[level]:02d}",
                )
            case.case_ids.append(item.nodeid)

    # -- outcomes ------------------------------------------------------------------

    def pytest_runtest_logreport(self, report):
        result = classify(report)
        if result is None:
            return
        previous = self.results.get(report.nodeid)
        # a failure in any phase wins; otherwise the first verdict stands
        if previous is None or (result[0] == "failed" and previous[0] != "failed"):
            self.results[report.nodeid] = result

    def case_results(self, case: DocCase) -> list[tuple[str, str]]:
        return [self.results.get(nodeid, ("not run", "")) for nodeid in case.case_ids]

    # -- output --------------------------------------------------------------------

    def pytest_sessionfinish(self, session):
        if hasattr(session.config, "workerinput") or not self.cases:
            return
        self.prefix.parent.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
        outputs = {
            self.prefix.with_name(self.prefix.name + ".md"): self.markdown(stamp),
            self.prefix.with_name(self.prefix.name + ".tex"): self.latex(stamp),
            self.prefix.with_name(
                self.prefix.name + "_summary.tex"
            ): self.latex_summary(stamp),
        }
        for path, text in outputs.items():
            path.write_text(text, encoding="utf-8")
        self.written = list(outputs)

    def pytest_terminal_summary(self, terminalreporter):
        for path in getattr(self, "written", []):
            terminalreporter.write_line(f"test documentation written to {path}")
        for nodeid in self.collection_errors:
            terminalreporter.write_line(
                f"test documentation is INCOMPLETE: {nodeid} could not be collected",
                red=True,
            )

    def rows(self):
        for case in self.cases.values():
            yield (
                [case.doc_id],
                [case.module],
                [case.level],
                [case.kind],
                [case.objective],
                numbered(case.steps, ordered=True),
                numbered(case.expected, ordered=False),
                [actual_result(self.case_results(case))],
            )

    def summary(self) -> list[tuple[str, str, int, Counter]]:
        """Per (level, type): number of test functions and case outcomes."""
        groups: dict[tuple[str, str], tuple[int, Counter]] = {}
        for case in self.cases.values():
            tests, counts = groups.get((case.level, case.kind), (0, Counter()))
            counts.update(outcome for outcome, _ in self.case_results(case))
            groups[(case.level, case.kind)] = (tests + 1, counts)
        return [(level, kind, *value) for (level, kind), value in groups.items()]

    def summary_rows(self) -> tuple[list[str], list[list[str]]]:
        summary = self.summary()
        used = [o for o in OUTCOMES if any(counts[o] for *_, counts in summary)]
        header = ["Level", "Type", "Tests", "Cases", *[o.capitalize() for o in used]]
        body = [
            [level, kind, tests, sum(counts.values()), *[counts[o] for o in used]]
            for level, kind, tests, counts in summary
        ]
        if body:
            totals = [sum(row[i] for row in body) for i in range(2, len(header))]
            body.append(["Total", "", *totals])
        return header, [[str(cell) for cell in row] for row in body]

    def markdown(self, stamp: str) -> str:
        headers = [name for name, _ in COLUMNS]
        lines = [
            "# Test cases",
            "",
            f"Generated by `pytest --test-doc` on {stamp}. Do not edit by hand.",
            "",
        ]
        lines += [
            f"**Not collected (missing from the table):** `{nodeid}`  "
            for nodeid in self.collection_errors
        ]
        lines += [
            "| " + " | ".join(headers) + " |",
            "|" + "---|" * len(headers),
        ]
        for row in self.rows():
            cells = [
                "<br>".join(markdown_escape(part) for part in cell) for cell in row
            ]
            lines.append("| " + " | ".join(cells) + " |")
        header, body = self.summary_rows()
        lines += ["", "## Summary", "", "| " + " | ".join(header) + " |"]
        lines.append("|" + "---|" * len(header))
        lines += ["| " + " | ".join(row) + " |" for row in body]
        lines += [
            "",
            "## Traceability",
            "",
            "| ID | pytest node | Cases |",
            "|---|---|---|",
        ]
        lines += [
            f"| {case.doc_id} | `{case.nodeid}` | {len(case.case_ids)} |"
            for case in self.cases.values()
        ]
        return "\n".join(lines) + "\n"

    def latex(self, stamp: str) -> str:
        spec = "".join(
            rf">{{\raggedright\arraybackslash\hspace{{0pt}}}}p{{{share:.2f}\testdocwidth}}"
            for _, share in COLUMNS
        )
        header = " & ".join(rf"\textbf{{{name}}}" for name, _ in COLUMNS) + r" \\"
        gaps = 2 * len(COLUMNS)
        more = (
            rf"\multicolumn{{{gaps // 2}}}{{r}}{{\emph{{continued on next page}}}} \\"
        )
        lines = [
            f"% Generated by pytest --test-doc on {stamp}. Do not edit by hand.",
            r"% Requires: booktabs, longtable, array.",
            *[f"% NOT COLLECTED (missing below): {n}" for n in self.collection_errors],
            r"\begingroup",
            r"\footnotesize",
            r"\setlength{\tabcolsep}{4pt}",
            r"\ifdefined\testdocwidth\else\newlength{\testdocwidth}\fi",
            rf"\setlength{{\testdocwidth}}{{\dimexpr\linewidth-{gaps}\tabcolsep}}",
            rf"\begin{{longtable}}{{{spec}}}",
            r"\caption{Test cases}\label{tab:test-cases}\\",
            r"\toprule",
            header,
            r"\midrule",
            r"\endfirsthead",
            r"\toprule",
            header,
            r"\midrule",
            r"\endhead",
            r"\midrule",
            more,
            r"\endfoot",
            r"\bottomrule",
            r"\endlastfoot",
        ]
        for row in self.rows():
            cells = [
                r" \newline ".join(latex_escape(part) for part in cell) for cell in row
            ]
            lines.append(" & ".join(cells) + r" \\")
            lines.append(r"\addlinespace")
        lines += [r"\end{longtable}", r"\endgroup"]
        return "\n".join(lines) + "\n"

    def latex_summary(self, stamp: str) -> str:
        header, body = self.summary_rows()
        lines = [
            f"% Generated by pytest --test-doc on {stamp}. Do not edit by hand.",
            rf"\begin{{tabular}}{{ll{'r' * (len(header) - 2)}}}",
            r"\toprule",
            " & ".join(rf"\textbf{{{name}}}" for name in header) + r" \\",
            r"\midrule",
        ]
        for index, row in enumerate(body):
            if index == len(body) - 1:
                lines.append(r"\midrule")
            lines.append(" & ".join(latex_escape(cell) for cell in row) + r" \\")
        lines += [r"\bottomrule", r"\end{tabular}"]
        return "\n".join(lines) + "\n"


def pytest_addoption(parser):
    parser.addoption(
        "--test-doc",
        metavar="PREFIX",
        default=None,
        help="write the tabular test-case documentation to PREFIX.md, PREFIX.tex "
        "and PREFIX_summary.tex",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "quality: data-quality test (completeness, consistency, distribution, "
        "conservation, reproducibility); other tests are functional",
    )
    prefix = config.getoption("--test-doc")
    if prefix:
        config.pluginmanager.register(DocPlugin(prefix), "test-doc")
