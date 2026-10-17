"""Every helper a page calls must be defined by a script that page loads.

This exists because of a real failure: `explain.js` was overwritten with a copy of
`chart.js`. Both files were valid JavaScript, both were served with HTTP 200, and every
API behind the page worked — so nothing in the test suite or in a manual check of the
endpoints noticed. The page itself died on the first call to a function that no longer
existed anywhere.

Checking that files parse is not enough; checking that the right *names* are defined is
what catches a file being replaced by the wrong contents.
"""

import re
from pathlib import Path

import pytest

STATIC = Path(__file__).resolve().parents[1] / "src" / "merx" / "api" / "static"
PAGES = sorted(STATIC.glob("*.html"))

# Names that come from the browser or the language, not from our own scripts.
BUILTIN = {
    "if",
    "for",
    "while",
    "switch",
    "catch",
    "return",
    "typeof",
    "function",
    "new",
    "await",
    "do",
    "else",
    "delete",
    "void",
    "in",
    "of",
    "instanceof",
    "Number",
    "String",
    "Boolean",
    "Array",
    "Object",
    "Date",
    "Math",
    "JSON",
    "Set",
    "Map",
    "Error",
    "Promise",
    "RegExp",
    "Intl",
    "parseInt",
    "parseFloat",
    "isNaN",
    "fetch",
    "alert",
    "confirm",
    "setTimeout",
    "setInterval",
    "clearTimeout",
    "encodeURIComponent",
    "decodeURIComponent",
    "URLSearchParams",
    "URL",
    "getComputedStyle",
    "requestAnimationFrame",
    "structuredClone",
}

_CALL = re.compile(r"(?<![.\w$])([A-Za-z_$][\w$]*)\s*\(")
_DEFINED = re.compile(
    r"(?:function\s+([A-Za-z_$][\w$]*)"
    r"|(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=\s*(?:async\s*)?(?:function|\())"
)


def strip_literals(source: str) -> str:
    """Return only the executable parts of some JavaScript.

    Prose in a string looks like code to a regular expression: "and more (200 shown)"
    reads as a call to ``more``. Comments and quoted strings are dropped outright.
    Template literals need a scanner rather than a pattern, because they nest through
    ``${...}`` — and what is inside ``${...}`` *is* code, so it is kept while the text
    around it is dropped.
    """
    out: list[str] = []
    stack: list[str] = []  # "text" inside a template, "code" inside a ${...}
    i, n = 0, len(source)
    while i < n:
        char, pair = source[i], source[i : i + 2]
        in_text = bool(stack) and stack[-1] == "text"

        if in_text:
            if char == "\\":
                i += 2
            elif char == "`":
                stack.pop()
                out.append(" ")
                i += 1
            elif pair == "${":
                stack.append("code")
                out.append(" ")
                i += 2
            else:
                i += 1  # template text: not code
            continue

        if pair == "//":
            i = source.find("\n", i) % (n + 1) or n
        elif pair == "/*":
            i = source.find("*/", i) + 2 or n
        elif char in "'\"":
            i += 1
            while i < n and source[i] != char:
                i += 2 if source[i] == "\\" else 1
            i += 1
            out.append(" ")
        elif char == "`":
            stack.append("text")
            i += 1
        elif char == "}" and stack and stack[-1] == "code":
            stack.pop()
            out.append(" ")
            i += 1
        else:
            out.append(char)
            i += 1
    return "".join(out)


def _defined_names(source: str) -> set[str]:
    return {name for match in _DEFINED.finditer(source) for name in match.groups() if name}


def _page_parts(page: Path) -> tuple[str, list[Path]]:
    html = page.read_text(encoding="utf-8")
    includes = [STATIC / src for src in re.findall(r'<script src="/static/([^"]+)"', html)]
    inline = "\n".join(re.findall(r"<script>(.*?)</script>", html, re.S))
    return inline, includes


@pytest.mark.parametrize("page", PAGES, ids=lambda p: p.name)
def test_every_function_a_page_calls_is_defined_somewhere_it_loads(page: Path):
    inline, includes = _page_parts(page)
    for script in includes:
        assert script.exists(), f"{page.name} loads {script.name}, which is missing"

    available = _defined_names(inline)
    for script in includes:
        available |= _defined_names(script.read_text(encoding="utf-8"))

    called = {n for n in _CALL.findall(strip_literals(inline)) if n not in BUILTIN}
    missing = sorted(called - available)
    assert not missing, (
        f"{page.name} calls {missing}, which no script it loads defines. "
        f"It loads: {[s.name for s in includes]}"
    )


def test_literal_stripping_handles_nested_templates():
    """The stripper is the part most likely to go quietly wrong, so it is tested too."""
    source = "const h = `a ${xs.map((r) => `and more (${r.n} shown)`)} b`; real(1);"
    stripped = strip_literals(source)
    assert "more" not in stripped
    assert "real" in stripped


def test_no_two_scripts_are_copies_of_each_other():
    """The failure that prompted these tests: one static file overwritten by another."""
    contents = {f.name: f.read_text(encoding="utf-8") for f in sorted(STATIC.glob("*.js"))}
    seen: dict[str, str] = {}
    for name, body in contents.items():
        stripped = "".join(body.split())
        assert stripped not in seen, f"{name} is a copy of {seen[stripped]}"
        seen[stripped] = name


def test_the_explainer_defines_what_the_client_page_needs():
    """Named explicitly, so the guarantee survives a refactor of the generic check."""
    defined = _defined_names((STATIC / "explain.js").read_text(encoding="utf-8"))
    assert "renderConcepts" in defined


def test_no_page_still_asks_for_the_markers_that_were_removed():
    """The per-term "?" markers were dropped: they interrupted the text they explained.

    The glossary they opened is still rendered, as one readable list. A page left
    marking terms would be asking for an affordance that no longer exists, and a marker
    rule left in the stylesheet would be dead weight.
    """
    for page in PAGES:
        body = page.read_text(encoding="utf-8")
        assert "data-explain" not in body, f"{page.name} still marks terms"
        assert "explainAgain" not in body, f"{page.name} still re-attaches markers"
    style = (STATIC / "style.css").read_text(encoding="utf-8")
    assert "explain-mark" not in style and "explain-pop" not in style


def test_the_client_page_holds_its_shape_before_the_data_arrives():
    """The placeholders are in the markup, not drawn by a script.

    A skeleton rendered by JavaScript appears only once the page's own code has run,
    which is most of the wait it is meant to cover. These are in the HTML, so they are
    there at first paint.
    """
    body = (STATIC / "client.html").read_text(encoding="utf-8")
    style = (STATIC / "style.css").read_text(encoding="utf-8")
    assert "data-loading" in body and 'aria-busy="true"' in body
    assert body.count("sk-card") >= 4, "the catalogue should show several card shapes"
    assert "sk-chart" in body, "the chart is the tallest empty space on the page"
    # Every placeholder class the markup uses must be styled, or it renders as nothing.
    for name in ("sk", "sk-line", "sk-card", "sk-chart"):
        assert f".{name} " in style or f".{name} {{" in style or f".{name}," in style

    # Taking the shapes away is the page's job once it has content.
    assert 'removeAttribute("data-loading")' in body
    assert 'setAttribute("aria-busy", "false")' in body


def test_a_page_that_cannot_load_says_so_in_its_own_voice():
    """The failure path used to dump a stack trace into the page."""
    body = (STATIC / "client.html").read_text(encoding="utf-8")
    assert "<pre>" not in body
    assert "could not load its data" in body
    assert "location.reload()" in body, "a failure should offer a way out of itself"
