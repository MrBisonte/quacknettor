"""
The Streamlit screen, run with Streamlit's own test harness. No browser and no service.
"""

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

REPO = Path(__file__).resolve().parent.parent
SCREEN = REPO / "ui" / "main.py"

# Streamlit logs here each time a script makes a call Streamlit plans to remove.
# Its loggers do not propagate, so the handler goes on this logger itself.
NOTICE_LOGGER = "streamlit.deprecation_util"

# The names configs/pipelines.yml reads with __ENV: tokens. The screen loads that file.
CONFIG_ENV_NAMES = ("DUCKEL_PG_PASSWORD", "DUCKEL_SF_PASSWORD")


class _Notices(logging.Handler):
    """Keeps the text of every record it is handed."""

    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


@dataclass(frozen=True)
class ScreenRun:
    """The builder page after one run, and what Streamlit logged about calls it will remove."""

    page: AppTest
    notices: tuple[str, ...]


@pytest.fixture(scope="module")
def builder_page() -> Iterator[ScreenRun]:
    """Open the landing page, press "Start New Pipeline", and hand back the builder page."""
    notices = _Notices()
    logger = logging.getLogger(NOTICE_LOGGER)
    logger.addHandler(notices)
    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.chdir(REPO)
            for name in CONFIG_ENV_NAMES:
                patch.setenv(name, "placeholder")
            page = AppTest.from_file(str(SCREEN), default_timeout=60)
            page.run()
            start = next(b for b in page.button if "Start New Pipeline" in b.label)
            start.click().run()
            yield ScreenRun(page=page, notices=tuple(notices.messages))
    finally:
        logger.removeHandler(notices)


def test_the_builder_page_runs_without_an_error(builder_page):
    assert [e.value for e in builder_page.page.exception] == []
    assert [e.value for e in builder_page.page.error] == []


def test_the_screen_makes_no_call_streamlit_will_remove(builder_page):
    assert builder_page.notices == ()


def test_the_flow_diagram_is_on_the_builder_page(builder_page):
    frames = builder_page.page.get("iframe")
    assert len(frames) == 1
    diagram = frames[0].proto.srcdoc
    assert "mermaid" in diagram
    assert "DuckEL Engine" in diagram
