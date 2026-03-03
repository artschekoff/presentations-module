import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--keep-s3",
        action="store_true",
        default=False,
        help="Do not delete S3 objects created during tests (for manual inspection).",
    )


@pytest.fixture
def keep_s3(request: pytest.FixtureRequest) -> bool:
    return bool(request.config.getoption("--keep-s3"))
