# GE Data Control Center image: api / worker / bootstrap (selected by command).
# Contains no Dagster -- the build fails if `dagster` is importable.
#
# Not yet included (arrives with the source-step adapters, M2): pandas,
# requests, pyodbc and the Microsoft ODBC driver for Evolution.
FROM python:3.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements/control-center.txt requirements/control-center.txt
RUN pip install -r requirements/control-center.txt

COPY pyproject.toml README.md ./
COPY src ./src
COPY sql ./sql
COPY scripts ./scripts
COPY pipelines ./pipelines
RUN pip install --no-deps -e . \
 && python -c "import importlib.util, sys; sys.exit('dagster must not be installed in the Control Center image' if importlib.util.find_spec('dagster') else 0)" \
 && python -c "import ge_control_center.cli, ge_control_center.api.app, ge_control_center.steps"

RUN useradd --create-home --uid 10001 gecc
USER gecc

CMD ["gecc", "--help"]
