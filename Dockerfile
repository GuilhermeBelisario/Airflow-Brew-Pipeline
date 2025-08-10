FROM python:latest
WORKDIR /app

RUN [pip install uv]

COPY pyproject.toml uv.lock* ./

RUN [uv sync --system --no-dev]

COPY src/ .