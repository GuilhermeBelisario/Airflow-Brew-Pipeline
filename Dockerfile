FROM python:latest
WORKDIR /app

RUN [pip install poetry]

COPY pyproject.toml poetry.lock* ./

RUN [poetry install]

COPY src/ .

CMD ["poetry", "run", "python", "main.py"]