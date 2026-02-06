FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY src /app/src
COPY config /app/config
COPY data/sample /app/data/sample

RUN pip install --upgrade pip && \
    pip install .

EXPOSE 8000

CMD ["uvicorn", "datalab.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
