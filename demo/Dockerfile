# Build context is the repository root, so the demo runs against the library in
# this working tree -- edit a module, restart, see the change.
FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[celery,fastapi]" "uvicorn[standard]"

COPY demo ./demo

ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
CMD ["uvicorn", "demo.app.api:api", "--host", "0.0.0.0", "--port", "8000"]
