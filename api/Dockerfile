FROM python:3.12-slim

WORKDIR /app

# Install Poetry
RUN pip install poetry==1.8.3

# Copy dependency files first (Docker layer caching)
# If only code changes, this layer is cached and deps aren't reinstalled
COPY pyproject.toml poetry.lock* ./

RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi --no-root

# Copy application code
COPY . .

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]