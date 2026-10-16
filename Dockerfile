# One image for every hosted process: the consoles, Dagster's UI and the daily refresh.
FROM python:3.14-slim

# Logs straight to the container's output; no .pyc files in the image.
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    MERX_DATA_DIR=/data DAGSTER_HOME=/data/dagster_home

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY contracts ./contracts
COPY reference ./reference
# Editable, so the contracts and reference files next to the code are the ones read
# (merx.config resolves them relative to the source tree).
RUN pip install --no-cache-dir -e ".[server]"

RUN useradd --create-home --uid 1000 merx
USER merx

CMD ["merx", "serve", "--host", "0.0.0.0", "--port", "8000"]
