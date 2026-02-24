# Custom Airflow image with project dependencies installed via uv
# Reference: https://docs.astral.sh/uv/guides/integration/docker/
FROM apache/airflow:2.9.1-python3.11

# Install uv into the image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Switch to root to install system deps if needed
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy dependency manifests first (layer cache optimisation)
COPY pyproject.toml uv.lock* ./

# Install all project dependencies directly into the system Python
# (no virtualenv inside the container — matches uv Docker best practice)
RUN uv pip install --system \
    polars \
    boto3 \
    "great-expectations>=0.18,<1.0" \
    sqlalchemy \
    psycopg2-binary \
    langchain \
    langchain-groq \
    faker \
    pydantic
