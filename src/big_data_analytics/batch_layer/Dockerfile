FROM python:3.13-slim-trixie
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY . /app

WORKDIR /app
RUN uv sync --locked  
CMD ["uv", "run", "./src/big_data_analytics/batch_layer/run_minio.py"]