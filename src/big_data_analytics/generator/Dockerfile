FROM python:3.9-slim

RUN pip install --no-cache-dir uv

WORKDIR /app

COPY requirements.txt .

RUN uv pip install --system --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]