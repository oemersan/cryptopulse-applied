FROM python:3.11-slim

WORKDIR /app

# System dependencies kept minimal for a smaller image.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# The actual source is mounted at runtime via docker-compose volumes,
# so changes during development don't require a rebuild.
ENV PYTHONPATH=/app

CMD ["tail", "-f", "/dev/null"]
