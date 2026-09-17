FROM python:3.11-slim

# Prevent Python from buffering logs
ENV PYTHONUNBUFFERED=1

# .git isn't copied into the image, so src/strategy/registry.py's git_provenance()
# can't shell out to git at runtime -- bake the commit in at build time instead
# (docker-compose.yml passes this from $GIT_COMMIT) as its documented fallback.
ARG GIT_COMMIT=unknown
ENV GIT_COMMIT=${GIT_COMMIT}

WORKDIR /app

# System deps (DuckDB does not need much)
RUN apt-get update && apt-get install -y \
    build-essential  \
       libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code only
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY config/ ./config/

# Default entrypoint
CMD ["python", "-m", "src.main"]
