FROM python:3.11-slim

# Prevent Python from buffering logs
ENV PYTHONUNBUFFERED=1

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

# Default entrypoint
CMD ["python", "-m", "src.main"]
