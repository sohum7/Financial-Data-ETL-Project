FROM python:3.10-slim

# Prevent python from writing pyc files and buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Create working directory
WORKDIR /app

# Install system deps only if needed (kept separate for caching)
#RUN apt-get update \
#    && apt-get install -y --no-install-recommends build-essential \
#    && rm -rf /var/lib/apt/lists/*

# Copy dependency file first (so Docker caches pip install)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application code (this invalidates cache only when code changes)
COPY . .

# Ensure Python can see shared modules
ENV PYTHONPATH=/app

# Run ETL
CMD ["python", "-m", "dividends.run_etl"]