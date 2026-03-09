FROM python:3.12-slim

WORKDIR /app

# Copy all project files
COPY . /app

# Install dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

# Run the full ETL pipeline
ENTRYPOINT ["python", "dividends/run_etl.py"]