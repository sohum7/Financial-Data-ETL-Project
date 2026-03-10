FROM python:3.11-slim

# Create and move into the /app folder
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your code
COPY . .

# Set the path so Python finds your 'shared' folder
ENV PYTHONPATH="."

# Run the ETL script
CMD ["python", "-m", "dividends.run_etl"]