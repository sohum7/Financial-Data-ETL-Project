FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy requirements from transform
COPY dividends/transform/requirements.txt ./requirements.txt

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy needed folders
COPY dividends/transform ./transform
COPY dividends/shared ./shared

ENV PYTHONPATH=/app

CMD ["python", "transform/main.py"]