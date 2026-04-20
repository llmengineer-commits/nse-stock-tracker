FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY nse_producer.py .

# The producer loops forever — no CMD exit expected
CMD ["python", "-u", "nse_producer.py"]
