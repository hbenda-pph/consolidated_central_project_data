FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY consolidated_tables_job.py .

# Comando por defecto
CMD ["python", "consolidated_tables_job.py"]

