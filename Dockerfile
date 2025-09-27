# =============================================================================
# DOCKERFILE PARA GENERATE SILVER VIEWS JOB
# =============================================================================

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY *.py ./

# Configurar permisos
RUN chmod +x generate_silver_views.py generate_silver_views_job.py

# Comando por defecto
CMD ["python", "generate_silver_views_job.py"]
