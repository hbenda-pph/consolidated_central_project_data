FROM python:3.9-slim

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instalar Google Cloud SDK
RUN curl https://sdk.cloud.google.com | bash
ENV PATH $PATH:/root/google-cloud-sdk/bin

# Instalar dependencias Python
RUN pip install --upgrade pip
RUN pip install google-cloud-bigquery pandas

# Crear directorio de trabajo
WORKDIR /app

# Copiar todos los archivos
COPY . .

# Configurar permisos
RUN chmod +x generate_silver_views.py

# Configurar autenticación (usar service account del job)
ENV GOOGLE_APPLICATION_CREDENTIALS=/secrets/service-account.json

# Comando por defecto
CMD ["python", "generate_silver_views.py", "--force"]
