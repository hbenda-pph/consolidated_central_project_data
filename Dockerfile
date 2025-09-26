FROM gcr.io/google.com/cloudsdktool/cloud-sdk:latest

# Instalar dependencias Python
RUN pip install --upgrade pip
RUN pip install google-cloud-bigquery pandas

# Crear directorio de trabajo
WORKDIR /app

# Copiar scripts
COPY *.py ./
COPY config.py ./

# Configurar permisos
RUN chmod +x generate_silver_views.py

# Comando por defecto
CMD ["python", "generate_silver_views.py", "--force"]
