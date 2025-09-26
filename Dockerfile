FROM gcr.io/google.com/cloudsdktool/cloud-sdk:latest

# Instalar dependencias Python
RUN pip install google-cloud-bigquery pandas

# Copiar scripts
COPY . /app
WORKDIR /app

# Configurar permisos
RUN chmod +x generate_silver_views.py

# Comando por defecto
CMD ["python", "generate_silver_views.py", "--force"]
