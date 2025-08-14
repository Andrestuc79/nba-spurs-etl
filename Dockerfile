FROM quay.io/astronomer/astro-runtime:13.1.0

# Instalar dependencias necesarias
USER root
RUN apt-get update && apt-get install -y build-essential

# Volver al usuario airflow
USER astro

# Instalar DBT para PostgreSQL (ajustá la versión si querés)
# Instalar DBT y solucionar conflicto con protobuf
RUN pip install --no-cache-dir dbt-postgres==1.7.9 && \
    pip install --no-cache-dir protobuf==4.25.3 grpcio==1.62.1 googleapis-common-protos==1.62.0

RUN mkdir -p /usr/local/airflow/.dbt


# Copiar profiles.yml al lugar correcto
COPY dbt/profiles.yml /usr/local/airflow/.dbt/profiles.yml

# Copiar el proyecto dbt completo
COPY dbt /usr/local/airflow/dbt


ENV AWS_DEFAULT_REGION us-east-1
ENV AWS_ACCESS_KEY_ID minioadmin
ENV AWS_SECRET_ACCESS_KEY minioadmin