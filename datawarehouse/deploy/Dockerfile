FROM apache/airflow:2.9.3-python3.12

USER root
# Certificado de AWS para TLS con DocumentDB (obligatorio por defecto en DocumentDB)
RUN curl -fsSL https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem -o /opt/airflow/global-bundle.pem \
    && chmod 644 /opt/airflow/global-bundle.pem
USER airflow

RUN pip install --no-cache-dir "Faker==37.0.0" "PyMySQL==1.1.2" "pymongo==4.16.0"

# dbt en un entorno virtual aparte (evita conflictos con Airflow); adaptador Redshift en lugar de DuckDB
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core~=1.9.4" "dbt-redshift~=1.9.0"
