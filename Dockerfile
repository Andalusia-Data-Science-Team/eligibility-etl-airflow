FROM apache/airflow:2.10.4-python3.10

# Switch to root to install system packages
USER root

# Install Microsoft ODBC Driver 17 for SQL Server
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        gnupg2 \
        unixodbc \
        unixodbc-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create a working directory for building the package and set ownership
RUN mkdir -p /tmp/build && chown -R airflow:root /tmp/build

# Copy project files to temp build directory with proper ownership
COPY --chown=airflow:root pyproject.toml setup.cfg setup.py /tmp/build/
COPY --chown=airflow:root src/ /tmp/build/src/

# Copy runtime files to airflow directory with proper ownership
COPY --chown=airflow:root sql/ /opt/airflow/sql/
COPY --chown=airflow:root passcode.json /opt/airflow/
COPY --chown=airflow:root .env /opt/airflow/

# Create DAGs directory (if not mounting via docker-compose)
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root src/ /opt/airflow/src/
# Switch to airflow user before pip operations
USER airflow

# Build and install the package as airflow user
WORKDIR /tmp/build
RUN pip install --upgrade pip setuptools wheel \
    && pip install .

# Clean up build directory as airflow user
RUN rm -rf /tmp/build

WORKDIR /opt/airflow

# Install additional Python packages that aren't in your package dependencies
RUN pip install --no-cache-dir \
    pymysql \
    apache-airflow-providers-openlineage>=1.8.0