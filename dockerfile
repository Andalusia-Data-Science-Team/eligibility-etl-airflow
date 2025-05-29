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

# Switch back to airflow user
USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    pymysql \
    apache-airflow-providers-openlineage>=1.8.0 \
    pyodbc

# Copy project files and install
COPY pyproject.toml setup.cfg setup.py /opt/airflow/
COPY src/ /opt/airflow/src/
COPY sql/ /opt/airflow/sql/
COPY passcode.json /opt/airflow/
WORKDIR /opt/airflow

RUN pip install --upgrade pip setuptools \
    && pip install -e .