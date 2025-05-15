FROM python:3.11-slim AS builder

# Download uv
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates build-essential
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh
ENV PATH="/root/.local/bin/:$PATH"

# Install the project into `/app`
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# Then, add the rest of the project source code and install it
# Installing separately from its dependencies allows optimal layer caching
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

ENV PATH="/app/.venv/bin:$PATH"

RUN apt-get update && apt-get install -y \
    postgresql \
    postgresql-contrib \
    default-mysql-server \
    && rm -rf /var/lib/apt/lists/*

# Final stage
FROM python:3.11-slim

ENV PATH="/app/.venv/bin:$PATH"

# Copy Python environment and app from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /app /app

RUN apt-get update && apt-get install -y \
    postgresql \
    postgresql-contrib \
    default-mysql-server \
    && rm -rf /var/lib/apt/lists/*

# Set up PostgreSQL
RUN service postgresql start && \
    su - postgres -c "createuser -s root" && \
    su - postgres -c "createdb lematerial" && \
    su - postgres -c "psql -c \"CREATE USER lematerial WITH PASSWORD 'lematerial';\"" && \
    su - postgres -c "psql -c \"ALTER USER lematerial WITH SUPERUSER;\"" && \
    su - postgres -c "psql -c \"GRANT ALL PRIVILEGES ON DATABASE lematerial TO lematerial;\"" && \
    # Update pg_hba.conf to use md5 authentication for the lematerial user
    pg_hba_path=$(find /etc/postgresql -name "pg_hba.conf") && \
    echo "host    all             lematerial      127.0.0.1/32            md5" >> "$pg_hba_path" && \
    echo "host    all             lematerial      ::1/128                 md5" >> "$pg_hba_path" && \
    service postgresql stop

# Create necessary directories and set permissions
RUN mkdir -p /var/lib/mysql /var/run/mysqld /docker-entrypoint-initdb.d && \
    chown -R mysql:mysql /var/lib/mysql /var/run/mysqld

# Initialize MariaDB
RUN mysql_install_db --user=mysql --datadir=/var/lib/mysql && \
    service mariadb start && \
    mariadb -u root -e "CREATE DATABASE IF NOT EXISTS lematerial;" && \
    mariadb -u root -e "CREATE USER 'lematerial'@'localhost' IDENTIFIED BY 'lematerial';" && \
    mariadb -u root -e "GRANT ALL PRIVILEGES ON lematerial.* TO 'lematerial'@'localhost';" && \
    mariadb -u root -e "FLUSH PRIVILEGES;" && \
    service mariadb stop

# Create necessary directories
RUN mkdir -p /app/logs /root/.cache/lematerial_fetcher

# Copy startup script
COPY lemat-traj.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/lemat-traj.sh

# Set environment variables
ENV LEMATERIALFETCHER_DB_PASSWORD=lematerial \
    LEMATERIALFETCHER_MYSQL_PASSWORD=lematerial \
    LEMATERIALFETCHER_DB_USER=lematerial \
    LEMATERIALFETCHER_DB_NAME=lematerial \
    LEMATERIALFETCHER_MYSQL_USER=lematerial \
    LEMATERIALFETCHER_MYSQL_DATABASE=lematerial \
    LEMATERIALFETCHER_TRANSFORMER_SOURCE_DB_USER=lematerial \
    LEMATERIALFETCHER_TRANSFORMER_SOURCE_DB_PASSWORD=lematerial \
    LEMATERIALFETCHER_TRANSFORMER_SOURCE_DB_NAME=lematerial \
    LEMATERIALFETCHER_TRANSFORMER_DEST_DB_USER=lematerial \
    LEMATERIALFETCHER_TRANSFORMER_DEST_DB_PASSWORD=lematerial \
    LEMATERIALFETCHER_TRANSFORMER_DEST_DB_NAME=lematerial

# Expose ports
EXPOSE 5432 3306

ENTRYPOINT []

CMD ["lemat-traj.sh"]
