FROM python:3.11-slim as builder

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Set up Python environment
WORKDIR /app
COPY pyproject.toml .
RUN uv sync

# Final stage
FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    postgresql \
    postgresql-contrib \
    default-mysql-server \
    && rm -rf /var/lib/apt/lists/*

# Copy Python environment from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Set up PostgreSQL
RUN service postgresql start && \
    su - postgres -c "createuser -s root" && \
    su - postgres -c "createdb lematerial" && \
    service postgresql stop

# Set up MySQL
RUN service mysql start && \
    mysql -e "CREATE DATABASE lematerial;" && \
    mysql -e "CREATE USER 'root'@'localhost' IDENTIFIED BY 'root';" && \
    mysql -e "GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION;" && \
    service mysql stop

# Copy application files
WORKDIR /app
COPY . .

# Create necessary directories
RUN mkdir -p /app/logs /root/.cache/lematerial_fetcher

# Copy startup script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Set environment variables
ENV LEMATERIALFETCHER_DB_PASSWORD=root \
    LEMATERIALFETCHER_MYSQL_PASSWORD=root \
    LEMATERIALFETCHER_DB_USER=root \
    LEMATERIALFETCHER_DB_NAME=lematerial \
    LEMATERIALFETCHER_MYSQL_USER=root \
    LEMATERIALFETCHER_MYSQL_DATABASE=lematerial

# Expose ports
EXPOSE 5432 3306

ENTRYPOINT ["docker-entrypoint.sh"] 
