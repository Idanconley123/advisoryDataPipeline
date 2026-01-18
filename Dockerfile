FROM postgres:15-alpine

# Set environment variables
ENV POSTGRES_DB=advisory_db
ENV POSTGRES_USER=advisory_user
ENV POSTGRES_PASSWORD=advisory_pass

# Copy initialization scripts
COPY docker/init-db.sql /docker-entrypoint-initdb.d/10-init-schema.sql
COPY docker/load-data.sh /docker-entrypoint-initdb.d/20-load-data.sh
COPY advisory_not_applicable.csv /tmp/advisory_not_applicable.csv

# Make sure scripts are executable
RUN chmod +x /docker-entrypoint-initdb.d/*.sh

# Expose Postgres port
EXPOSE 5432

# The postgres image will automatically run scripts in /docker-entrypoint-initdb.d/
# in alphabetical order when the container starts for the first time
