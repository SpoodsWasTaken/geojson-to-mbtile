# Dockerfile

# Stage 1: Build tippecanoe
# Use a build-environment with all the necessary compilers and libraries
FROM debian:bullseye-slim AS builder

# Install dependencies needed to build tippecanoe
RUN apt-get update && apt-get install -y \
    build-essential \
    libsqlite3-dev \
    zlib1g-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Clone and build tippecanoe from source
WORKDIR /usr/src
RUN git clone https://github.com/felt/tippecanoe.git
WORKDIR /usr/src/tippecanoe
RUN make -j && make install

# Stage 2: Create the final, lightweight application image
FROM python:3.10-slim-bullseye

# Set the working directory in the container
WORKDIR /app

# Copy the compiled tippecanoe binaries from the builder stage
COPY --from=builder /usr/local/bin/tippecanoe /usr/local/bin/
COPY --from=builder /usr/local/bin/tile-join /usr/local/bin/

# Install runtime dependencies for tippecanoe (sqlite and zlib) and the web app (unzip)
RUN apt-get update && apt-get install -y \
    libsqlite3-0 \
    zlib1g \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Copy application files
COPY requirements.txt .
COPY app.py .
COPY start.sh .
COPY templates/ ./templates/

# Make startup script executable
RUN chmod +x start.sh

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port the app runs on (Railway sets this dynamically)
EXPOSE 8080

# Define the command to run the application
# Use startup script to ensure proper configuration
CMD ["./start.sh"]

