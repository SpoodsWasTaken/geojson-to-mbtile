#!/bin/bash
# Startup script for Railway deployment
# Ensures proper port binding and adds tippecanoe to PATH

# Add tippecanoe to PATH
export PATH="/usr/local/bin:$PATH"

# Railway provides PORT, default to 8080 for local dev
PORT=${PORT:-8080}

echo "Starting application on port $PORT..."
echo "Tippecanoe version: $(tippecanoe --version 2>&1 | head -1)"

# Start gunicorn with proper configuration
exec gunicorn \
    --bind "0.0.0.0:$PORT" \
    --timeout 300 \
    --workers 2 \
    --worker-class sync \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    app:app

