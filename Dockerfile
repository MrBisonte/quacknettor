FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (e.g. for DuckDB extensions if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . /app

# Install dependencies (from current directory)
RUN pip install --no-cache-dir .

# Expose Streamlit port
EXPOSE 8501

# Set entrypoint
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]
