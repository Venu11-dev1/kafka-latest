# Use slim Python 3.10 base image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Install system dependencies (optional: good for psycopg2, kafka libs, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first (better caching for builds)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Now copy the rest of the application
COPY . .

# Make sure topics/, consumer/, producer/ are treated as Python packages
# (They must contain __init__.py files)

# Run the app
CMD ["python", "main.py"]
