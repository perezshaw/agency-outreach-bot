FROM python:3.11-slim

WORKDIR /app

# Copy application files
COPY app.py .
COPY static/ static/

# Expose port (Cloud Run sets PORT env var)
EXPOSE 8080

# Run the server
CMD ["python", "app.py"]
