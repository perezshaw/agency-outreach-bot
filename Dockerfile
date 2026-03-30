FROM python:3.11-slim

WORKDIR /app

# Copy application files
COPY app.py .
COPY static/ static/

# Set default port for Render
ENV PORT=10000
EXPOSE 10000

# Run the server
CMD ["python", "app.py"]
