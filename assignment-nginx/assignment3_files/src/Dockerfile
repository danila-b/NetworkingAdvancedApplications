FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y iptables iputils-ping tcpdump curl && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code
COPY . .

# Expose port 80 for the application
EXPOSE 80

# Command to run the application
CMD ["python", "backend.py"]