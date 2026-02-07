FROM mongo:8.0

# Install iptables, ping and tcpdump
RUN apt-get update && apt-get install -y iptables iputils-ping tcpdump && rm -rf /var/lib/apt/lists/*
