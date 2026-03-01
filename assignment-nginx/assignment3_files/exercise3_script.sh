
#!/bin/bash

metrics_fetch() {
    # More precise version - matches only the metric line
    curl -s "http://127.0.0.1:8081/metrics" | grep "^$1[[:space:]]*[0-9]"
    curl -s "http://127.0.0.1:8082/metrics" | grep "^$1[[:space:]]*[0-9]"
    curl -s "http://127.0.0.1:8083/metrics" | grep "^$1[[:space:]]*[0-9]"
}

metrics_reset() {
    # More precise version - matches only the metric line
    curl -s "http://127.0.0.1:8081/reset"
    curl -s "http://127.0.0.1:8082/reset"
    curl -s "http://127.0.0.1:8083/reset"
}

e3_link_block_nginx_to_backend1() {
    docker exec backend1 iptables -A OUTPUT -d nginx -j DROP
}

e3_link_restore() {
    docker exec backend1 iptables -F
}