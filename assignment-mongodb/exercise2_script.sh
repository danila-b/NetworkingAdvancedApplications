validate_mongo_node() {
    local node=$1
    case $node in
        mongo1)
            echo "$node" "27017"
            ;;
        mongo2)
            echo "$node" "27018"
            ;;
        mongo3)
            echo "$node" "27019"
            ;;
        *)
            echo "mongo1" "27017"
            ;;
    esac
}



e2_rs_initiate_default() {
    docker exec mongo1 mongosh --port 27017 --eval '
    rs.initiate({
    _id: "rs0",
    members: [
        {_id: 0, host: "mongo1:27017", priority: 1},
        {_id: 1, host: "mongo2:27018", priority: 0.5},
        {_id: 2, host: "mongo3:27019", priority: 0.5}
    ],
    settings: {
        chainingAllowed: false,
        electionTimeoutMillis: 10000,
        heartbeatTimeoutSecs: 10
    }
    })'
}

# Needed only in scenario #3
e2_rs_initiate_slow_election_detection() {
    docker exec mongo1 mongosh --port 27017 --eval '
    rs.initiate({
    _id: "rs0",
    members: [
        {_id: 0, host: "mongo1:27017", priority: 1},
        {_id: 1, host: "mongo2:27018", priority: 0.5},
        {_id: 2, host: "mongo3:27019", priority: 0.5}
    ],
    settings: {
        chainingAllowed: false,
        electionTimeoutMillis: 30000,
        heartbeatTimeoutSecs: 30,
        heartbeatIntervalMillis: 30000
    }
    })'
}

e2_rs_monitor() {
    read node port < <(validate_mongo_node "$1")
    echo "Status on ${node} port ${port}:"
    docker exec -it "${node}" mongosh  --port "${port}" --quiet --eval '
    while(true) {
    try {
        print(new Date().toISOString());
        let status = rs.status();
        if (status && status.members) {
        status.members.forEach(m => print("  " + m.name + " : " + m.stateStr));
        } else {
        print("  No replica set status available");
        }
    } catch(e) {
        print("  Error: " + e.message);
    }
    print("");
    sleep(1000);
    }'
}

e2_rs_status_get() {
    read node port < <(validate_mongo_node "$1")
    echo "Status on ${node} port ${port}:"
    docker exec -it "${node}" mongosh  --port "${port}" --eval 'rs.status().members.forEach(m => print(m.name + " : " + m.stateStr))'
}

e2_rs_status_getall() {
    echo "Status on mongo1:"
    docker exec -it mongo1 mongosh --port 27017 --eval 'rs.status().members.forEach(m => print(m.name + " : " + m.stateStr))'
    echo "Status on mongo2:"
    docker exec -it mongo2 mongosh --port 27018 --eval 'rs.status().members.forEach(m => print(m.name + " : " + m.stateStr))'
    echo "Status on mongo3:"
    docker exec -it mongo3 mongosh --port 27019 --eval 'rs.status().members.forEach(m => print(m.name + " : " + m.stateStr))'
}

e2_link_block_m1_to_m2() {
    docker exec mongo1 iptables -A OUTPUT -d mongo2 -j DROP
}

e2_link_block_m1_to_m2_and_m3() {
    docker exec mongo1 iptables -A OUTPUT -d mongo2 -j DROP
    docker exec mongo1 iptables -A OUTPUT -d mongo3 -j DROP
}

e2_link_restore_m1() {
    docker exec mongo1 iptables -F
}
