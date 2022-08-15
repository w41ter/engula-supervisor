#!/bin/bash

BASE_DIR=$(pwd)

ENGULA_DIR=${BASE_DIR}/../engula
NUM_SERVERS=5
TEST_NAME=cluster_test

function run_supervisor() {
    ulimit -c unlimited
    ulimit -n 102400
    setsid ${BASE_DIR}/target/debug/engula-supervisor \
        --config ${BASE_DIR}/chaos-config \
        >${BASE_DIR}/log 2>&1 &
}

function random_range() {
    local min=$1
    local max=$2
    local len=$(($max - $min + 1))

    echo $((${min} + $RANDOM % ${len}))
}

function random_server_id() {
    random_range 1 ${NUM_SERVERS}
}

function restart_server() {
    local server_id=$(random_server_id)

    pushd $ENGULA_DIR >/dev/null 2>&1
    echo "stop server ${server_id}"
    ./scripts/bootstrap.sh stop ${server_id}

    local seconds=$(random_range 30 120)
    echo "sleep ${seconds}"
    sleep ${seconds}

    echo "start server ${server_id}"
    ./scripts/bootstrap.sh start ${server_id}

    popd >/dev/null 2>&1
}

function next_random_op() {
    local ops=(restart_server)

    ${ops[$RANDOM % ${#ops[@]}]}

    # sleep 3 seconds before check status
    sleep 3
}

function has_core() {
    local dir=$1
    local num_cores=$(ls -lha ${dir}/core*)
    if [[ $num_cores != "0" ]]; then
        echo "find ${num_cores} cores in dir ${dir}"
        exit 1
    fi
}

function check_cluster_status() {
    pushd $ENGULA_DIR >/dev/null 2>&1

    # check live servers
    local live_servers=$(./scripts/bootstrap.sh status | wc -l)
    if [[ $live_servers != ${NUM_SERVERS} ]]; then
        echo "there only ${live_servers} lives ..."
        exit 1
    fi

    # check cores
    has_core ${ENGULA_DIR}/${TEST_NAME}/server/*/

    popd $ENGULA_DIR >/dev/null 2>&1

    local num_supervisor=$(ps -ef | grep engula-supervisor | grep -v grep | wc -l)
    if [[ $num_supervisor == "0" ]]; then
        echo "supervisor was died ..."
        exit 1
    fi

    has_core .
}

function start_cluster() {
    pushd ${ENGULA_DIR} >/dev/null 2>&1
    ./scripts/bootstrap.sh setup
    popd ${ENGULA_DIR} >/dev/null 2>&1
}

start_cluster
sleep 10
run_supervisor

while [[ true ]]; do
    next_random_op
    check_cluster_status

    seconds=$(random_range 60 120)
    echo "sleep ${seconds} before next rounds"
    sleep ${seconds}
done
