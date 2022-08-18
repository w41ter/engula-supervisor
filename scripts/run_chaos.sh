#!/bin/bash

BASE_DIR=$(pwd)

ENGULA_DIR=${BASE_DIR}/../engula
NUM_SERVERS=5
TEST_NAME=cluster_test
ROUND_INTERVAL_MIN=30
ROUND_INTERVAL_MAX=60
RESTART_INTERVAL_MIN=16
RESTART_INTERVAL_MAX=32

export RUST_LOG=debug

function msg() {
    echo "$(date '+%Y-%m-%d %H:%M:%S'): $@"
}

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
    msg "stop server ${server_id}"
    ./scripts/bootstrap.sh stop ${server_id}

    local seconds=$(random_range ${RESTART_INTERVAL_MIN} ${RESTART_INTERVAL_MAX})
    msg "wait ${seconds} seconds before start server"
    sleep ${seconds}

    msg "start server ${server_id}"
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
    local num_cores=$(ls -lha ${dir} | grep core | wc -l)
    if [[ $num_cores != "0" ]]; then
        msg "find ${num_cores} cores in dir ${dir}"
        exit 1
    fi
}

function check_cluster_status() {
    pushd $ENGULA_DIR >/dev/null 2>&1

    # check live servers
    local live_servers=$(./scripts/bootstrap.sh status | wc -l)
    if [[ $live_servers != ${NUM_SERVERS} ]]; then
        msg "there only ${live_servers} lives ..."
        exit 1
    fi

    # check cores
    has_core ${ENGULA_DIR}/${TEST_NAME}/server/

    popd $ENGULA_DIR >/dev/null 2>&1

    local num_supervisor=$(ps -ef | grep engula-supervisor | grep -v grep | wc -l)
    if [[ $num_supervisor == "0" ]]; then
        msg "supervisor was died ..."
        exit 1
    fi

    has_core .
}

function start_cluster() {
    pushd ${ENGULA_DIR} >/dev/null 2>&1
    ./scripts/bootstrap.sh setup
    popd ${ENGULA_DIR} >/dev/null 2>&1
}

pidof engula-supervisor >/dev/null
if [[ $? != "1" ]]; then
    msg "there exists a supervisor"
    exit 1
fi

start_cluster
sleep 10
run_supervisor

while [[ true ]]; do
    next_random_op
    check_cluster_status

    seconds=$(random_range ${ROUND_INTERVAL_MIN} ${ROUND_INTERVAL_MAX})
    msg "sleep ${seconds} seconds before next rounds"
    sleep ${seconds}
done
