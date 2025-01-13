#!/bin/bash

BRANCH=rc
LOG=/var/share/tt
QUICK=5
RUN="pmubihtHTsS"
TS=`date +%s`
SELF_LOG=$LOG/logs/run-release-tests-$TS.log
HOSTS=()

while [[ $# -gt 0 ]]
do
    key=$1

    case $key in
        -q)
        shift
        QUICK=$1
        ;;

        -r)
        shift
        RUN=$1
        ;;

        -h)
        echo "Usage: $0 [-r <pmubihtHTsS>] [-h] [<vms>]"
        exit 0
        ;;

        -?)
        echo "Usage: $0 [-r <pmubihtHTsS>] [-h] [<vms>]"
        exit 0
        ;;

        *)
        HOSTS+=("$1")
        ;;
    esac
    shift
done


log() {
    H=$1
    M=$2
    TS=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[$TS] [$H] $M"
    echo "[$TS] [$H] $M" >> $SELF_LOG
}

error() {
    H=$1
    M=$2
    TS=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[$TS] [$H] [ERROR] $M"
    echo "[$TS] [$H] [ERROR] $M" >> $SELF_LOG
}

fail() {
    H=$1
    M=$2
    TS=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[$TS] [$H] [FAIL] $M"
    echo "[$TS] [$H] [FAIL] $M" >> $SELF_LOG
}

pass() {
    H=$1
    M=$2
    TS=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[$TS] [$H] [PASS] $M"
    echo "[$TS] [$H] [PASS] $M" >> $SELF_LOG
}

build_pull() {
    H=$1
    ssh $H << EOF
    cd /home/yongtao/src/tt/$BRANCH;
    git pull;
EOF
    return $?
}

build_make() {
    H=$1
    ssh $H << EOF
    cd /home/yongtao/src/tt/$BRANCH;
    make clean all;
EOF
    return $?
}

ping_host() {
    ping -c 1 -W 3 $1
    return $?
}

start_host() {

    # start the VM, if not already started
    tmp=$(virsh list --all | grep " $1 " | awk '{ print $3}')

    if ([ "x$tmp" == "x" ] || [ "x$tmp" != "xrunning" ]); then
        log $1 "Start VM $1"
        virsh start $1
        if [[ $? -ne 0 ]]; then
            return 1
        fi
    else
        log $1 "VM $1 is already started"
    fi

    # wait for the VM to become available
    for i in {1..30}; do
        nmap -p 22 $1 --open | grep '22/tcp open  ssh' > /dev/null
        if [[ $? -eq 0 ]]; then
            break
        fi
        sleep 1
    done

    nmap -p 22 $1 --open | grep '22/tcp open  ssh' > /dev/null
    if [[ $? -ne 0 ]]; then
        return 1
    else
        log $1 "VM $1 is available"
    fi

    echo 0
}

stop_host() {
    virsh shutdown $1
    return $?
}

start_tt() {

    H=$1
    log $H "Starting TickTockDB on $H"
    ssh $H << EOF
    cd /home/yongtao/src/tt/$BRANCH;
    ./run-tt-release.sh -r -d
EOF
    return $?
}

stop_tt() {

    H=$1
    log $H "Stopping TickTockDB on $H"
    ssh $H << EOF
    cd /home/yongtao/src/tt/$BRANCH;
    ./admin/stop.sh;
    for ((i=0; i<30; i++)); do
        pgrep -u $USER -x tt >/dev/null 2>&1
        if [ $? -ne 0 ]; then
            break
        fi
        sleep 1
    done;
EOF
    return $?
}

run_ut() {

    H=$1
    log $H "Running unit-tests on $H"
    ssh $H << EOF
    cd /home/yongtao/src/tt/$BRANCH;
    rm -rf /tmp/tt_u;
    ./bin/all_tests >> $LOG/$H/ut.log
EOF

    tail $LOG/$H/ut.log | grep ' FAILED: 0, '
    if [[ $? -eq 0 ]]; then
        pass $H "[UT] Unit test passed!"
    else
        fail $H "[UT] Unit test failed!"
    fi
}

run_it() {

    H=$1
    log $H "Running integration-tests on $H"

    # start openTSDB
    ssh dock << EOF
    run-opentsdb.sh
EOF

    ssh $H << EOF
    cd /home/yongtao/src/tt/$BRANCH;
    rm -rf /tmp/tt_i/*;
    ./run-it.sh >> $LOG/$H/it.log
EOF

    # stop openTSDB
    ssh dock << EOF
    stop-opentsdb.sh
EOF

    tail $LOG/$H/it.log | grep ' FAILED: 0; '
    if [[ $? -eq 0 ]]; then
        pass $H "[IT] Unit test passed!"
    else
        fail $H "[IT] Unit test failed!"
    fi
}

run_bats() {

    H=$1
    log $H "Running bats-tests on $H"
    ssh $H << EOF
    cd /home/yongtao/src/tt/bats;
    rm -rf /tmp/tt_bats/*;
    export TT_SRC=../rc
    ./bats/bin/bats tests/ > $LOG/$H/bats.log
EOF

    grep '^not ok ' $LOG/$H/bats.log > /dev/null
    if [[ $? -eq 0 ]]; then
        fail $H "[BATS] Bats test failed!"
        ssh $H << EOF
    cd /home/yongtao/src/tt/bats;
    ./kill.sh
EOF
    else
        pass $H "[BATS] Bats test passed!"
    fi
}

run_bm() {

    H=$1
    log $H "Running BM $2 on $H"

    start_tt $H
    if [[ $? -ne 0 ]]; then
        error $H "Failed to start TickTockDB on $H. Skip it!"
        return 1
    fi

    echo "[BM] run-bm.sh -b -d $H -q $QUICK $2" >> $LOG/$H/run-bm.log
    ssh bench << EOF
    run-bm.sh -b -d $H -q $QUICK $2 >> $LOG/$H/run-bm.log
EOF

    stop_tt $H
    if [[ $? -ne 0 ]]; then
        error $H "Failed to stop TickTockDB on $H."
    fi

    EXPECTED=`grep 'INGESTION           ' $LOG/$H/run-bm.log | head -n 1 | awk '{print $3}'`

    # count results
    ssh $H << EOF
    cd /home/yongtao/src/tt/main;
    make tools;
    ./bin/inspect -q -b -d /tmp/tt/data &> $LOG/$H/inspect.log
EOF

    ACTUAL=`grep '^Grand Total = ' $LOG/$H/inspect.log | awk '{print $4}'`

    if [[ "$ACTUAL" == "$EXPECTED" ]]; then
        pass $H "[BM] Benchmark tests '$2' passed on $H"
    else
        fail $H "[BM] Benchmark tests '$2' failed on $H"
    fi
}



# Beginning of testing scripts

if [ ${#HOSTS[@]} -eq 0 ]; then
    # default set of hosts to run tests against
    HOSTS=("ami" "cent" "deb" "deb32" "dev" "dora" "kali" "kali32" "suse" "suse32")
fi

MSG="Running pre-release tests on ${#HOSTS[@]} hosts: ${HOSTS[@]}"
log $HOSTNAME "$MSG"

# confirm
read -p "Continue? [y/n] " -n 1 -r
echo

if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    echo "Aborted!"
    rm -f $SELF_LOG
    exit 0
fi


#start_host "dock"
#if [[ $? -ne 0 ]]; then
#    error "dock" "Failed to start VM dock. Abort!"
#    exit 1
#fi

start_host "bench"
if [[ $? -ne 0 ]]; then
    error "bench" "Failed to start VM bench. Abort!"
    exit 1
fi


# run tests against each hosts in $HOSTS
for HOST in "${HOSTS[@]}"; do

    log $HOST "Running pre-release tests against $HOST"

    start_host $HOST
    if [[ $? -ne 0 ]]; then
        error $HOST "Failed to start VM $HOST. Skip it!"
        continue
    fi

    log $HOST "Preparing TickTockDB on $HOST for testing"

    if [[ "$RUN" == *p* ]]; then
        build_pull $HOST
        if [[ $? -ne 0 ]]; then
            error $HOST "Failed to pull TickTockDB from GitHub on $HOST. Skip it!"
            continue
        fi
    fi

    if [[ "$RUN" == *m* ]]; then
        build_make $HOST
        if [[ $? -ne 0 ]]; then
            error $HOST "Failed to build TickTockDB on $HOST. Skip it!"
            continue
        fi
    fi

    # unit test
    if [[ "$RUN" == *u* ]]; then
        run_ut $HOST
    fi

    # integration test
    if [[ "$RUN" == *i* ]]; then
        run_it $HOST
    fi

    # bats test
    if [[ "$RUN" == *b* ]]; then
        run_bats $HOST
    fi

    if [[ "$RUN" == *h* ]]; then
        run_bm $HOST "-p http"
    fi

    if [[ "$RUN" == *t* ]]; then
        run_bm $HOST "-p tcp"
    fi

    if [[ "$RUN" == *H* ]]; then
        run_bm $HOST "-p http -l"
    fi

    if [[ "$RUN" == *T* ]]; then
        run_bm $HOST "-p tcp -l"
    fi

    if [[ "$RUN" == *s* ]]; then
        run_bm $HOST "-t s"
    fi

    if [[ "$RUN" == *S* ]]; then
        stop_host $HOST
        if [[ $? -ne 0 ]]; then
            error $HOST "Failed to shutdown VM $HOST. Abort!"
            break
        fi
    fi
done

log $HOSTNAME "Finished pre-release tests on ${#HOSTS[@]} hosts: ${HOSTS[@]}"

exit 0
