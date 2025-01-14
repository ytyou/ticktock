#!/bin/bash

BRANCH=rc
CLEAN=0
LOG=/var/share/tt
QUICK=5
RUN="pmubihtHTsS"
TS=`date +%s`
SCALE=s
SELF_LOG=$LOG/logs/run-release-tests-$TS.log
SHELL=/bin/bash
HOSTS=()

while [[ $# -gt 0 ]]
do
    key=$1

    case $key in
        -b)
        shift
        BRANCH=$1
        ;;

        -c)
        CLEAN=1
        ;;

        -q)
        shift
        QUICK=$1
        ;;

        -r)
        shift
        RUN=$1
        ;;

        -t)
        shift
        SCALE=$1
        ;;

        -h)
        echo "Usage: $0 [-r <pmubihtHTsS>] [-b <branch>] [-q #] [-t [l|m|s|t]] [-h] [<vms>]"
        exit 0
        ;;

        -?)
        echo "Usage: $0 [-r <pmubihtHTsS>] [-b <branch>] [-q #] [-t [l|m|s|t]] [-h] [<vms>]"
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
    echo "[$TS] [$H] [STAT] [FAIL] $M"
    echo "[$TS] [$H] [STAT] [FAIL] $M" >> $SELF_LOG
}

pass() {
    H=$1
    M=$2
    TS=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[$TS] [$H] [STAT] [PASS] $M"
    echo "[$TS] [$H] [STAT] [PASS] $M" >> $SELF_LOG
}

build_pull() {
    H=$1
    ssh $H $SHELL << EOF
    cd /home/yongtao/src/tt/$BRANCH;
    git pull;
EOF
    return $?
}

build_make() {
    H=$1
    ssh $H $SHELL << EOF
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

    log $1 "Starting VM $1"

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

    return 0
}

stop_host() {
    log $1 "Stopping VM $1"
    virsh shutdown $1
    return $?
}

start_tt() {

    H=$1
    log $H "Starting TickTockDB on $H"
    ssh $H $SHELL << EOF
    cd /home/yongtao/src/tt/$BRANCH;
    ./run-tt-release.sh -r -d
EOF
    return $?
}

stop_tt() {

    H=$1
    log $H "Stopping TickTockDB on $H"
    ssh $H $SHELL << EOF
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
    echo "[UT] Running unit-tests on $H" > $LOG/$H/ut.log
    log $H "[UT] Running unit-tests on $H"

    ssh $H $SHELL << EOF
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
    echo "[IT] Running integration-tests on $H" > $LOG/$H/it.log
    log $H "Running integration-tests on $H"

    # start openTSDB
    ssh dock $SHELL << EOF
    /home/yongtao/bin/run-opentsdb.sh
EOF

    echo > $LOG/$H/it.log
    ssh $H $SHELL << EOF
    cd /home/yongtao/src/tt/$BRANCH;
    rm -rf /tmp/tt_i;
    ./run-it.sh >> $LOG/$H/it.log
EOF

    # stop openTSDB
    ssh dock $SHELL << EOF
    /home/yongtao/bin/stop-opentsdb.sh
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
    echo "[BATS] Running bats-tests on $H" > $LOG/$H/bats.log
    log $H "[BATS] Running bats-tests on $H"

    ssh $H $SHELL << EOF
    cd /home/yongtao/src/tt/bats;
    rm -rf /tmp/tt_bats;
    export TT_SRC=../$BRANCH
    ./bats/bin/bats tests/ >> $LOG/$H/bats.log
EOF

    grep '^not ok ' $LOG/$H/bats.log > /dev/null
    if [[ $? -eq 0 ]]; then
        fail $H "[BATS] Bats test failed!"
        ssh $H $SHELL << EOF
    cd /home/yongtao/src/tt/bats;
    ./kill.sh
EOF
    else
        pass $H "[BATS] Bats test passed!"
    fi
}

# run iotdb bms
run_iotdb_bm() {

    H=$1
    PROTO=$2
    if [[ $# -gt 2 ]]; then
        OPT="-p $PROTO $3"
        BM_LOG=$LOG/$H/bm_${PROTO}_l.log
        INSPECT_LOG=$LOG/$H/inspect_${PROTO}_l.log
    else
        OPT="-p $PROTO"
        BM_LOG=$LOG/$H/bm_${PROTO}.log
        INSPECT_LOG=$LOG/$H/inspect_${PROTO}.log
    fi

    log $H "[BM] Running IotDB BM $OPT on $H"

    start_tt $H
    if [[ $? -ne 0 ]]; then
        error $H "Failed to start TickTockDB on $H. Skip it!"
        return 1
    fi

    log $H "[BM] run-bm.sh -b -d $H -q $QUICK $OPT"
    echo "[BM] run-bm.sh -b -d $H -q $QUICK $OPT" > $BM_LOG

    ssh bench $SHELL << EOF
    /home/yongtao/bin/run-bm.sh -b -d $H -q $QUICK $OPT &>> $BM_LOG
EOF

    sleep 10
    stop_tt $H
    if [[ $? -ne 0 ]]; then
        error $H "Failed to stop TickTockDB on $H."
    fi

    EXPECTED=`tail -30 $BM_LOG | grep 'INGESTION           ' | head -n 1 | awk '{print $3}'`

    # count results
    echo > $INSPECT_LOG
    ssh $H $SHELL << EOF
    cd /home/yongtao/src/tt/main;
    make tools;
    ./bin/inspect -q -b -d /tmp/tt/data &> $INSPECT_LOG
EOF

    ACTUAL=`grep '^Grand Total = ' $INSPECT_LOG | awk '{print $4}'`

    if [[ "$ACTUAL" == "$EXPECTED" ]]; then
        pass $H "[BM] Benchmark tests '$OPT' passed on $H"
    else
        fail $H "[BM] Benchmark tests '$OPT' failed on $H; expected: $EXPECTED, actual: $ACTUAL"
    fi
}

# run timescale bms
run_timescale_bm() {

    H=$1
    log $H "[BM] Running TimeScale BM $2 on $H"

    start_tt $H
    if [[ $? -ne 0 ]]; then
        error $H "Failed to start TickTockDB on $H. Skip it!"
        return 1
    fi

    log $H "[BM] run-bm.sh -b -d $H $2"
    echo "[BM] run-bm.sh -b -d $H $2" > $LOG/$H/bm_ts.log

    ssh bench $SHELL << EOF
    /home/yongtao/bin/run-bm.sh -b -d $H $2 &>> $LOG/$H/bm_ts.log
EOF

    sleep 10
    stop_tt $H
    if [[ $? -ne 0 ]]; then
        error $H "Failed to stop TickTockDB on $H."
    fi

    EXPECTED=`tail $LOG/$H/bm_ts.log | grep ' values, took ' | awk '{print $6 }'`

    # count results
    echo > $LOG/$H/inspect_ts.log
    ssh $H $SHELL << EOF
    cd /home/yongtao/src/tt/main;
    make tools;
    ./bin/inspect -q -b -d /tmp/tt/data &> $LOG/$H/inspect_ts.log
EOF

    ACTUAL=`grep '^Grand Total = ' $LOG/$H/inspect_ts.log | awk '{print $4}'`

    if [[ "$ACTUAL" == "$EXPECTED" ]]; then
        pass $H "[BM] Benchmark tests '$2' passed on $H"
    else
        fail $H "[BM] Benchmark tests '$2' failed on $H; expected: $EXPECTED, actual: $ACTUAL"
    fi
}



# Beginning of testing scripts

if [ ${#HOSTS[@]} -eq 0 ]; then
    # default set of hosts to run tests against
    HOSTS=("ami" "cent" "deb" "deb32" "dev" "dora" "kali" "kali32" "suse" "suse32")
fi

MSG="Running pre-release tests ($RUN -q $QUICK -t $SCALE, branch=$BRANCH) on ${#HOSTS[@]} hosts: ${HOSTS[@]}"
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


if [[ $CLEAN -eq 1 ]]; then
    rm -f $LOG/logs/*
fi


if [[ "$RUN" == *i* ]]; then
    start_host "dock"
    if [[ $? -ne 0 ]]; then
        error "dock" "Failed to start VM dock. Abort!"
        exit 1
    fi
fi

if [[ "$RUN" == *h* ]] || [[ "$RUN" == *H* ]] || [[ "$RUN" == *s* ]] || [[ "$RUN" == *t* ]] || [[ "$RUN" == *T* ]]; then
    start_host "bench"
    if [[ $? -ne 0 ]]; then
        error "bench" "Failed to start VM bench. Abort!"
        exit 1
    fi
fi


# run tests against each hosts in $HOSTS
for HOST in "${HOSTS[@]}"; do

    log $HOST "Running pre-release tests ($RUN) against $HOST"

    if [[ $CLEAN -eq 1 ]]; then
        rm -f $LOG/$HOST/*
    fi

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
        run_iotdb_bm $HOST "http"
    fi

    if [[ "$RUN" == *t* ]]; then
        run_iotdb_bm $HOST "tcp"
    fi

    if [[ "$RUN" == *H* ]]; then
        run_iotdb_bm $HOST "http" "-l"
    fi

    if [[ "$RUN" == *T* ]]; then
        run_iotdb_bm $HOST "tcp" "-l"
    fi

    if [[ "$RUN" == *s* ]]; then
        run_timescale_bm $HOST "-t $SCALE"
    fi

    if [[ "$RUN" == *S* ]]; then
        stop_host $HOST
        if [[ $? -ne 0 ]]; then
            error $HOST "Failed to shutdown VM $HOST. Abort!"
            break
        fi
    fi
done

if [[ "$RUN" == *S* ]]; then
    stop_host "dock"
    stop_host "bench"
fi

log $HOSTNAME "Finished pre-release tests on ${#HOSTS[@]} hosts: ${HOSTS[@]}"

exit 0
