#!/bin/bash

BRANCH=rc
CLEAN=0
LOG=/var/share/tt
QUICK=1
RUN="pmubihtHTsS"
TS=`date +%s`
SELF_LOG=$LOG/logs/run-release-tests-$TS.log
SHELL=/bin/bash
HOSTS=()
FAILED=0

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

        -h)
        echo "Usage: $0 [-r <pmubihtHTsS>] [-b <branch>] [-q <1-5>] [-c] [-h] [<vms>]"
        exit 0
        ;;

        -?)
        echo "Usage: $0 [-r <pmubihtHTsS>] [-b <branch>] [-q <1-5>] [-c] [-h] [<vms>]"
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
    FAILED=$((FAILED + 1))
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

wait_for_opentsdb() {
    while : ; do
        curl http://dock:4242/api/stats >/dev/null 2>&1
        if [ $? -eq 0 ]; then
            break
        fi
        echo "Waiting for OpenTSDB..."
        sleep 5
    done
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
    TT_HOME=/tmp/tt
    log $H "Starting TickTockDB on $H"
    ssh $H $SHELL << EOF
    rm -rf /tt/*;
    rm -rf $TT_HOME;
    if [[ -d "/tt" ]]; then ln -s /tt /tmp/tt; else mkdir -p /tmp/tt/data; mkdir /tmp/tt/log; fi;
    cd /home/yongtao/src/tt/$BRANCH;
    bin/tt -c conf/tt.conf -r -d -p $TT_HOME/tt.pid --ticktock.home=$TT_HOME --tsdb.timestamp.resolution=millisecond --tsdb.flush.frequency=3min --tsdb.thrashing.threshold=5min
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

    log $H "[IT] Running integration tests on $H"

    # start openTSDB
    ssh dock $SHELL << EOF
    /home/yongtao/bin/run-opentsdb.sh
EOF

    wait_for_opentsdb

    if [[ $QUICK -ge 5 ]]; then
        M=2
    elif [[ $QUICK -eq 4 ]]; then
        M=4
    elif [[ $QUICK -eq 3 ]]; then
        M=8
    else
        M=16
    fi

    echo > $LOG/$H/it.log

    # find out python version on host
    ssh $H 'python --version' 2>&1 | grep "Python 3"

    if [[ $? -eq 0 ]]; then
        ssh $H $SHELL << EOF
        rm -rf /tmp/tt_i;
        cd /home/yongtao/src/tt/$BRANCH;
        ./test/int_test3.py -o dock -n $M &>> $LOG/$H/it.log
EOF
    else
        ssh $H $SHELL << EOF
        rm -rf /tmp/tt_i;
        cd /home/yongtao/src/tt/$BRANCH;
        ./test/int_test.py -o dock &>> $LOG/$H/it.log
EOF
    fi

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

    Q=5

    if [[ $QUICK -ge 5 ]]; then
        Q=1000
    elif [[ $QUICK -eq 4 ]]; then
        Q=200
    elif [[ $QUICK -eq 3 ]]; then
        Q=50
    elif [[ $QUICK -eq 2 ]]; then
        Q=10
    fi

    log $H "[BM] run-bm.sh -b -d $H -q $Q $OPT"
    echo "[BM] run-bm.sh -b -d $H -q $Q $OPT" > $BM_LOG

    ssh bench $SHELL << EOF
    /home/yongtao/bin/run-bm.sh -b -d $H -q $Q $OPT &>> $BM_LOG
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
    cd /tmp;
    /home/yongtao/src/tt/main/bin/inspect -q -b -d /tmp/tt/data &> $INSPECT_LOG
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
    SCALE=s

    if [[ $QUICK -gt 2 ]]; then
        SCALE=t
    fi

    log $H "[BM] Running TimeScale BM -t $SCALE on $H"

    start_tt $H
    if [[ $? -ne 0 ]]; then
        error $H "Failed to start TickTockDB on $H. Skip it!"
        return 1
    fi

    log $H "[BM] run-bm.sh -b -d $H -t $SCALE"
    echo "[BM] run-bm.sh -b -d $H -t $SCALE" > $LOG/$H/bm_ts.log

    ssh bench $SHELL << EOF
    /home/yongtao/bin/run-bm.sh -b -d $H -t $SCALE &>> $LOG/$H/bm_ts.log
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
    cd /tmp;
    /home/yongtao/src/tt/main/bin/inspect -q -b -d /tmp/tt/data &> $LOG/$H/inspect_ts.log
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

MSG="Running pre-release tests ($RUN -q $QUICK branch=$BRANCH) on ${#HOSTS[@]} hosts: ${HOSTS[@]}"
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


BEGIN=$(date +%s)

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

    if [[ $FAILED -gt 0 ]]; then
        break
    fi

    # bats test
    if [[ "$RUN" == *b* ]]; then
        run_bats $HOST
    fi

    if [[ $FAILED -gt 0 ]]; then
        break
    fi

    # integration test
    if [[ "$RUN" == *i* ]]; then
        run_it $HOST
    fi

    if [[ $FAILED -gt 0 ]]; then
        break
    fi

    if [[ "$RUN" == *h* ]]; then
        run_iotdb_bm $HOST "http"
    fi

    if [[ $FAILED -gt 0 ]]; then
        break
    fi

    if [[ "$RUN" == *t* ]]; then
        run_iotdb_bm $HOST "tcp"
    fi

    if [[ $FAILED -gt 0 ]]; then
        break
    fi

    if [[ "$RUN" == *H* ]]; then
        run_iotdb_bm $HOST "http" "-l"
    fi

    if [[ $FAILED -gt 0 ]]; then
        break
    fi

    if [[ "$RUN" == *T* ]]; then
        run_iotdb_bm $HOST "tcp" "-l"
    fi

    if [[ $FAILED -gt 0 ]]; then
        break
    fi

    if [[ "$RUN" == *s* ]]; then
        run_timescale_bm $HOST
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

END=$(date +%s)
ELAPSED=$((END - BEGIN))

log $HOSTNAME "Finished pre-release tests on ${#HOSTS[@]} hosts: ${HOSTS[@]}"
if [[ $FAILED -gt 0 ]]; then
log $HOSTNAME "$FAILED tests FAILED!!!"
else
log $HOSTNAME "ALL TESTS PASSED!!!"
fi
printf 'Elapsed Time: %02d:%02d:%02d\n' $((ELAPSED/3600)) $((ELAPSED%3600/60)) $((ELAPSED%60))

exit 0
