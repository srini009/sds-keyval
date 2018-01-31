#!/bin/bash -x

if [ -z $srcdir ]; then
    echo srcdir variable not set.
    exit 1
fi
source $srcdir/test/test-util.sh

find_db_name 

# start a server with 2 second wait,
# 20s timeout, and my_test_db as database
test_start_server 2 20 $test_db_full

sleep 1

#####################

# tear down
run_to 10 bin/sdskv-shutdown $svr_addr
if [ $? -ne 0 ]; then
    wait
    exit 1
fi

wait

echo cleaning up $TMPBASE
rm -rf $TMPBASE

echo cleaning up $test_db_name
rm_db

exit 0
