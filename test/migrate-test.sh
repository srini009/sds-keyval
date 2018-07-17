#!/bin/bash -x

if [ -z $srcdir ]; then
    echo srcdir variable not set.
    exit 1
fi
source $srcdir/test/test-util.sh

find_db_name

# start a server with 2 second wait,
# 20s timeout, and my_test_db as database
a="A"
test_db_nameA=${test_db_name}$a
test_db_full="${test_db_nameA}:${test_db_type}"
test_start_server 2 20 $test_db_full
svr_addrA=$svr_addr
b="B"
test_db_nameB=${test_db_name}$b
test_db_full="${test_db_nameB}:${test_db_type}"
test_start_server 2 20 $test_db_full
svr_addrB=$svr_addr

sleep 3

#####################

run_to 20 test/sdskv-migrate-test $svr_addrA 1 $test_db_nameA $svr_addrB 1 $test_db_nameB 10
if [ $? -ne 0 ]; then
    wait
    exit 1
fi

wait

echo cleaning up $TMPBASE
rm -rf $TMPBASE

exit 0
