#!/bin/bash

if [ -f ../status/test_failed ]; then
    echo "STAGES THAT FAILED:"
    cat ../status/test_failed
    rm -f ../status/test_failed
    exit 1
fi
./update_testlink.pl
exit 0