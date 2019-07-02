#!/bin/sh

which time 
echo "Running mrtarget with parameters $*"
/usr/bin/time -v python -m mrtarget.CommandLine "$@"

exit $!