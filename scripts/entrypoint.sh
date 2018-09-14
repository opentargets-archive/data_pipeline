#!/bin/sh

echo "Running mrtarget with parameters $*"
python -m mrtarget.CommandLine "$@"

exit $!