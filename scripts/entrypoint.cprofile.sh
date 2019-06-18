#!/bin/sh

echo "Running mrtarget with parameters $*"
#python -m mrtarget.CommandLine "$@"
python -m cProfile -s time mrtarget/CommandLine.py "$@"

exit $!