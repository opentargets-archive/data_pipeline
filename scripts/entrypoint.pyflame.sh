#!/bin/sh

#this is a script to use pyflame for profiling https://github.com/uber/pyflame

#you must have ptrace enabled in the kernel:
#
##in /etc/sysctl.conf
##enable ptrace for all
##kernel.yama.ptrace_scope = 0: all processes can be debugged, as long as they have same uid. This is the classical way of how ptracing worked.
##kernel.yama.ptrace_scope = 1: only a parent process can be debugged.
##kernel.yama.ptrace_scope = 2: Only admin can use ptrace, as it required CAP_SYS_PTRACE capability.
##kernel.yama.ptrace_scope = 3: No processes may be traced with ptrace. Once set, a reboot is needed to enable ptracing again.
#kernel.yama.ptrace_scope=0
#
#then run
#sudo sysctl -p

echo "Running mrtarget with parameters $*"
python -m mrtarget.CommandLine "$@" &
MRT_PID=$!
#wait a bit to start
sleep 2

#run pyflame to gather information
#Usage: pyflame [options] [-p] PID
#       pyflame [options] -t command arg1 arg2...
#
#Common Options:
#  --threads                Enable multi-threading support
#  -d, --dump               Dump stacks from all threads (implies --threads)
#  -h, --help               Show help
#  -n, --no-line-numbers    Do not append line numbers to function names
#  -o, --output=PATH        Output to file path
#  -p, --pid=PID            The PID to trace
#  -r, --rate=RATE          Sample rate, as a fractional value of seconds (default 0.01)
#  -s, --seconds=SECS       How many seconds to run for (default 1)
#  -t, --trace              Trace a child process
#  -v, --version            Show the version
#  -x, --exclude-idle       Exclude idle time from statistics
#
#Advanced Options:
#  --abi                    Force a particular Python ABI (26, 34, 36)
#  --flamechart             Include timestamps for generating Chrome "flamecharts"

#monitor for 1 hour
#check twice a second
#check child processess too
/pyflame/src/pyflame -p `pgrep -f -o "python -m mrtarget.CommandLine"` -o /usr/src/app/log/pyflame.$MRT_PID.out --threads --abi 27 -s 360 -r 0.5

#run flamegraph to make the svg
/FlameGraph/flamegraph.pl </usr/src/app/log/pyflame.$MRT_PID.out >/usr/src/app/log/pyflame.$MRT_PID.svg

wait $MRT_PID

exit $!