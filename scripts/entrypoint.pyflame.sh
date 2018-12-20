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

#run pyflame to gather information
/pyflame/src/pyflame -p `pgrep -f -o "python -m mrtarget.CommandLine"` -o /usr/src/app/log/pyflame.out

#run flamegraph to make the svg
/FlameGraph/flamegraph.pl </usr/src/app/log/pyflame.out >/usr/src/app/log/pyflame.svg

wait $MRT_PID

exit $!