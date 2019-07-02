#!/bin/bash

pip install py-spy

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


# If this script is killed, kill mrtarget
trap "kill $MRT_PID 2> /dev/null" EXIT

# While mrtarget is running, attach py-spy to 
# any running mrtarget processes
# this will be the original, and any subprocesses
PIDS_DONE=""
while kill -0 $MRT_PID 2> /dev/null
do
    sleep 1

    #run py-spy to gather information
    for PID in `pgrep -f "python -m mrtarget.CommandLine"`
    do
      #echo PID $PID
      if [[ $PIDS_DONE != *" $PID "* ]]
      then
        #run py-spy to monitor and make the svg
        DATETIME=$(date +%Y%m%d%H%M%S)
        echo py-spy monitoring $PID at $DATETIME
        #monitor for 1 hour
        #check 100x a second
        #NOTE: will not check child *processes*
        bash -c "py-spy -p $PID -f /usr/src/app/log/py-spy.$DATETIME-$PID.svg -r 100 -d 3600" &

        PIDS_DONE="$PIDS_DONE $PID"
      fi
    done

done

echo "waiting for mrtarget"
wait $MRT_PID
MRT_EXIT=$?

#wait for processes to finish
echo "waiting py-spy $PIDS_DONE"
wait

echo "exiting with code $MRT_EXIT"
exit $MRT_EXIT