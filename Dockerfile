FROM ubuntu:noble AS main


RUN apt-get update && apt-get install -y gdb valgrind wget libc++-dev libc++abi-dev liburing2 liburing-dev

COPY build/sidecar /

WORKDIR /
RUN chmod +x /sidecar

# this script is used to run the sidecar in gdb
COPY crash_bt.gdb /crash_bt.gdb

# gdb --batch -x /crash_bt.gdb --args
#ENTRYPOINT ["gdb", "--batch", "-x", "/crash_bt.gdb", "--args", "/sidecar" ]
# Use exec -a to tag the process name (defaults to "sidecar" if PROC_NAME not set)
ENTRYPOINT [ "/bin/bash", "-c", "exec -a \"${PROC_NAME:-sidecar}\" /sidecar \"$@\"", "--" ]
CMD [ "/config.yaml" ]

