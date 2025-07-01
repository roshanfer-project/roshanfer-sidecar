FROM ubuntu:noble AS main


RUN apt-get update && apt-get install -y gdb valgrind wget

COPY build/sidecar /

WORKDIR /
RUN chmod +x /sidecar

# this script is used to run the sidecar in gdb
COPY crash_bt.gdb /crash_bt.gdb

# gdb --batch -x /crash_bt.gdb --args
ENTRYPOINT [ "/bin/bash", "-c", "/sidecar /config.yaml" ]
CMD [ "/config.yaml" ]

