FROM ubuntu:noble AS main


COPY build/sidecar /

WORKDIR /
RUN chmod +x /sidecar

ENTRYPOINT [ "/sidecar" ]
CMD [ "/config.yaml" ]

