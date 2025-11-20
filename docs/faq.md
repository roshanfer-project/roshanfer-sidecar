# Known situations

## Issues with rpc and fd already existing in the mapper
This is probably because the workload generator is timing out requests, which causes the connection to close.

The proper way of avoiding these issues is to cancel all in-flight RPCs in the microservice, but again we are not an enterprise.