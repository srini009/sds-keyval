#!/bin/bash
CLIENT=(include/sdskv-client.h include/sdskv-common.h src/sdskv-client.c)
SERVER=(include/sdskv-server.h src/sdskv-rpc-types.h src/sdskv-server.cc src/bulk.h src/datastore/*)
echo "************** CLIENT ***************"
sloccount "${CLIENT[@]}"

echo "************** SERVER ***************"
sloccount "${SERVER[@]}"
