#!/bin/bash

# ****************************** #
# generate grpc apis for golang 
#
# [third-party lib dependencies]
# * libprotoc 3.12.3
# * protoc-gen-go v1.24.0
# ****************************** #

if [ -z ${GOMODULEPATH} ]; then
	echo "no GOMODULEPATH provided!!!"
	exit 1
fi

cd ${GOMODULEPATH}
for i in $(ls ${GOMODULEPATH}/github.com/amazingchow/photon-dance-delay-queue/pb/*.proto); do
	fn=github.com/amazingchow/photon-dance-delay-queue/pb/$(basename "$i")
	echo "compile" $fn
	/usr/local/bin/protoc -I/usr/local/include -I . --go_out=plugins=grpc:. "$fn"
done