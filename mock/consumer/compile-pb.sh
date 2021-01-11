#!/bin/bash

cd ../../
echo $PWD
for i in $(ls $PWD/pb/*.proto); do
	fn=$PWD/pb/$(basename "$i")
	echo "compile" $fn
	protoc -I/usr/local/include -I . --proto_path=$PWD/pb --python_out=$PWD/mock/consumer/api "$fn"
done
