#!/usr/bin/env sh

cur_dir=$(cd "$(dirname "$0")" ;  pwd)

export LD_LIBRARY_PATH=${cur_dir}/lib/:${LD_LIBRARY_PATH}

${cur_dir}/bin/bfdproxy -c $1
