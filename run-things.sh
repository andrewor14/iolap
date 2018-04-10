#!/bin/bash

INPUT_PATH="/disk/local/disk2/andrew/data/"
INPUT_DATA="$INPUT_PATH/students1g.json" SHOULD_CACHE_TABLES=true EXPR_NAME="timing_1g_cache" ./run-timing.sh
INPUT_DATA="$INPUT_PATH/students1g.json" SHOULD_CACHE_TABLES=false EXPR_NAME="timing_1g_nocache" ./run-timing.sh
INPUT_DATA="$INPUT_PATH/students30g.json" SHOULD_CACHE_TABLES=true EXPR_NAME="timing_30g_cache" ./run-timing.sh
INPUT_DATA="$INPUT_PATH/students30g.json" SHOULD_CACHE_TABLES=false EXPR_NAME="timing_30g_nocache" ./run-timing.sh

