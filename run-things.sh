#!/bin/bash

# Available runtime configs:
#   - LOG_DIR
#   - INPUT_DATA
#   - NUM_BATCHES
#   - NUM_PARTITIONS
#   - NUM_BOOTSTRAP_TRIALS
#   - SHOULD_CACHE_TABLES
#   - IOLAP_CACHE_ENABLED
#   - EXPR_NAME
#   - RUNTIME_MEMORY

INPUT_PATH="/disk/local/disk2/andrew/data"
#INPUT_DATA="$INPUT_PATH/students1g.json" SHOULD_CACHE_TABLES=true EXPR_NAME="timing_1g_cache" ./run-timing.sh
#INPUT_DATA="$INPUT_PATH/students1g.json" SHOULD_CACHE_TABLES=false EXPR_NAME="timing_1g_nocache" ./run-timing.sh
#INPUT_DATA="$INPUT_PATH/students30g.json" SHOULD_CACHE_TABLES=true EXPR_NAME="timing_30g_cache" ./run-timing.sh
#INPUT_DATA="$INPUT_PATH/students30g.json" SHOULD_CACHE_TABLES=false EXPR_NAME="timing_30g_nocache" ./run-timing.sh

# 4-20
export RUNTIME_MEMORY="20g"
INPUT_DATA="$INPUT_PATH/students12g.json" SHOULD_CACHE_TABLES=true IOLAP_CACHE_ENABLED=true EXPR_NAME="12g_full_cache" ./run-timing.sh
INPUT_DATA="$INPUT_PATH/students12g.json" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=true EXPR_NAME="12g_iolap_cache_only" ./run-timing.sh
INPUT_DATA="$INPUT_PATH/students12g.json" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=false EXPR_NAME="12g_no_cache" ./run-timing.sh
INPUT_DATA="$INPUT_PATH/students33g.json" SHOULD_CACHE_TABLES=true IOLAP_CACHE_ENABLED=true EXPR_NAME="33g_full_cache" ./run-timing.sh
INPUT_DATA="$INPUT_PATH/students33g.json" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=true EXPR_NAME="33g_iolap_cache_only" ./run-timing.sh
INPUT_DATA="$INPUT_PATH/students33g.json" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=false EXPR_NAME="33g_no_cache" ./run-timing.sh
INPUT_DATA="$INPUT_PATH/students66g.json" SHOULD_CACHE_TABLES=true IOLAP_CACHE_ENABLED=true EXPR_NAME="66g_full_cache" ./run-timing.sh
INPUT_DATA="$INPUT_PATH/students66g.json" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=true EXPR_NAME="66g_iolap_cache_only" ./run-timing.sh
INPUT_DATA="$INPUT_PATH/students66g.json" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=false EXPR_NAME="66g_no_cache" ./run-timing.sh

