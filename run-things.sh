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

INPUT_PATH="/disk/local/disk2/stafman"
#INPUT_DATA="$INPUT_PATH/students1g.json" SHOULD_CACHE_TABLES=true EXPR_NAME="timing_1g_cache" ./run-timing.sh
#INPUT_DATA="$INPUT_PATH/students1g.json" SHOULD_CACHE_TABLES=false EXPR_NAME="timing_1g_nocache" ./run-timing.sh
#INPUT_DATA="$INPUT_PATH/students30g.json" SHOULD_CACHE_TABLES=true EXPR_NAME="timing_30g_cache" ./run-timing.sh
#INPUT_DATA="$INPUT_PATH/students30g.json" SHOULD_CACHE_TABLES=false EXPR_NAME="timing_30g_nocache" ./run-timing.sh

# 4-20
export RUNTIME_MEMORY="20g"
#INPUT_DATA="$INPUT_PATH/students12g.json" SHOULD_CACHE_TABLES=true IOLAP_CACHE_ENABLED=true EXPR_NAME="12g_full_cache" ./run-timing.sh
#INPUT_DATA="$INPUT_PATH/students12g.json" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=true EXPR_NAME="12g_iolap_cache_only" ./run-timing.sh
#INPUT_DATA="$INPUT_PATH/students12g.json" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=false EXPR_NAME="12g_no_cache" ./run-timing.sh
#INPUT_DATA="$INPUT_PATH/students33g.json" SHOULD_CACHE_TABLES=true IOLAP_CACHE_ENABLED=true EXPR_NAME="33g_full_cache" ./run-timing.sh
#INPUT_DATA="$INPUT_PATH/students33g.json" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=true EXPR_NAME="33g_iolap_cache_only" ./run-timing.sh
#INPUT_DATA="$INPUT_PATH/students33g.json" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=false EXPR_NAME="33g_no_cache" ./run-timing.sh
#INPUT_DATA="$INPUT_PATH/students66g.json" SHOULD_CACHE_TABLES=true IOLAP_CACHE_ENABLED=true EXPR_NAME="66g_full_cache" ./run-timing.sh
#INPUT_DATA="$INPUT_PATH/students66g.json" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=true EXPR_NAME="66g_iolap_cache_only" ./run-timing.sh
#NUM_BOOTSTRAP_TRIALS=5 NUM_BATCHES=160 INPUT_DATA="$INPUT_PATH/generate_16k.parquet" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=true IS_FAIR=true EXPR_NAME="25g_5boot" ./run-timing.sh
#NUM_BOOTSTRAP_TRIALS=10 NUM_BATCHES=160 INPUT_DATA="$INPUT_PATH/generate_16k.parquet" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=true IS_FAIR=true EXPR_NAME="25g_10boot" ./run-timing.sh
#NUM_BOOTSTRAP_TRIALS=25 NUM_BATCHES=160 INPUT_DATA="$INPUT_PATH/generate_16k.parquet" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=true IS_FAIR=true EXPR_NAME="25g_25boot" ./run-timing.sh
#NUM_BOOTSTRAP_TRIALS=50 NUM_BATCHES=160 INPUT_DATA="$INPUT_PATH/generate_16k.parquet" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=true IS_FAIR=true EXPR_NAME="25g_50boot" ./run-timing.sh
NUM_BOOTSTRAP_TRIALS=75 NUM_BATCHES=80 INPUT_DATA="$INPUT_PATH/generate_16k.parquet" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=true IS_FAIR=true EXPR_NAME="25g_75boot" ./run-timing.sh
#NUM_BOOTSTRAP_TRIALS=100 NUM_BATCHES=160 INPUT_DATA="$INPUT_PATH/generate_16k.parquet" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=true IS_FAIR=true EXPR_NAME="25g_100boot" ./run-timing.sh
#NUM_BOOTSTRAP_TRIALS=150 NUM_BATCHES=160 INPUT_DATA="$INPUT_PATH/generate_16k.parquet" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=true IS_FAIR=true EXPR_NAME="25g_150boot" ./run-timing.sh
#NUM_BOOTSTRAP_TRIALS=15 NUM_BATCHES=160 INPUT_DATA="$INPUT_PATH/generate_16k.parquet" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=true IS_FAIR=true EXPR_NAME="25g_15boot" ./run-timing.sh
#NUM_BOOTSTRAP_TRIALS=8 NUM_BATCHES=160 INPUT_DATA="$INPUT_PATH/generate_16k.parquet" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=true IS_FAIR=true EXPR_NAME="25g_8boot" ./run-timing.sh
#NUM_BOOTSTRAP_TRIALS=3 NUM_BATCHES=160 INPUT_DATA="$INPUT_PATH/generate_16k.parquet" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=true IS_FAIR=true EXPR_NAME="25g_3boot" ./run-timing.sh
#INPUT_DATA="$INPUT_PATH/generate_16k.parquet" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=true IS_FAIR=false EXPR_NAME="25g_iolap_cache_only_weight_loss" ./run-timing.sh
#INPUT_DATA="$INPUT_PATH/generate_16k.parquet" SHOULD_CACHE_TABLES=true IOLAP_CACHE_ENABLED=true EXPR_NAME="25g_full_cache_3" ./run-timing.sh
#INPUT_DATA="$INPUT_PATH/generate_16k.parquet" SHOULD_CACHE_TABLES=false IOLAP_CACHE_ENABLED=false EXPR_NAME="25g_no_cache_3" ./run-timing.sh
