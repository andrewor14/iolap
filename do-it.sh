#!/bin/bash

#INPUT_PATH="data/students.json" ./run-iolap.sh true 10 100
#INPUT_PATH="data/students.json" ./run-iolap.sh false 10 100
#INPUT_PATH="data/students10.json" ./run-iolap.sh true 10 100
#INPUT_PATH="data/students10.json" ./run-iolap.sh false 10 100
#INPUT_PATH="data/students1g.json" ./run-iolap.sh true 10 100
#INPUT_PATH="data/students1g.json" ./run-iolap.sh false 10 100
#INPUT_PATH="data/students10g.json" ./run-iolap.sh true 10 100
#INPUT_PATH="data/students10g.json" ./run-iolap.sh false 10 100

PREPARE_DATAFRAMES="true" INPUT_PATH="data/students.json" ./run-iolap.sh true 10 500
PREPARE_DATAFRAMES="true" INPUT_PATH="data/students.json" ./run-iolap.sh false 10 500
#PREPARE_DATAFRAMES="true" INPUT_PATH="data/students10.json" ./run-iolap.sh true 10 500
#PREPARE_DATAFRAMES="true" INPUT_PATH="data/students10.json" ./run-iolap.sh false 10 500
PREPARE_DATAFRAMES="true" INPUT_PATH="data/students1g.json" ./run-iolap.sh true 10 500
PREPARE_DATAFRAMES="true" INPUT_PATH="data/students1g.json" ./run-iolap.sh false 10 500
PREPARE_DATAFRAMES="true" INPUT_PATH="data/students5g.json" ./run-iolap.sh true 10 500
PREPARE_DATAFRAMES="true" INPUT_PATH="data/students5g.json" ./run-iolap.sh false 10 500
#PREPARE_DATAFRAMES="true" INPUT_PATH="data/students10g.json" ./run-iolap.sh true 10 500
#PREPARE_DATAFRAMES="true" INPUT_PATH="data/students10g.json" ./run-iolap.sh false 10 500

#NUM_PARTS="500" INPUT_PATH="data/students.json" ./run-iolap.sh true 10 500
#NUM_PARTS="500" INPUT_PATH="data/students.json" ./run-iolap.sh false 10 500
#NUM_PARTS="500" INPUT_PATH="data/students10.json" ./run-iolap.sh true 10 500
#NUM_PARTS="500" INPUT_PATH="data/students10.json" ./run-iolap.sh false 10 500
#NUM_PARTS="500" INPUT_PATH="data/students1g.json" ./run-iolap.sh true 10 500
#NUM_PARTS="500" INPUT_PATH="data/students1g.json" ./run-iolap.sh false 10 500
#NUM_PARTS="500" INPUT_PATH="data/students10g.json" ./run-iolap.sh true 10 500
#NUM_PARTS="500" INPUT_PATH="data/students10g.json" ./run-iolap.sh false 10 500
#
#INPUT_PATH="data/students.json" ./run-iolap.sh true 10 1000
#INPUT_PATH="data/students.json" ./run-iolap.sh false 10 1000
#INPUT_PATH="data/students10.json" ./run-iolap.sh true 10 1000
#INPUT_PATH="data/students10.json" ./run-iolap.sh false 10 1000
#INPUT_PATH="data/students1g.json" ./run-iolap.sh true 10 1000
#INPUT_PATH="data/students1g.json" ./run-iolap.sh false 10 1000
#INPUT_PATH="data/students10g.json" ./run-iolap.sh true 10 1000
#INPUT_PATH="data/students10g.json" ./run-iolap.sh false 10 1000

#PREPARE_DATAFRAMES="true" INPUT_PATH="data/students5g.json" ./run-iolap.sh true 1 500

