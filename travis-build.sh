#!/bin/bash
# Build script for Travis CI.

export WORKING_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export OUTPUT_FILE=$WORKING_DIR/output.out
touch $OUTPUT_FILE

set -ex

mvn install -DskipTests=true >> $OUTPUT_FILE 2>&1

