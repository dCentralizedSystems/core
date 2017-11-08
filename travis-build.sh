#!/bin/bash
# Build script for Travis CI.

set -ex

mvn install -DskipTests=true

