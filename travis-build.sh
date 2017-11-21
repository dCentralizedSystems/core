#!/bin/bash
# Build script for Travis CI.

set -ex
git fetch --tags
mvn install -DskipTests=true

