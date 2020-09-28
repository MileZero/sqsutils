#!/bin/bash
set -eo pipefail
gradle -q packageLibs --stacktrace
mv build/distributions/sqsutils.zip build/sqsutils-lib.zip