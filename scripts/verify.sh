#!/usr/bin/env sh
set -eu
command -v mvn >/dev/null 2>&1 || {
  echo "Maven 3.9+ is required (mvn was not found)." >&2
  exit 1
}
mvn -B -ntp clean test
