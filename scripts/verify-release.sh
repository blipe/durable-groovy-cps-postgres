#!/usr/bin/env sh
set -eu
command -v mvn >/dev/null 2>&1 || {
  echo "Maven 3.9+ is required (mvn was not found)." >&2
  exit 1
}
mvn -B -ntp clean verify
mvn -B -ntp -Pload-tests test

# A release must preserve every checked-in durable fixture.
find src/test/resources/compatibility -type f -name '*.snapshot' -print | sort
