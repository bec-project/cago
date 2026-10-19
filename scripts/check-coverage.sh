#!/usr/bin/env bash
# Require statement coverage strictly above the threshold for every package and
# the statement-weighted total. Run against a complete go test ./... profile.
set -euo pipefail

profile=${1:?usage: check-coverage.sh COVERPROFILE [THRESHOLD_PERCENT]}
threshold=${2:-90}

LC_ALL=C awk -v threshold="$threshold" '
BEGIN {
    if (threshold !~ /^[0-9]+([.][0-9]+)?$/ || threshold < 0 || threshold >= 100) {
        print "invalid coverage threshold: " threshold > "/dev/stderr"
        exit 2
    }
}
NR == 1 {
    if ($0 !~ /^mode: (set|count|atomic)$/) {
        print "invalid coverage profile header" > "/dev/stderr"
        bad = 1
        exit 2
    }
    next
}
{
    if (NF != 3 || $1 !~ /[.]go:[0-9]+[.][0-9]+,[0-9]+[.][0-9]+$/ || $2 !~ /^[0-9]+$/ || $3 !~ /^[0-9]+$/) {
        print "invalid coverage profile line " NR > "/dev/stderr"
        bad = 1
        exit 2
    }
    path = $1
    sub(/:[0-9].*$/, "", path)
    sub(/\/[^\/]+$/, "", path)
    statements[path] += $2
    total += $2
    if ($3 > 0) {
        covered[path] += $2
        totalCovered += $2
    }
}
END {
    if (bad) exit 2
    if (total == 0) {
        print "coverage profile contains no statements" > "/dev/stderr"
        exit 2
    }
    for (path in statements) {
        pct = statements[path] > 0 ? 100 * covered[path] / statements[path] : 0
        printf "%s: %.2f%%\n", path, pct
        if (pct <= threshold) failed = 1
    }
    pct = 100 * totalCovered / total
    printf "total: %.2f%% (required: >%s%% for every package and total)\n", pct, threshold
    if (pct <= threshold) failed = 1
    exit failed
}
' "$profile"
