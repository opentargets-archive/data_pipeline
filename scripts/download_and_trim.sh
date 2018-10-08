#!/usr/bin/env bash
# Download from a URL to a file and keep a random percentage of lines
#
# Usage:
#   download_and_trim <source_url> <filename> <percentage>
#

if [ $# -ne 3 ]; then
    echo "Usage:"
    echo "$0  <source_url> <filename> <percentage>"
fi


if [ ! -f "$2" ]
then
    echo "Downloading from $1 to $2"
    curl --silent --output /tmp/$2 $1
    LINES=$( gunzip -c /tmp/$2 | wc -l | tr -s ' ')
    KEEP=$(($LINES*$3/100))
    echo "Keeping only $KEEP lines (${3}% of $LINES)"
    gunzip -c /tmp/$2 | shuf -n $KEEP | gzip > $2
    rm /tmp/$2
else
    echo "$2 already exists, skipping"
fi

