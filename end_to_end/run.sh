#!/bin/bash

set -eu

function print_header() {
  echo ====================================================
  echo "$@"
  echo ====================================================
}

sql_file=$1
dbname=e2e_test
log_file=/tmp/gpbackup-end-to-end.log
backupdir=/tmp/gpbackup-end-to-end


print_header Loading "$sql_file" into "$dbname"
dropdb "$dbname" > /dev/null 2>&1 || true
createdb "$dbname"
psql "$dbname" < "$sql_file" > /dev/null 2>&1

set -o pipefail

print_header Backing up "$dbname" to "$backupdir"
gpbackup --dbname "$dbname" --backupdir "$backupdir" | tee "$log_file"
timestamp=$(head -1 "$log_file" | grep "Backup Timestamp " | grep -Eo "\d{14}")

print_header Restoring "$dbname" from "$backupdir"
dropdb "$dbname"
gprestore --timestamp "$timestamp" --backupdir /tmp/gpbackup-end-to-end --createdb

print_header "$sql_file" SUCCESS!
