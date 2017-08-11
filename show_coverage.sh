#!/bin/bash

DIR="github.com/greenplum-db/gpbackup"
RESULTS="/tmp/results.out"
go test -coverpkg $DIR/backup $DIR/integration -coverprofile=/tmp/coverage.out 2> /dev/null | awk '{print $2 " test " $4 "\t" $5}' | awk -F"/" '{print $4}' > $RESULTS
for PACKAGE in "backup" "restore" "utils"; do
  # Generate code coverage statistics for all packages, write the coverage statistics to a file, and print the coverage percentage to the shell
  go test -coverpkg "$DIR/$PACKAGE" "$DIR/$PACKAGE" -coverprofile="/tmp/unit_$PACKAGE.out" | awk '{print $2 " unit test " $4 "\t" $5}' | awk -F"/" '{print $4}' >> $RESULTS
  # Filter out the first "mode: set" line from each coverage file and concatenate them all
  cat "/tmp/unit_$PACKAGE.out" | awk '{if($1!="mode:") {print $1 " " $2 " " $3}}' >> /tmp/coverage.out
done

# Print the total coverage percentage and generate a coverage HTML page
go tool cover -func=/tmp/coverage.out | awk '{if($1=="total:") {print $1 "\t\t\t\t" $3}}' >> $RESULTS
cat $RESULTS
rm $RESULTS
exit 0
