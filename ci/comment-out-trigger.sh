#!/usr/bin/env bash

set -e

FILE="pipeline.yml"
cp ${FILE} .${FILE}.bak

# detect line number of resource
resrc_line_num=`grep -Fn 'name: nightly-trigger' $FILE | grep -Eo '^[^:]+'`

# comment out all 7 lines of resource
for i in {0..6} ; do
  ln=$((${resrc_line_num}+$i))
  sed -i.del "${ln}s/.*/#&/" $FILE
done

# detect line number of job "get" refs
nt_line_nums=`grep -Fn 'get: nightly-trigger' $FILE | grep -Eo '^[^:]+'`
# comment out "get" refs and "trigger: true"
for ln in $nt_line_nums ; do
  sed -i.del "${ln}s/.*/#&/" $FILE
  next_ln=$((${ln}+1))
  sed -i.del "${next_ln}s/.*/#&/" $FILE
done

rm $FILE.del

echo "The nightly-trigger has been commented out, and a backup file has been created at '.pipeline.yml.bak'."
echo "To add the nightly trigger back, run ./uncomment-trigger.sh"
