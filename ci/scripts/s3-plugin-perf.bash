#!/bin/bash

set -ex

ccp_src/scripts/setup_ssh_to_cluster.sh
ssh -t centos@mdw "sudo yum install -y s3fs-fuse"
ssh -t mdw "mkdir -p /tmp/s3 && \
    echo ${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY} > \${HOME}/.passwd-s3fs && \
    chmod 600 \${HOME}/.passwd-s3fs && \
    s3fs gpbackup-s3-plugin-test /tmp/s3 -o passwd_file=\${HOME}/.passwd-s3fs && \
    ln -s /tmp/s3 ~/tpch_data && \
    ls -l ~/tpch_data/benchmark/tpch/lineitem/${SCALE_FACTOR}"

cat << EOF > lineitem.ddl
CREATE TABLE lineitem (
    l_orderkey       INTEGER NOT NULL,
    l_partkey        INTEGER NOT NULL,
    l_suppkey        INTEGER NOT NULL,
    l_linenumber     INTEGER NOT NULL,
    l_quantity       DECIMAL(15,2) NOT NULL,
    l_extendedprice  DECIMAL(15,2) NOT NULL,
    l_discount       DECIMAL(15,2) NOT NULL,
    l_tax            DECIMAL(15,2) NOT NULL,
    l_returnflag     CHAR(1) NOT NULL,
    l_linestatus     CHAR(1) NOT NULL,
    l_shipdate       DATE NOT NULL,
    l_commitdate     DATE NOT NULL,
    l_receiptdate    DATE NOT NULL,
    l_shipinstruct   CHAR(25) NOT NULL,
    l_shipmode       CHAR(10) NOT NULL,
    l_comment        VARCHAR(44) NOT NULL
)
DISTRIBUTED BY (l_orderkey);
EOF

cat << EOF > gpload.yml
---
VERSION: 1.0.0.1
DATABASE: tpchdb
USER: gpadmin
HOST: localhost
PORT: ${PG_PORT}
GPLOAD:
   INPUT:
    - SOURCE:
         FILE:
           - /home/gpadmin/tpch_data/benchmark/tpch/lineitem/${SCALE_FACTOR}/lineitem.tbl
    - FORMAT: text
    - DELIMITER: '|'
    - HEADER: false
   OUTPUT:
    - TABLE: lineitem
    - MODE: insert
    - UPDATE_CONDITION: 'boolean_condition'
   PRELOAD:
    - TRUNCATE: true
    - REUSE_TABLES: false
EOF

cat <<SCRIPT > /tmp/run_perf.bash
#!/bin/bash

set -ex
source env.sh

function print_header() {
    set +x
    header="### \$1 ###"
    len=\$(echo \$header | awk '{print length}')
    printf "%0.s#" \$(seq 1 \$len) && echo
    echo -e "\$header"
    printf "%0.s#" \$(seq 1 \$len) && echo
    set -x
}

createdb tpchdb
createdb restoredb
psql -d tpchdb -a -f lineitem.ddl

print_header "LOAD lineitem data using gpload with ${SCALE_FACTOR} GB of data"
time gpload -f gpload.yml
time psql -d tpchdb -c "CREATE TABLE lineitem_1 AS SELECT * FROM lineitem DISTRIBUTED BY (l_orderkey)"
time psql -d tpchdb -c "CREATE TABLE lineitem_2 AS SELECT * FROM lineitem DISTRIBUTED BY (l_orderkey)"
time psql -d tpchdb -c "CREATE TABLE lineitem_3 AS SELECT * FROM lineitem DISTRIBUTED BY (l_orderkey)"
time psql -d tpchdb -c "CREATE TABLE lineitem_4 AS SELECT * FROM lineitem DISTRIBUTED BY (l_orderkey)"
time psql -d tpchdb -c "CREATE TABLE lineitem_5 AS SELECT * FROM lineitem DISTRIBUTED BY (l_orderkey)"

log_file=/tmp/gpbackup.log
print_header "GPBACKUP with ${SCALE_FACTOR} GB of data for each table"
time gpbackup --dbname tpchdb --plugin-config ~/s3_config.yaml | tee "\$log_file"
echo
timestamp=\$(head -5 "\$log_file" | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}")
print_header "GPRESTORE with ${SCALE_FACTOR} GB of data for each table"
time gprestore --redirect-db restoredb --timestamp "\$timestamp" --plugin-config ~/s3_config.yaml
echo
\${GPHOME}/bin/gpbackup_s3_plugin delete_backup ~/s3_config.yaml "\$timestamp"

mkdir -p /data/gpdata/stage1 /data/gpdata/stage2
pushd /data/gpdata/stage1
# Copy data from S3 to local using restore_directory
print_header "RESTORE_DIRECTORY (SERIAL) with ${SCALE_FACTOR} GB of data"
time \${GPHOME}/bin/gpbackup_s3_plugin restore_directory \
    ~/s3_config.yaml benchmark/tpch/lineitem/${SCALE_FACTOR}/lineitem_data
echo
mkdir -p tmp/\$timestamp && mv benchmark tmp/\$timestamp
# Copy data from local to S3 using backup_directory
print_header "BACKUP_DIRECTORY (SERIAL) with ${SCALE_FACTOR} GB of data"
time \${GPHOME}/bin/gpbackup_s3_plugin backup_directory \
    ~/s3_config.yaml tmp/\$timestamp/benchmark/tpch/lineitem
echo
rm -rf ~/tpch_data/tmp/\$timestamp

popd && pushd /data/gpdata/stage2
print_header "RESTORE_DIRECTORY (PARALLEL=5) with ${SCALE_FACTOR} GB of data"
# Copy data from S3 to local using restore_directory_parallel
time \${GPHOME}/bin/gpbackup_s3_plugin restore_directory_parallel \
    ~/s3_config.yaml benchmark/tpch/lineitem/${SCALE_FACTOR}/lineitem_data
echo
mkdir -p tmp/\$timestamp && mv benchmark tmp/\$timestamp
# Copy data from local to S3 using backup_directory_parallel
print_header "BACKUP_DIRECTORY (PARALLEL=5) with ${SCALE_FACTOR} GB of data"
time \${GPHOME}/bin/gpbackup_s3_plugin backup_directory_parallel \
    ~/s3_config.yaml tmp/\$timestamp/benchmark/tpch/lineitem
echo
rm -rf ~/tpch_data/tmp/\$timestamp
popd

SCRIPT

chmod +x /tmp/run_perf.bash
scp lineitem.ddl gpload.yml /tmp/run_perf.bash mdw:
ssh -t mdw "/home/gpadmin/run_perf.bash"
