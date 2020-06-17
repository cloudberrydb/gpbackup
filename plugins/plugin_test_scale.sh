#!/bin/bash
set -e
set -o pipefail

plugin=$1
plugin_config=$2
MINIMUM_API_VERSION="0.3.0"

# ----------------------------------------------
# Test suite setup
# This will put small amounts of data in the
# plugin destination location
# ----------------------------------------------
if [ $# -lt 2 ] || [ $# -gt 3 ]
  then
    echo "Usage: plugin_test_scale.sh [path_to_executable] [plugin_config] [optional_config_for_secondary_destination]"
    exit 1
fi

if [[ "$plugin_config" != /* ]] ; then
    echo "Must provide an absolute path to the plugin config"
    exit 1
fi

logdir="/tmp/test_scale_logs"$
mkdir -p $logdir
test_db=plugin_test_db

test_preparedata() {
    set +e
    echo "Preparing test data for plugin scale test"
    psql -X -d postgres -qc "DROP DATABASE IF EXISTS $test_db" 2>/dev/null
    createdb $test_db
    psql -X -d $test_db -qc "CREATE TABLE test1(i int) DISTRIBUTED RANDOMLY; INSERT INTO test1 select generate_series(1,100000000)"
    psql -X -d $test_db -qc "CREATE TABLE test2(i int) DISTRIBUTED RANDOMLY; INSERT INTO test2 select generate_series(1,100000000)"
    psql -X -d $test_db -qc "CREATE TABLE test3(i int) DISTRIBUTED RANDOMLY; INSERT INTO test3 select generate_series(1,100000000)"
    psql -X -d $test_db -qc "CREATE TABLE test4(i int) DISTRIBUTED RANDOMLY; INSERT INTO test4 select generate_series(1,100000000)"
    psql -X -d $test_db -qc "CREATE TABLE test5(i int) DISTRIBUTED RANDOMLY; INSERT INTO test5 VALUES(3333)"
    psql -X -d $test_db -qc "CREATE TABLE test6(i int) DISTRIBUTED RANDOMLY; INSERT INTO test6 select generate_series(1,100000000)"
    psql -X -d $test_db -qc "CREATE TABLE test7(i int) DISTRIBUTED RANDOMLY; INSERT INTO test7 select generate_series(1,100000000)"
    psql -X -d $test_db -qc "CREATE TABLE test8(i int) DISTRIBUTED RANDOMLY; INSERT INTO test8 select generate_series(1,100000000)"
    psql -X -d $test_db -qc "CREATE TABLE test9(i int) DISTRIBUTED RANDOMLY; INSERT INTO test9 VALUES(9999)"
}

test_backup_and_restore_with_plugin() {
    config=$1
    backup_flags=$2
    restore_filter=$3
    log_file="$logdir/plugin_test_log_file"
    TIMEFORMAT=%R

    set +e
    # save the encrypt key file, if it exists
    if [ -f "$MASTER_DATA_DIRECTORY/.encrypt" ] ; then
        mv $MASTER_DATA_DIRECTORY/.encrypt /tmp/.encrypt_saved
    fi
    echo "gpbackup_ddboost_plugin: 66706c6c6e677a6965796f68343365303133336f6c73366b316868326764" > $MASTER_DATA_DIRECTORY/.encrypt

    echo
    echo "[RUNNING] gpbackup (flags: [${backup_flags}])"
    time gpbackup --dbname $test_db --plugin-config $config $backup_flags &> $log_file
    if [ ! $? -eq 0 ]; then
        echo
        cat $log_file
        echo
        echo "gpbackup failed. Check gpbackup log file in ~/gpAdminLogs for details."
        exit 1
    fi
    timestamp=`head -4 $log_file | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}"`

    if [ "$restore_filter" == "restore-filter" ] ; then
      flags_restore=" --include-table public.test9"
    fi
    echo "[RUNNING] gprestore (flags: [${flags_restore}])"

    time gprestore --timestamp $timestamp --plugin-config $config --create-db --redirect-db ${test_db}_restore $flags_restore &> $log_file

    dropdb ${test_db}_restore

    if [ ! $? -eq 0 ]; then
        echo
        cat $log_file
        echo
        echo "gprestore failed. Check gprestore log file in ~/gpAdminLogs for details."
        exit 1
    fi

    # replace the encrypt key file to its proper location
    if [ -f "/tmp/.encrypt_saved" ] ; then
        mv /tmp/.encrypt_saved $MASTER_DATA_DIRECTORY/.encrypt
    fi
    set -e
}

# ----------------------------------------------
# Run scale test for gpbackup and gprestore with plugin
# ----------------------------------------------
test_preparedata
test_backup_and_restore_with_plugin "$plugin_config"
test_backup_and_restore_with_plugin "$plugin_config" "--single-data-file"
test_backup_and_restore_with_plugin "$plugin_config" "" "restore-filter"
test_backup_and_restore_with_plugin "$plugin_config" "--single-data-file" "restore-filter"
test_backup_and_restore_with_plugin "$plugin_config" "--single-data-file --no-compression" "restore-filter"

echo
echo "DISABLED restore_subset"
plugin_config_temp=$(mktemp)
cat $plugin_config > $plugin_config_temp
echo "  restore_subset: \"off\"" >> $plugin_config_temp
test_backup_and_restore_with_plugin "$plugin_config_temp" "--single-data-file --no-compression" "restore-filter"

# ----------------------------------------------
# Cleanup test artifacts
# ----------------------------------------------
echo "Cleaning up leftover test artifacts"

dropdb $test_db
rm -r $logdir

if (( 1 == $(echo "0.4.0 $api_version" | awk '{print ($1 > $2)}') )) ; then
  echo "[SKIPPING] cleanup of uploaded test artifacts using plugins (only compatible with version >= 0.4.0)"
else
  $plugin delete_backup $plugin_config $time_second
fi

echo "# ----------------------------------------------"
echo "# Finished gpbackup plugin scale tests"
echo "# ----------------------------------------------"
