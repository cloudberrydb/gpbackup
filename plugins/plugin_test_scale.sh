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

logdir="/tmp/test_scale_logs"
mkdir -p $logdir

print_header() {
    header="### $1 ###"
    len=$(echo $header | awk '{print length}')
    printf "%0.s#" $(seq 1 $len) && echo
    echo -e "$header"
    printf "%0.s#" $(seq 1 $len) && echo
}

print_time_exec() {
  echo $1
  time eval $1
}

test_backup_and_restore_with_plugin() {
    config=$1
    backup_flags=$2
    restore_flags=$3
    log_file="$logdir/plugin_test_log_file"
    TIMEFORMAT=%R

    if [[ "$plugin" == *gpbackup_ddboost_plugin ]]; then
      # save the encrypt key file, if it exists
      if [ -f "$MASTER_DATA_DIRECTORY/.encrypt" ] ; then
          mv $MASTER_DATA_DIRECTORY/.encrypt /tmp/.encrypt_saved
      fi
      echo "gpbackup_ddboost_plugin: 66706c6c6e677a6965796f68343365303133336f6c73366b316868326764" > $MASTER_DATA_DIRECTORY/.encrypt
    fi

    echo
    print_header "GPBACKUP $test_db (flags: [${backup_flags}])"
    print_time_exec "gpbackup --dbname $test_db --plugin-config $config $backup_flags &> $log_file"

    if [ ! $? -eq 0 ]; then
        echo
        cat $log_file
        echo
        echo "gpbackup failed. Check gpbackup log file in ~/gpAdminLogs for details."
        exit 1
    fi
    timestamp=`head -10 $log_file | grep "Backup Timestamp " | grep -Eo "[[:digit:]]{14}"`

    print_header "GPRESTORE $test_db (flags: [${restore_flags}])"
    print_time_exec "gprestore --quiet --timestamp $timestamp --plugin-config $config --create-db --redirect-db restoredb $restore_flags &> $log_file"

    dropdb restoredb
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
    $plugin delete_backup $config $timestamp
}

# ----------------------------------------------
# Run scale tests for gpbackup and gprestore with plugin with tpchdb
# ----------------------------------------------
test_db=tpchdb
restore_filter="--include-table public.lineitem_100"
#test_backup_and_restore_with_plugin "$plugin_config" "" "--jobs=4"
test_backup_and_restore_with_plugin "$plugin_config" "" "$restore_filter --jobs=4"

if [[ "$plugin" == *gpbackup_s3_plugin ]]; then
  test_backup_and_restore_with_plugin "$plugin_config" "--single-data-file" "$restore_filter"
fi

test_backup_and_restore_with_plugin "$plugin_config" "--single-data-file --no-compression" "$restore_filter"

if [[ "$plugin" == *gpbackup_ddboost_plugin ]]; then
  echo
  echo "DISABLED restore_subset"
  cp $plugin_config ${plugin_config}_nofilter
  echo "  restore_subset: \"off\"" >> ${plugin_config}_nofilter
  test_backup_and_restore_with_plugin "${plugin_config}_nofilter" "--single-data-file --no-compression" "$restore_filter"
fi


# ----------------------------------------------
# Cleanup test artifacts
# ----------------------------------------------
echo "Cleaning up leftover test artifacts"
dropdb tpchdb
rm -r $logdir


echo "# ----------------------------------------------"
echo "# Finished gpbackup plugin scale tests"
echo "# ----------------------------------------------"
