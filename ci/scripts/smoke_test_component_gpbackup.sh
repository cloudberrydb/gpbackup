#!/bin/bash
set -e

package_version=`cat version`
if [[ `gpbackup --version` != "gpbackup $package_version" ]]
then
  exit 1
fi
if [[ `gprestore --version` != "gprestore $package_version" ]]
then
  exit 1
fi
if [[ `gpbackup_helper --version` != "gpbackup_helper $package_version" ]]
then
  exit 1
fi
gpbackup_s3_plugin --version
gpbackup_ddboost_plugin --version

