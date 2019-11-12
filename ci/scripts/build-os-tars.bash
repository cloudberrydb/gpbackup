#!/bin/bash

set -ex

pushd gpbackup_tar
  # Create install script
  printf "#!/bin/sh\nset -x\ntar -xzvf bin_gpbackup.tar.gz -C \$GPHOME" > install_gpdb_component
  chmod +x install_gpdb_component

  tar -xzf ../gpbackup-go-components/go_components.tar.gz
  cp ../ddboost_components/*version .
  cp gpbackup_version version

  mkdir -p bin lib
  cp gpbackup gpbackup_helper gprestore gpbackup_s3_plugin gpbackup_manager bin/
  cp ../ddboost_components/gpbackup_ddboost_plugin bin/
  cp ../ddboost_components/libDDBoost.so lib/
  tar -czvf bin_gpbackup.tar.gz bin/ lib/

  tar -czvf gpbackup-$(cat version).tar.gz bin_gpbackup.tar.gz install_gpdb_component *version
popd
