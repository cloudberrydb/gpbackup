#!/bin/bash
set -e

setup_plugin(){
  mkdir -p /tmp/plugin_dest
}

cleanup_plugin(){
  :
}

restore_metadata() {
  filename=`basename "$1"`
  directory=`dirname "$1"`
  mkdir -p $directory
	cat /tmp/plugin_dest/$filename > $1
}

backup_metadata() {
  filename=`basename "$1"`
	cat $1 > /tmp/plugin_dest/$filename
  rm $1
}

backup_data() {
  filename=`basename "$1"`
	cat - > /tmp/plugin_dest/$filename
}

restore_data() {
  filename=`basename "$1"`
	cat /tmp/plugin_dest/$filename
}

"$@"
