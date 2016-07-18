#!/bin/bash
[[ $(readlink $0) ]] && path=$(readlink $0) || path=$0
java -jar `dirname $path`/../target/graql-shell-*-jar-with-dependencies.jar ${1+"$@"}
