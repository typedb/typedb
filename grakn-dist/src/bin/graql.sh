#!/bin/bash
[[ $(readlink $0) ]] && path=$(readlink $0) || path=$0
java -cp "`dirname $path`/../lib/*" io.grakn.graql.GraqlShell ${1+"$@"}
