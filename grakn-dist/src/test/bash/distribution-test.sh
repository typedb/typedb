#!/bin/bash

BASEDIR=$(dirname "$0")
GRAKN_DIST_TARGET=$BASEDIR/../../../target/
GRAKN_DIST_TMP=$GRAKN_DIST_TARGET/grakn-bash-test/
REDIS_DATA_DIR=./db/redis

STORAGE_PID=/tmp/grakn-storage.pid
GRAKN_PID=/tmp/grakn.pid
QUEUE_PID=/tmp/grakn-queue.pid

must_properly_start() {
  "${GRAKN_DIST_TMP}"/grakn server start
  local count_running_cassandra_process=`ps -ef | grep 'CassandraDaemon' | grep -v grep | awk '{ print $2}' | wc -l`
  local count_running_redis_process=`ps -ef | grep 'redis-server' | grep -v grep | awk '{ print $2}' | wc -l`
  local count_running_grakn_process=`ps -ef | grep 'Grakn' | grep -v grep | awk '{ print $2}' | wc -l`

  if [[ $count_running_cassandra_process -ne 1 ]]; then
    echo "Error in starting Cassandra: Expected to find 1 running Cassandra process. Found " $count_running_cassandra_process
    return 1
  fi
  if [[ $count_running_redis_process -ne 1 ]]; then
    echo "Error in starting Redis: Expected to find 1 running Redis process. Found " $count_running_redis_process
    return 1
  fi
  if [[ $count_running_grakn_process -ne 1 ]]; then
    echo "Error in starting Grakn: Expected to find 1 running Redis process. Found " $count_running_grakn_process
    return 1
  fi

  return 0
}

pid_files_must_exist() {
  if [[ ! -s "${STORAGE_PID}" ]]; then
    echo "Either Cassandra PID file is missing, or it is an empty file"
    return 1
  fi
  if [[ ! -s "${QUEUE_PID}" ]]; then
    echo "Either Redis PID file is missing, or it is an empty file"
    return 1
  fi
  if [[ ! -s "${GRAKN_PID}" ]]; then
    echo "Either Grakn PID file is missing, or it is an empty file"
    return 1
  fi
  return 0
}

untar_grakn_dist() {
  local package=$(ls $GRAKN_DIST_TARGET/grakn-dist-*.tar.gz | head -1)
  mkdir -p ${GRAKN_DIST_TMP}
  mkdir -p ${REDIS_DATA_DIR}
  tar -xf $GRAKN_DIST_TARGET/$(basename ${package}) -C ${GRAKN_DIST_TMP} --strip-components=1
}

load_data(){
  echo "Inserting data!"
  "${GRAKN_DIST_TMP}"/graql console < insert-data.gql
}

count_person_equal_to_4()
{
  local count=$(cat query-data.gql | "${GRAKN_DIST_TMP}"/graql console | grep -v '>>>' | grep person | wc -l)
  if [[ $count -eq 4 ]]; then
    return 0
  else
    return 1
  fi
}

count_marriage_equal_to_1()
{
  local count=$(cat query-marriage.gql | "${GRAKN_DIST_TMP}"/graql console |  grep -v '>>>' | grep person | wc -l)
  if [[ $count -eq 1 ]]; then
    return 0
  else
    return 1
  fi
}

pid_files_must_not_exist() {
  if [[ -e "${STORAGE_PID}" ]]; then
    echo "Cassandra PID file is not deleted"
    return 1
  fi
  if [[ -e "${QUEUE_PID}" ]]; then
    echo "Redis PID file is not deleted"
    return 1
  fi
  if [[ -e "${GRAKN_PID}" ]]; then
    echo "Grakn PID file is not deleted"
    return 1
  fi
  return 0
}

must_properly_stop() {
  "${GRAKN_DIST_TMP}"/grakn server stop

  local count_running_cassandra_process=`ps -ef | grep 'CassandraDaemon' | grep -v grep | awk '{ print $2}' | wc -l`
  local count_running_redis_process=`ps -ef | grep 'redis-server' | grep -v grep | awk '{ print $2}' | wc -l`
  local count_running_grakn_process=`ps -ef | grep 'Grakn' | grep -v grep | awk '{ print $2}' | wc -l`

  if [[ $count_running_cassandra_process -ne 0 ]]; then
    echo "Error in stopping Cassandra: Expected to find 0 running Cassandra process. Found " $count_running_cassandra_process
    return 1
  fi
  if [[ $count_running_redis_process -ne 0 ]]; then
    echo "Error in stopping Redis: Expected to find 0 running Redis process. Found " $count_running_redis_process
    return 1
  fi
  if [[ $count_running_grakn_process -ne 0 ]]; then
    echo "Error in stopping Grakn: Expected to find 0 running Redis process. Found " $count_running_grakn_process
    return 1
  fi

  return 0
}

# not using grakn server clean as it is to be removed
wipe_out_keyspaces() {
  # rm -rf db/cassandra
  # mkdir -p db/cassandra/data db/cassandra/commitlog db/cassandra/saved_caches
  rm -rf "$GRAKN_DIST_TMP"
}

wipe_out_files() {
  rm -f "${QUEUE_PID}"
  rm -f "${GRAKN_PID}"
  rm -f "${STORAGE_PID}"
}

force_kill() {
  echo "Force kill initiated - attempting to clean up any running processes..."
  local cassandra_pid=`ps -ef | grep 'CassandraDaemon' | grep -v grep | awk '{ print $2}'`
  local redis_pid=`ps -ef | grep 'redis-server' | grep -v grep | awk '{ print $2}'`
  local grakn_pid=`ps -ef | grep 'Grakn' | grep -v grep | awk '{ print $2}'`

  if [[ ! -z $grakn_pid ]]; then
    echo "Force killing Grakn (pid=$grakn_pid)"
    kill -9 $grakn_pid
  fi

  if [[ ! -z $redis_pid ]]; then
    echo "Force killing Redis (pid=$redis_pid)"
    kill -9 $redis_pid
  fi

  if [[ ! -z $cassandra_pid ]]; then
    echo "Force killing Cassandra (pid=$cassandra_pid)"
    kill -9 $cassandra_pid
  fi

  echo "Wiping out keyspaces..."
  wipe_out_keyspaces

  echo "Wiping out PID files..."
  wipe_out_files

  echo "Force kill finished."
  return 0
}

main() {
  echo "Checking if there's any dangling Redis process..."
  local redis_pid=`ps -ef | grep 'redis-server' | grep -v grep | awk '{ print $2}'`
  if [[ ! -z $redis_pid ]]; then
    echo "Force killing Redis (pid=$redis_pid)"
    kill -9 $redis_pid
  else
    echo "no dangling Redis process found."
  fi

  echo "Unarchiving distribution tarball..."
  untar_grakn_dist
  local untar_status=$?
  if [[ $untar_status -ne 0 ]]; then
    echo "Unable to unarchive distribution tarball. Halting test."
    force_kill
    exit 1
  fi

  echo "Initiating Grakn start/stop test (check if grakn can start and stop properly)"
  must_properly_start
  local startup_status=$?
  if [[ $startup_status -ne 0 ]]; then
    echo "Unable to start Grakn. Halting test."
    force_kill
    exit 1
  fi

  pid_files_must_exist
  local check_pid_exist_status=$?
  if [[ $check_pid_exist_status -ne 0 ]]; then
    echo "Some PID files are missing. Halting test."
    force_kill
    exit 1
  fi

  echo "Grakn successfully started. Testing data loading and querying..."
  load_data
  local load_data_status=$?
  if [[ $load_data_status -ne 0 ]]; then
    echo "Data could not be properly loaded. Halting test."
    force_kill
    exit 1
  fi

  echo "Data was properly loaded. Testing person query..."
  count_person_equal_to_4
  local count_person_status=$?
  if [[ $count_person_status -ne 0 ]]; then
    echo "Person count != 4. Halting test."
    force_kill
    exit 1
  fi

  echo "Testing marriage query..."
  count_marriage_equal_to_1
  local count_marriage_status=$?
  if [[ $count_marriage_status -ne 0 ]]; then
    echo "Marriage count != 1. Halting test."
    force_kill
    exit 1
  fi

  echo "Now testing graceful stop..."
  must_properly_stop
  local stop_status=$?

  if [[ $stop_status -ne 0 ]]; then
    echo "Unable to start Grakn. Halting test."
    force_kill
    exit 1
  fi

  pid_files_must_not_exist
  local check_pid_not_exist_status=$?
  if [[ $check_pid_not_exist_status -ne 0 ]]; then
    echo "Some PID files are still present after cleanup. Halting test."
    force_kill
    exit 1
  fi

  echo "Wiping out keyspaces..."
  wipe_out_keyspaces

  echo "Grakn start/stop test successful"
  exit 0
}

main
