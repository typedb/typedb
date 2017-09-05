export GRAKN_CONFIG="${GRAKN_HOME}/conf/main/grakn.properties"

if [ -z "$FOREGROUND" ]; then
  export FOREGROUND=false
fi

# Set USE_CASSANDRA if knowledge-base.mode is Janus
GRAKN_ENGINE_FACTORY=$(grep ^knowledge-base.mode= "${GRAKN_CONFIG}"| cut -d '=' -f 2)
if [[ "$GRAKN_ENGINE_FACTORY" == "production" ]]; then
    USE_CASSANDRA=true
fi
