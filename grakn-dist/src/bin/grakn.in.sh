export GRAKN_CONFIG="${GRAKN_HOME}/conf/main/grakn.properties"

if [ -z "$FOREGROUND" ]; then
  export FOREGROUND=false
fi

# Set USE_CASSANDRA if factory.internal is Titan
GRAKN_ENGINE_FACTORY=$(grep ^factory.internal= "${GRAKN_CONFIG}"| cut -d '=' -f 2)
if [[ "$GRAKN_ENGINE_FACTORY" == "ai.grakn.factory.TitanInternalFactory" ]]; then
    USE_CASSANDRA=true
fi
