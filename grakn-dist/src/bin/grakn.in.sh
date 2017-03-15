export GRAKN_ENGINE_CONFIG="${GRAKN_HOME}/conf/main/grakn.properties"

if [ -z "$FOREGROUND" ]; then
  export FOREGROUND=false
fi

# Check grakn.properties
# Set USE_KAFKA if taskmanager.distributed is true 
GRAKN_TASKMANAGER_DISTRIBUTED=$(grep ^taskmanager.distributed= "${GRAKN_ENGINE_CONFIG}"| cut -d '=' -f 2)
if [[ "$GRAKN_TASKMANAGER_DISTRIBUTED" == "true" ]]; then
    USE_KAFKA=true
fi


# Check grakn.properties
GRAKN_CONFIG="${GRAKN_HOME}/conf/$(grep ^graphdatabase.config= "${GRAKN_ENGINE_CONFIG}"| cut -d '=' -f 2)"

# Set USE_CASSANDRA if factory.internal is Titan
GRAKN_ENGINE_FACTORY=$(grep ^factory.internal= "${GRAKN_CONFIG}"| cut -d '=' -f 2)
if [[ "$GRAKN_ENGINE_FACTORY" == "ai.grakn.factory.TitanInternalFactory" ]]; then
    USE_CASSANDRA=true
fi
