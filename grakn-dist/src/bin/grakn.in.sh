export GRAKN_CONFIG="${GRAKN_HOME}/conf/main/grakn.properties"

if [ -z "$FOREGROUND" ]; then
  export FOREGROUND=false
fi

# Check grakn-engine.properties
# Set USE_KAFKA if taskmanager.distributed is true 
GRAKN_TASKMANAGER_IMPL=$(grep ^taskmanager.implementation= "${GRAKN_CONFIG}"| cut -d '=' -f 2)
if [[ "$GRAKN_TASKMANAGER_IMPL" != "ai.grakn.engine.tasks.manager.StandaloneTaskManager" ]]; then
    USE_KAFKA=true
fi


# Set USE_CASSANDRA if factory.internal is Titan
GRAKN_ENGINE_FACTORY=$(grep ^factory.internal= "${GRAKN_CONFIG}"| cut -d '=' -f 2)
if [[ "$GRAKN_ENGINE_FACTORY" == "ai.grakn.factory.TitanInternalFactory" ]]; then
    USE_CASSANDRA=true
fi
