export GRAKN_ENGINE_CONFIG="${GRAKN_HOME}/conf/main/grakn-engine.properties"

if [ -z "$FOREGROUND" ]; then
  export FOREGROUND=false
fi

# Check factory.internal
GRAKN_CONFIG=$(grep ^graphdatabase.config= "${GRAKN_ENGINE_CONFIG}"| cut -d '=' -f 2)
GRAKN_ENGINE_FACTORY=$(grep ^factory.internal= "${GRAKN_HOME}/conf/${GRAKN_CONFIG}"| cut -d '=' -f 2)
if [[ "$GRAKN_ENGINE_FACTORY" == "ai.grakn.factory.TitanInternalFactory" ]]; then
    USE_CASSANDRA=true
fi

