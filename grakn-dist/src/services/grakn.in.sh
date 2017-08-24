export GRAKN_CONFIG="${GRAKN_HOME}/conf/grakn.properties"

if [ -z "$FOREGROUND" ]; then
  export FOREGROUND=false
fi

# Set USE_CASSANDRA if factory.internal is Janus
GRAKN_ENGINE_FACTORY=$(grep ^factory.internal= "${GRAKN_CONFIG}"| cut -d '=' -f 2)
if [[ "$GRAKN_ENGINE_FACTORY" == "ai.grakn.factory.TxFactoryJanus" ]]; then
    USE_CASSANDRA=true
fi
