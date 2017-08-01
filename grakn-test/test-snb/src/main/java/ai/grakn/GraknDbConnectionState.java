package ai.grakn;

import com.ldbc.driver.DbConnectionState;

import java.io.IOException;
import java.util.Map;

/**
 * @author Felix Chapman
 */
public class GraknDbConnectionState extends DbConnectionState {

    private final GraknSession session;

    public GraknDbConnectionState(Map<String, String> properties) {

        String uri;

        uri = properties.getOrDefault("uri", properties.getOrDefault("ai.grakn.uri","localhost:4567"));


        String keyspace;

        keyspace = properties.getOrDefault("keyspace", properties.getOrDefault("ai.grakn.keyspace","snb"));


        session = Grakn.session(uri, keyspace);
    }

    @Override
    public void close() throws IOException {
        session.close();
    }

    GraknSession session() {
        return this.session;
    }
}
