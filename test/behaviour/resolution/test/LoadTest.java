package grakn.core.test.behaviour.resolution.test;

import grakn.core.kb.server.Session;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static grakn.core.test.behaviour.resolution.common.Utils.loadGqlFile;


public class LoadTest {

    public static void loadTestCase(Session session, String testCase) {
        try {
            Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", testCase, "schema.gql").toAbsolutePath();
            Path dataPath = Paths.get("test", "behaviour", "resolution", "test", "cases", testCase, "data.gql").toAbsolutePath();
            // Load a schema incl. rules
            loadGqlFile(session, schemaPath);
            // Load data
            loadGqlFile(session, dataPath);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
