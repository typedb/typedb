package grakn.core.test.behaviour.resolution.test;

import grakn.client.GraknClient;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static grakn.verification.resolution.common.Utils.loadGqlFile;

public class LoadTest {

    public static void loadTestCase(GraknClient.Session session, String testCase) {
        try {
            Path schemaPath = Paths.get("resolution", "test", "cases", testCase, "schema.gql").toAbsolutePath();
            Path dataPath = Paths.get("resolution", "test", "cases", testCase, "data.gql").toAbsolutePath();
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
