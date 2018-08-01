package ai.grakn.client;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.util.SimpleURI;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static ai.grakn.client.ClientJavaE2EConstants.GRAKN_UNZIPPED_DIRECTORY;
import static ai.grakn.client.ClientJavaE2EConstants.assertGraknRunning;
import static ai.grakn.client.ClientJavaE2EConstants.assertGraknStopped;
import static ai.grakn.client.ClientJavaE2EConstants.assertZipExists;
import static ai.grakn.client.ClientJavaE2EConstants.unzipGrakn;
import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests performing queries with the client-java
 *
 * @author Ganeshwara Herawan Hananda
 */
public class ClientJavaE2E {
    private static final SimpleURI graknHost = new SimpleURI("localhost", 48555);
    private static final Keyspace keyspace = Keyspace.of("grakn");

    private static ProcessExecutor commandExecutor = new ProcessExecutor()
            .directory(GRAKN_UNZIPPED_DIRECTORY.toFile())
            .redirectOutput(System.out)
            .redirectError(System.err)
            .readOutput(true);

    @BeforeClass
    public static void setup_prepareDistribution() throws IOException, InterruptedException, TimeoutException {
        assertGraknStopped();

        // unzip grakn
        assertZipExists();
        unzipGrakn();

        // start grakn
        commandExecutor.command("./grakn", "server", "start").execute();
        assertGraknRunning();

        // define schema
        defineSchema();
        assertSchemaDefined();
    }

    @AfterClass
    public static void cleanup_cleanupDistribution() throws IOException, InterruptedException, TimeoutException {
        commandExecutor.command("./grakn", "server", "stop").execute();
        assertGraknStopped();
        FileUtils.deleteDirectory(GRAKN_UNZIPPED_DIRECTORY.toFile());
    }

    private static void defineSchema() {
        try (Grakn.Session session = Grakn.session(graknHost, keyspace)) {
            try (Grakn.Transaction transaction = session.transaction(GraknTxType.WRITE)) {
                DefineQuery defineSchema = transaction.graql().define(
                        label("marriage").sub("relationship").relates("spouse"),
                        label("parentship").sub("relationship").relates("parent").relates("child"),
                        label("name").sub("attribute").datatype(AttributeType.DataType.STRING),
                        label("person").sub("entity").has("name").plays("spouse").plays("child")
                );
                defineSchema.execute();
                transaction.commit();
            }
        }
    }

    private static void assertSchemaDefined() {
        try (Grakn.Session session = Grakn.session(graknHost, keyspace)) {
            try (Grakn.Transaction transaction = session.transaction(GraknTxType.READ)) {
                List<ConceptMap> querySchema = transaction.graql().match(var("t").sub("thing")).get().execute();
                List<String> definedSchema = querySchema.stream().map(answer -> answer.get(var("t")).asType().label().getValue()).collect(Collectors.toList());
                String[] correctSchema = new String[] { "thing", "entity", "relationship", "attribute", "person", "marriage", "parentship", "@has-name", "name" };
                assertThat(definedSchema, hasItems(correctSchema));
            }
        }
    }

    /**
     * TODO: verify
     */
    @Test
    public void shouldBeAbleToPerformInsert() throws IOException, InterruptedException, TimeoutException {
//        String insert = "insert isa person has name \"Bill Gates\";" +
//                "insert isa person has name \"Melinda Gates\";" +
//                "insert isa person has name \"Jennifer Katharine Gates\";" +
//                "insert isa person has name \"Phoebe Adele Gates\";" +
//                "insert isa person has name \"Rory John Gates\";";
//        commandExecutor
//                .redirectInput(new ByteArrayInputStream(insert.getBytes(UTF_8)))
//                .command("./graql", "console", "-k", "grakn").execute().outputUTF8();
    }
//
//    /**
//     * TODO: verify
//     */
//    @Test
//    public void shouldBeAbleToPerformMatchGet() throws IOException, InterruptedException, TimeoutException {
//        String matchGet = "match $p isa person, has name $n; get;";
//        commandExecutor
//                .redirectInput(new ByteArrayInputStream(matchGet.getBytes(UTF_8)))
//                .command("./graql", "console", "-k", "grakn").execute().outputUTF8();
//    }
//
//    /**
//     TODO: verify
//     */
//    @Test
//    public void shouldBeAbleToPerformMatchInsert() throws IOException, InterruptedException, TimeoutException {
//        String matchInsert = "match $p isa person, has name \"Melinda Gates\"; insert $p has name \"Melinda Ann French\";";
//        commandExecutor
//                .redirectInput(new ByteArrayInputStream(matchInsert.getBytes(UTF_8)))
//                .command("./graql", "console", "-k", "grakn").execute().outputUTF8();
//    }
//
//    /**
//     * TODO: verify
//     */
//    @Test
//    public void shouldBeAbleToPerformMatchDelete() throws IOException, InterruptedException, TimeoutException {
//        String matchDelete = "match $n isa name \"Melinda Ann French\"; delete $n;";
//        commandExecutor
//                .redirectInput(new ByteArrayInputStream(matchDelete.getBytes(UTF_8)))
//                .command("./graql", "console", "-k", "grakn").execute().outputUTF8();
//    }
//
//
//    /**
//     * TODO: match sibling via rule
//     */
//
//
//    /**
//     * TODO: verify
//     */
//    @Test
//    public void shouldBeAbleToPerformMatchAggregate() throws IOException, InterruptedException, TimeoutException {
//        String matchGet = "match $p isa person; aggregate count;";
//        commandExecutor
//                .redirectInput(new ByteArrayInputStream(matchGet.getBytes(UTF_8)))
//                .command("./graql", "console", "-k", "grakn").execute().outputUTF8();
//    }
//
//    /**
//     * TODO: verify
//     */
//    public void shouldBeAbleToPerformComputeCount() throws IOException, InterruptedException, TimeoutException {
//        String matchGet = "compute count in person;";
//        commandExecutor
//                .redirectInput(new ByteArrayInputStream(matchGet.getBytes(UTF_8)))
//                .command("./graql", "console", "-k", "grakn").execute().outputUTF8();
//    }
}
