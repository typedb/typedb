package ai.grakn.client;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.util.SimpleURI;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeroturnaround.exec.ProcessExecutor;

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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests performing queries with the client-java
 *
 * @author Ganeshwara Herawan Hananda
 */
public class ClientJavaE2E {
    private static final SimpleURI graknHost = new SimpleURI("localhost", 48555);

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
    }

    @AfterClass
    public static void cleanup_cleanupDistribution() throws IOException, InterruptedException, TimeoutException {
        commandExecutor.command("./grakn", "server", "stop").execute();
        assertGraknStopped();
        FileUtils.deleteDirectory(GRAKN_UNZIPPED_DIRECTORY.toFile());
    }

    @Test
    public void clientJavaE2E() {
        // define a schema
        Keyspace keyspace = Keyspace.of("grakn");
        try (Grakn.Session session = Grakn.session(graknHost, keyspace)) {
            try (Grakn.Transaction transaction = session.transaction(GraknTxType.WRITE)) {
                defineGatesFamilySchema(transaction).execute();
                transaction.commit();
            }
        }

        // assert schema
        try (Grakn.Session session = Grakn.session(graknHost, keyspace)) {
            try (Grakn.Transaction transaction = session.transaction(GraknTxType.READ)) {
                List<String> definedSchema = getSchema(transaction).execute().stream()
                        .map(answer -> answer.get(var("t")).asType().label().getValue()).collect(Collectors.toList());
                String[] correctSchema = new String[] { "thing", "entity", "relationship", "attribute",
                        "person", "marriage", "parentship", "@has-name", "name" };
                assertThat(definedSchema, hasItems(correctSchema));
            }
        }

        // insert
        try (Grakn.Session session = Grakn.session(graknHost, keyspace)) {
            try (Grakn.Transaction transaction = session.transaction(GraknTxType.WRITE)) {
                insertGatesFamilyMembers(transaction).execute();
                transaction.commit();
            }
        }

        // match get
        try (Grakn.Session session = Grakn.session(graknHost, keyspace)) {
            try (Grakn.Transaction transaction = session.transaction(GraknTxType.READ)) {
                List<ConceptMap> people = getGatesFamilyMembers(transaction).execute();
                List<String> insertedNames = people.stream().map(answer -> answer.get("n").asAttribute().value().toString()).collect(Collectors.toList());
                assertThat(insertedNames, containsInAnyOrder(gatesFamilyNames()));
            }
        }
    }

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

    private DefineQuery defineGatesFamilySchema(Grakn.Transaction transaction) {
        return transaction.graql().define(
                label("marriage").sub("relationship").relates("spouse"),
                label("parentship").sub("relationship").relates("parent").relates("child"),
                label("name").sub("attribute").datatype(AttributeType.DataType.STRING),
                label("person").sub("entity").has("name").plays("spouse").plays("child")
        );
    }

    private GetQuery getSchema(Grakn.Transaction transaction) {
        return transaction.graql().match(var("t").sub("thing")).get();
    }

    private InsertQuery insertGatesFamilyMembers(Grakn.Transaction transaction) {
        String[] familyMembers = gatesFamilyNames();
        return transaction.graql().insert(
                var().isa("person").has("name").val(familyMembers[0]),
                var().isa("person").has("name").val(familyMembers[1]),
                var().isa("person").has("name").val(familyMembers[2]),
                var().isa("person").has("name").val(familyMembers[3]),
                var().isa("person").has("name").val(familyMembers[4])
        );
    }

    private GetQuery getGatesFamilyMembers(Grakn.Transaction transaction) {
        return transaction.graql().match(var("p").isa("person").has("name", var("n"))).get();
    }

    private String[] gatesFamilyNames() {
        return new String[] { "Bill Gates", "Melinda Gates", "Jennifer Katharine Gates",
                "Phoebe Adele Gates", "Rory John Gates" };
    }
}
