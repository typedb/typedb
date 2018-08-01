package ai.grakn.client;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.util.GraqlSyntax;
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
import static ai.grakn.graql.Graql.count;
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
                transaction.graql().define(
                        label("marriage").sub("relationship").relates("spouse"),
                        label("parentship").sub("relationship").relates("parent").relates("child"),
                        label("name").sub("attribute").datatype(AttributeType.DataType.STRING),
                        label("person").sub("entity").has("name").plays("spouse").plays("child")
                ).execute();
                transaction.commit();
            }
        }

        // assert schema
        try (Grakn.Session session = Grakn.session(graknHost, keyspace)) {
            try (Grakn.Transaction transaction = session.transaction(GraknTxType.READ)) {
                List<String> definedSchema = transaction.graql().match(var("t").sub("thing")).get().execute().stream()
                        .map(answer -> answer.get(var("t")).asType().label().getValue()).collect(Collectors.toList());
                String[] correctSchema = new String[] { "thing", "entity", "relationship", "attribute",
                        "person", "marriage", "parentship", "@has-name", "name" };
                assertThat(definedSchema, hasItems(correctSchema));
            }
        }

        // insert
        try (Grakn.Session session = Grakn.session(graknHost, keyspace)) {
            try (Grakn.Transaction transaction = session.transaction(GraknTxType.WRITE)) {
                String[] familyMembers = gatesFamilyNames();
                transaction.graql().insert(
                        var().isa("person").has("name").val(familyMembers[0]),
                        var().isa("person").has("name").val(familyMembers[1]),
                        var().isa("person").has("name").val(familyMembers[2]),
                        var().isa("person").has("name").val(familyMembers[3]),
                        var().isa("person").has("name").val(familyMembers[4])
                ).execute();
                transaction.commit();
            }
        }

        // match get
        try (Grakn.Session session = Grakn.session(graknHost, keyspace)) {
            try (Grakn.Transaction transaction = session.transaction(GraknTxType.READ)) {
                List<ConceptMap> people = transaction.graql().match(var("p").isa("person").has("name", var("n"))).get().execute();
                List<String> insertedNames = people.stream().map(answer -> answer.get("n").asAttribute().value().toString()).collect(Collectors.toList());
                assertThat(insertedNames, containsInAnyOrder(gatesFamilyNames()));
            }
        }

        // match insert
        try (Grakn.Session session = Grakn.session(graknHost, keyspace)) {
            try (Grakn.Transaction transaction = session.transaction(GraknTxType.WRITE)) {
                List<ConceptMap> insertedMarriage = transaction.graql()
                        .match(var("husband").isa("person").has("name").val("Bill Gates"),
                                var("wife").isa("person").has("name").val("Melinda Gates"))
                        .insert(var().isa("marriage").rel("spouse", var("husband"))
                                .rel("spouse", var("wife"))).execute();
                assertThat(insertedMarriage, hasSize(1));
            }
        }

        // match aggregate
        try (Grakn.Session session = Grakn.session(graknHost, keyspace)) {
            try (Grakn.Transaction transaction = session.transaction(GraknTxType.READ)) {
                int aggregateCount = transaction.graql().match(var("p").isa("person")).aggregate(count()).execute().number().intValue();
                assertThat(aggregateCount, equalTo(gatesFamilyNames().length));
            }
        }

        // compute count
        try (Grakn.Session session = Grakn.session(graknHost, keyspace)) {
            try (Grakn.Transaction transaction = session.transaction(GraknTxType.READ)) {
                int computeCount = transaction.graql().compute(GraqlSyntax.Compute.Method.COUNT).in("person")
                        .execute().get(0).number().intValue();
                assertThat(computeCount, equalTo(gatesFamilyNames().length));
            }
        }

        // match delete
        try (Grakn.Session session = Grakn.session(graknHost, keyspace)) {
            try (Grakn.Transaction transaction = session.transaction(GraknTxType.WRITE)) {
                transaction.graql().match(var("m").isa("marriage")).delete(var("m")).execute();
                List<ConceptMap> marriages = transaction.graql().match(var("m").isa("marriage")).get().execute();
                assertThat(marriages, hasSize(0));
            }
        }
    }

//    /**
//     * TODO: match sibling via rule
//     */
//

    private String[] gatesFamilyNames() {
        return new String[] { "Bill Gates", "Melinda Gates", "Jennifer Katharine Gates",
                "Phoebe Adele Gates", "Rory John Gates" };
    }
}
