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
import java.util.function.Consumer;
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
    @BeforeClass
    public static void setup_prepareDistribution() throws IOException, InterruptedException, TimeoutException {
        ProcessExecutor commandExecutor = new ProcessExecutor()
                .directory(GRAKN_UNZIPPED_DIRECTORY.toFile())
                .redirectOutput(System.out)
                .redirectError(System.err)
                .readOutput(true);

        assertGraknStopped();
        assertZipExists();
        unzipGrakn();
        commandExecutor.command("./grakn", "server", "start").execute();
        assertGraknRunning();
    }

    @AfterClass
    public static void cleanup_cleanupDistribution() throws IOException, InterruptedException, TimeoutException {
        ProcessExecutor commandExecutor = new ProcessExecutor()
                .directory(GRAKN_UNZIPPED_DIRECTORY.toFile())
                .redirectOutput(System.out)
                .redirectError(System.err)
                .readOutput(true);

        commandExecutor.command("./grakn", "server", "stop").execute();
        assertGraknStopped();
        FileUtils.deleteDirectory(GRAKN_UNZIPPED_DIRECTORY.toFile());
    }

    @Test
    public void clientJavaE2E() {
        // define a schema
        localGraknTx(tx -> {
            tx.graql().define(
                    label("marriage").sub("relationship").relates("spouse"),
                    label("parentship").sub("relationship").relates("parent").relates("child"),
                    label("name").sub("attribute").datatype(AttributeType.DataType.STRING),
                    label("person").sub("entity").has("name").plays("spouse").plays("child")
            ).execute();
            tx.commit();
        });

        // assert schema defined
        localGraknTx(tx -> {
            List<String> definedSchema = tx.graql().match(var("t").sub("thing")).get().execute().stream()
                    .map(answer -> answer.get(var("t")).asType().label().getValue()).collect(Collectors.toList());
            String[] correctSchema = new String[] { "thing", "entity", "relationship", "attribute",
                    "person", "marriage", "parentship", "@has-name", "name" };
            assertThat(definedSchema, hasItems(correctSchema));
        });

        // insert
        localGraknTx(tx -> {
            String[] familyMembers = gatesFamilyNames();
            tx.graql().insert(
                    var().isa("person").has("name").val(familyMembers[0]),
                    var().isa("person").has("name").val(familyMembers[1]),
                    var().isa("person").has("name").val(familyMembers[2]),
                    var().isa("person").has("name").val(familyMembers[3]),
                    var().isa("person").has("name").val(familyMembers[4])
            ).execute();
            tx.commit();
        });

        // match get
        localGraknTx(tx -> {
            List<ConceptMap> people = tx.graql().match(var("p").isa("person").has("name", var("n"))).get().execute();
            List<String> insertedNames = people.stream().map(answer -> answer.get("n").asAttribute().value().toString()).collect(Collectors.toList());
            assertThat(insertedNames, containsInAnyOrder(gatesFamilyNames()));
        });

        // match insert
        localGraknTx(tx -> {
            String[] familyMembers = gatesFamilyNames();
            List<ConceptMap> insertedMarriage = tx.graql()
                    .match(
                            var("husband").isa("person").has("name").val(familyMembers[0]),
                            var("wife").isa("person").has("name").val(familyMembers[1]))
                    .insert(var().isa("marriage").rel("spouse", var("husband")).rel("spouse", var("wife")))
                    .execute();
            assertThat(insertedMarriage, hasSize(1));
        });

        // match aggregate
        localGraknTx(tx -> {
            int aggregateCount = tx.graql().match(var("p").isa("person")).aggregate(count()).execute().number().intValue();
            assertThat(aggregateCount, equalTo(gatesFamilyNames().length));
        });

        // compute count
        localGraknTx(tx -> {
            int computeCount = tx.graql().compute(GraqlSyntax.Compute.Method.COUNT).in("person")
                    .execute().get(0).number().intValue();
            assertThat(computeCount, equalTo(gatesFamilyNames().length));
        });

        // match delete
        localGraknTx(tx -> {
            tx.graql().match(var("m").isa("marriage")).delete(var("m")).execute();
            List<ConceptMap> marriages = tx.graql().match(var("m").isa("marriage")).get().execute();
            assertThat(marriages, hasSize(0));
        });
    }

    private String[] gatesFamilyNames() {
        return new String[] { "Bill Gates", "Melinda Gates", "Jennifer Katharine Gates",
                "Phoebe Adele Gates", "Rory John Gates" };
    }

    private void localGraknTx(Consumer<Grakn.Transaction> fn) {
        SimpleURI graknHost = new SimpleURI("localhost", 48555);
        Keyspace keyspace = Keyspace.of("grakn");

        try (Grakn.Session session = Grakn.session(graknHost, keyspace)) {
            try (Grakn.Transaction transaction = session.transaction(GraknTxType.WRITE)) {
                fn.accept(transaction);
            }
        }
    }
}
