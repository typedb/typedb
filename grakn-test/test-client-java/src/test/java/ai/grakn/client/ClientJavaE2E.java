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
import static ai.grakn.graql.Graql.define;
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

    /**
     * Performs various queries with the client-java library
     * define a schema, define a rule, match; get;, match; insert;, match; delete;, match; aggregate;, and compute count
     *
     * The tests are highly interconnected hence why they are grouped into a single test.
     * If you split them into multiple tests, there is no guarantee that they are ran in the order they are defined,
     * and there is a chance that the match; get; test is performed before the define a schema test, which would cause it to fail.
     *
     * The schema describes a lion family which consists of a lion, lioness, and the offspring - three young lions. The mating
     * relationship captures the mating act between the male and female partners (ie., the lion and lioness). The child-bearing
     * relationship captures the child-bearing act which results from the mating act.
     *
     * The rule is one such that if there is an offspring which is the result of a certain child-bearing act, then
     * that offspring is the child of the male and female partners which are involved in the mating act.
     *
     */
    @Test
    public void clientJavaE2E() {
        // define a schema
        localGraknTx(tx -> {
            tx.graql().define(
                    label("child-bearing").sub("relationship").relates("offspring").relates("child-bearer"),
                    label("mating").sub("relationship").relates("male-partner").relates("female-partner").plays("child-bearer"),
                    label("parentship").sub("relationship").relates("parent").relates("child"),
                    label("name").sub("attribute").datatype(AttributeType.DataType.STRING),
                    label("lion").sub("entity").has("name").plays("male-partner").plays("female-partner").plays("offspring")
            ).execute();
            tx.commit();
        });

        // assert schema defined
        localGraknTx(tx -> {
            List<String> definedSchema = tx.graql().match(var("t").sub("thing")).get().execute().stream()
                    .map(answer -> answer.get(var("t")).asType().label().getValue()).collect(Collectors.toList());
            String[] correctSchema = new String[] { "thing", "entity", "relationship", "attribute",
                    "lion", "mating", "parentship", "child-bearing", "@has-name", "name" };
            assertThat(definedSchema, hasItems(correctSchema));
        });

        // insert
        localGraknTx(tx -> {
            String[] names = lionNames();
            tx.graql().insert(
                    var().isa("lion").has("name").val(names[0]),
                    var().isa("lion").has("name").val(names[1]),
                    var().isa("lion").has("name").val(names[2]),
                    var().isa("lion").has("name").val(names[3]),
                    var().isa("lion").has("name").val(names[4])
            ).execute();
            tx.commit();
        });

        // match get
        localGraknTx(tx -> {
            List<ConceptMap> people = tx.graql().match(var("p").isa("lion").has("name", var("n"))).get().execute();
            List<String> insertedNames = people.stream().map(answer -> answer.get("n").asAttribute().value().toString()).collect(Collectors.toList());
            assertThat(insertedNames, containsInAnyOrder(lionNames()));
        });

        // match insert
        localGraknTx(tx -> {
            String[] familyMembers = lionNames();
            List<ConceptMap> insertedMating = tx.graql()
                    .match(
                            var("male-partner").isa("lion").has("name").val(familyMembers[0]),
                            var("female-partner").isa("lion").has("name").val(familyMembers[1]))
                    .insert(var().isa("mating").rel("male-partner", var("male-partner")).rel("female-partner", var("female-partner")))
                    .execute();
            assertThat(insertedMating, hasSize(1));
        });

        // match aggregate
        localGraknTx(tx -> {
            int aggregateCount = tx.graql().match(var("p").isa("lion")).aggregate(count()).execute().number().intValue();
            assertThat(aggregateCount, equalTo(lionNames().length));
        });

        // compute count
        localGraknTx(tx -> {
            int computeCount = tx.graql().compute(GraqlSyntax.Compute.Method.COUNT).in("lion")
                    .execute().get(0).number().intValue();
            assertThat(computeCount, equalTo(lionNames().length));
        });

        // match delete
        localGraknTx(tx -> {
            tx.graql().match(var("m").isa("mating")).delete(var("m")).execute();
            List<ConceptMap> matings = tx.graql().match(var("m").isa("mating")).get().execute();
            assertThat(matings, hasSize(0));
        });
    }

    private String[] lionNames() {
        return new String[] { "male-partner", "female-partner", "young-lion-1",
                "young-lion-2", "young-lion-3" };
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
