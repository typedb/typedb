package grakn.core.client;

import grakn.core.Keyspace;
import grakn.core.Transaction;
import grakn.core.concept.AttributeType;
import grakn.core.graql.AggregateQuery;
import grakn.core.graql.ComputeQuery;
import grakn.core.graql.DefineQuery;
import grakn.core.graql.DeleteQuery;
import grakn.core.graql.GetQuery;
import grakn.core.graql.InsertQuery;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.ConceptSet;
import grakn.core.graql.answer.Value;
import grakn.core.graql.Syntax;
import grakn.core.util.SimpleURI;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessExecutor;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static grakn.core.client.ClientJavaE2EConstants.GRAKN_UNZIPPED_DIRECTORY;
import static grakn.core.client.ClientJavaE2EConstants.assertGraknRunning;
import static grakn.core.client.ClientJavaE2EConstants.assertGraknStopped;
import static grakn.core.client.ClientJavaE2EConstants.assertZipExists;
import static grakn.core.client.ClientJavaE2EConstants.unzipGrakn;
import static grakn.core.graql.Graql.and;
import static grakn.core.graql.Graql.count;
import static grakn.core.graql.Graql.label;
import static grakn.core.graql.Graql.var;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;

/**
 *
 * Performs various queries with the client-java library:
 *  - define a schema with a rule
 *  - match; get;
 *  - match; get of an inferred relationship
 *  - match; insert;
 *  - match; delete;
 *  - match; aggregate;
 *  - and compute count
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
public class ClientJavaE2E {
    private static Logger LOG = LoggerFactory.getLogger(ClientJavaE2E.class);

    @BeforeClass
    public static void setup_prepareDistribution() throws IOException, InterruptedException, TimeoutException {
        ProcessExecutor commandExecutor = new ProcessExecutor()
                .directory(GRAKN_UNZIPPED_DIRECTORY.toFile())
                .redirectOutput(System.out)
                .redirectError(System.err)
                .readOutput(true);

        LOG.info("setup_prepareDistribution() - check if there is no existing Grakn process running...");
        assertZipExists();
        unzipGrakn();
        assertGraknStopped();
        LOG.info("setup_prepareDistribution() - done.");

        LOG.info("setup_prepareDistribution() - starting a new Grakn instance...");
        commandExecutor.command("./grakn", "server", "start").execute();
        assertGraknRunning();
        LOG.info("setup_prepareDistribution() - Grakn started successfully.");
    }

    @AfterClass
    public static void cleanup_cleanupDistribution() throws IOException, InterruptedException, TimeoutException {
        ProcessExecutor commandExecutor = new ProcessExecutor()
                .directory(GRAKN_UNZIPPED_DIRECTORY.toFile())
                .redirectOutput(System.out)
                .redirectError(System.err)
                .readOutput(true);
        LOG.info("cleanup_cleanupDistribution() - stopping the Grakn instance...");
        commandExecutor.command("./grakn", "server", "stop").execute();
        assertGraknStopped();
        LOG.info("cleanup_cleanupDistribution() - done.");

        LOG.info("cleanup_cleanupDistribution() - deleting the Grakn distribution...");
        FileUtils.deleteDirectory(GRAKN_UNZIPPED_DIRECTORY.toFile());
        LOG.info("cleanup_cleanupDistribution() - cleanup successful.");
    }

    @Test
    public void clientJavaE2E() {
        LOG.info("clientJavaE2E() - starting client-java E2E...");

        localhostGraknTx(tx -> {
            DefineQuery defineQuery = tx.graql().define(
                    label("child-bearing").sub("relationship").relates("offspring").relates("child-bearer"),
                    label("mating").sub("relationship").relates("male-partner").relates("female-partner").plays("child-bearer"),
                    label("parentship").sub("relationship").relates("parent").relates("child"),

                    label("name").sub("attribute").datatype(AttributeType.DataType.STRING),
                    label("lion").sub("entity").has("name").plays("male-partner").plays("female-partner").plays("offspring"),

                    label("infer-parentship-from-mating-and-child-bearing").sub("rule")
                            .when(and(
                                    var().rel("male-partner", var("male")).rel("female-partner", var("female")).isa("mating"),
                                    var("childbearing").rel("child-bearer").rel("offspring", var("offspring")).isa("child-bearing")
                            ))
                            .then(and(
                                    var().rel("parent", var("male")).rel("parent", var("female")).rel("child", var("offspring")).isa("parentship")
                            ))
            );
            LOG.info("clientJavaE2E() - define a schema...");
            LOG.info("clientJavaE2E() - '" + defineQuery + "'");
            List<ConceptMap> answer = defineQuery.execute();
            tx.commit();
            LOG.info("clientJavaE2E() - done.");
        });

        localhostGraknTx(tx -> {
            GetQuery getThingQuery = tx.graql().match(var("t").sub("thing")).get();
            LOG.info("clientJavaE2E() - assert if schema defined...");
            LOG.info("clientJavaE2E() - '" + getThingQuery + "'");
            List<String> definedSchema = getThingQuery.execute().stream()
                    .map(answer -> answer.get(var("t")).asType().label().getValue()).collect(Collectors.toList());
            String[] correctSchema = new String[] { "thing", "entity", "relationship", "attribute",
                    "lion", "mating", "parentship", "child-bearing", "@has-name", "name" };
            assertThat(definedSchema, hasItems(correctSchema));
            LOG.info("clientJavaE2E() - done.");
        });

        localhostGraknTx(tx -> {
            String[] names = lionNames();
            InsertQuery insertLionQuery = tx.graql().insert(
                    var().isa("lion").has("name").val(names[0]),
                    var().isa("lion").has("name").val(names[1]),
                    var().isa("lion").has("name").val(names[2])
            );
            LOG.info("clientJavaE2E() - insert some data...");
            LOG.info("clientJavaE2E() - '" + insertLionQuery + "'");
            List<ConceptMap> answer = insertLionQuery.execute();
            tx.commit();
            LOG.info("clientJavaE2E() - done.");
        });

        localhostGraknTx(tx -> {
            String[] familyMembers = lionNames();
            LOG.info("clientJavaE2E() - inserting mating relationships...");
            InsertQuery insertMatingQuery = tx.graql()
                    .match(
                            var("lion").isa("lion").has("name").val(familyMembers[0]),
                            var("lioness").isa("lion").has("name").val(familyMembers[1]))
                    .insert(var().isa("mating").rel("male-partner", var("lion")).rel("female-partner", var("lioness")));
            LOG.info("clientJavaE2E() - '" + insertMatingQuery + "'");
            List<ConceptMap> insertedMating = insertMatingQuery.execute();

            LOG.info("clientJavaE2E() - inserting child-bearing relationships...");
            InsertQuery insertChildBearingQuery = tx.graql()
                    .match(
                            var("lion").isa("lion").has("name").val(familyMembers[0]),
                            var("lioness").isa("lion").has("name").val(familyMembers[1]),
                            var("offspring").isa("lion").has("name").val(familyMembers[2]),
                            var("mating").rel("male-partner", var("lion")).rel("female-partner", var("lioness")).isa("mating")
                    )
                    .insert(var("childbearing").rel("child-bearer", var("mating")).rel("offspring", var("offspring")).isa("child-bearing"));
            LOG.info("clientJavaE2E() - '" + insertChildBearingQuery + "'");
            List<ConceptMap> insertedChildBearing = insertChildBearingQuery.execute();

            tx.commit();

            assertThat(insertedMating, hasSize(1));
            assertThat(insertedChildBearing, hasSize(1));
            LOG.info("clientJavaE2E() - done.");
        });

        localhostGraknTx(tx -> {
            LOG.info("clientJavaE2E() - execute match get on the lion instances...");
            GetQuery getLionQuery = tx.graql().match(var("p").isa("lion").has("name", var("n"))).get();
            LOG.info("clientJavaE2E() - '" + getLionQuery + "'");
            List<ConceptMap> insertedLions = getLionQuery.execute();
            List<String> insertedNames = insertedLions.stream().map(answer -> answer.get("n").asAttribute().value().toString()).collect(Collectors.toList());
            assertThat(insertedNames, containsInAnyOrder(lionNames()));

            LOG.info("clientJavaE2E() - execute match get on the mating relationships...");
            GetQuery getMatingQuery = tx.graql().match(var().isa("mating")).get();
            LOG.info("clientJavaE2E() - '" + getMatingQuery + "'");
            List<ConceptMap> insertedMating = getMatingQuery.execute();
            assertThat(insertedMating, hasSize(1));

            LOG.info("clientJavaE2E() - execute match get on the child-bearing...");
            GetQuery getChildBearingQuery = tx.graql().match(var().isa("child-bearing")).get();
            LOG.info("clientJavaE2E() - '" + getChildBearingQuery + "'");
            List<ConceptMap> insertedChildBearing = getChildBearingQuery.execute();
            assertThat(insertedChildBearing, hasSize(1));
            LOG.info("clientJavaE2E() - done.");
        });

        localhostGraknTx(tx -> {
            LOG.info("clientJavaE2E() - match get inferred relationships...");
            GetQuery getParentship = tx.graql().match(
                    var("parentship")
                            .isa("parentship")
                            .rel("parent", var("parent"))
                            .rel("child", var("child"))).get();
            LOG.info("clientJavaE2E() - '" + getParentship + "'");
            List<ConceptMap> parentship = getParentship.execute();
            assertThat(parentship, hasSize(2));
            LOG.info("clientJavaE2E() - done.");
        });

        localhostGraknTx(tx -> {
            LOG.info("clientJavaE2E() - match aggregate...");
            AggregateQuery<Value> aggregateQuery = tx.graql().match(var("p").isa("lion")).aggregate(count());
            LOG.info("clientJavaE2E() - '" + aggregateQuery + "'");
            int aggregateCount = aggregateQuery.execute().get(0).number().intValue();
            assertThat(aggregateCount, equalTo(lionNames().length));
            LOG.info("clientJavaE2E() - done.");
        });

        localhostGraknTx(tx -> {
            LOG.info("clientJavaE2E() - compute count...");
            final ComputeQuery<Value> computeQuery = tx.graql().compute(Syntax.Compute.Method.COUNT).in("lion");
            LOG.info("clientJavaE2E() - '" + computeQuery + "'");
            int computeCount = computeQuery.execute().get(0).number().intValue();
            assertThat(computeCount, equalTo(lionNames().length));
            LOG.info("clientJavaE2E() - done.");
        });

        localhostGraknTx(tx -> {
            LOG.info("clientJavaE2E() - match delete...");
            DeleteQuery deleteQuery = tx.graql().match(var("m").isa("mating")).delete(var("m"));
            LOG.info("clientJavaE2E() - '" + deleteQuery + "'");
            List<ConceptSet> answer = deleteQuery.execute();
            List<ConceptMap> matings = tx.graql().match(var("m").isa("mating")).get().execute();
            assertThat(matings, hasSize(0));
            LOG.info("clientJavaE2E() - done.");
        });

        LOG.info("clientJavaE2E() - client-java E2E test done.");
    }

    private String[] lionNames() {
        return new String[] { "male-partner", "female-partner", "young-lion" };
    }

    private void localhostGraknTx(Consumer<Grakn.Transaction> fn) {
        SimpleURI graknHost = new SimpleURI("localhost", 48555);
        Keyspace keyspace = Keyspace.of("grakn");

        try (Grakn.Session session = new Grakn(graknHost).session(keyspace)) {
            try (Grakn.Transaction transaction = session.transaction(Transaction.Type.WRITE)) {
                fn.accept(transaction);
            }
        }
    }
}
