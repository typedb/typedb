package grakn.core.graql.gremlin;

import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.query.GraqlInsert;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static grakn.core.server.kb.Schema.EdgeLabel.SHARD;
import static grakn.core.server.kb.Schema.VertexProperty.LABEL_ID;
import static grakn.core.server.kb.Schema.VertexProperty.THING_TYPE_LABEL_ID;
import static graql.lang.Graql.var;

/**
 * Goal of this class is to micro-benchmark small JanusGraph operations
 * Eventually using this to derive constants for the query planner (eg. cost of an edge traversal)
 * we may want to pick the cheapest thing, for instance looking up a set of vertices by property as the base unit cost
 * and make everything else a multiplier based on this
 * The tests should fail if the constant multipliers we derive are no longer hold due to a Janus implementation change,
 * new indexing, etc.
 */
public class JanusMicroBenchmark {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer(
            Paths.get("test-integration/resources/grakn-no-caches.properties"),
            Paths.get("test-integration/resources/cassandra-embedded-no-caches.yaml"));

    private void defineSchema(SessionImpl session) {
        try (TransactionOLTP tx = session.transaction().write()) {
            tx.execute(Graql.parse("define " +
                    "age sub attribute, datatype long; " +
                    "name sub attribute, datatype string;" +
                    "person sub entity, plays role1, has age; " +
                    "plant sub entity, plays role2, has name; " +
                    "someRelation sub relation, relates role1, relates role2; ").asDefine());
            tx.commit();
        }
    }

    private String randomString() {
        return java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    }

    private Long randomLong() {
        Random random = new Random();
        return random.nextLong();
    }

    private void insert(SessionImpl session, int size, int ownersPerAge, int ownersPerName) throws InterruptedException {
        int numConcurrent = 4;
        ExecutorService pool = Executors.newFixedThreadPool(numConcurrent);
        Long startTime = System.currentTimeMillis();
        for (int j = 0; j < numConcurrent; j++) {
            pool.submit(() -> {
                String nameAttr = "";
                long ageAttr = 0;
                for (int i = 0; i < size/numConcurrent; i++) {
                    if (i % ownersPerAge == 0) {
                        ageAttr = randomLong();
                    }
                    if (i % ownersPerName == 0) {
                        nameAttr = randomString();
                    }

                    try (TransactionOLTP tx = session.transaction().write()) {
                        GraqlInsert insertQuery = Graql.insert(
                                var("pers").isa("person").has("age", ageAttr),
                                var("pl").isa("plant").has("name", nameAttr),
                                var("r").isa("someRelation").rel("role1", var("pers")).rel("role2", var("pl"))
                        );
                        tx.execute(insertQuery);
                        tx.commit();
                    }
                }
                System.out.println("Thread finished inserting");
            });
        }
        pool.awaitTermination(60, TimeUnit.SECONDS);
        System.out.println("Insert time: " + (System.currentTimeMillis() - startTime)/1000);
    }

    private SessionImpl newSession(int initialSize, int ownersPerAge, int ownersPerName) {
        SessionImpl session = server.sessionWithNewKeyspace();
        defineSchema(session);
        try {
            insert(session, initialSize, ownersPerAge, ownersPerName);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
        return session;
    }

    private List<ConceptMap> traversalToAnswers(GraphTraversal<Vertex, Map<String, Element>> traversal, TransactionOLTP tx, Set<Variable> vars) {
        return traversal
                .toStream()
                .map(elements -> createAnswer(vars, elements, tx))
                .distinct()
                .sequential()
                .map(ConceptMap::new)
                .collect(Collectors.toList());
    }

    private Map<Variable, Concept> createAnswer(Set<Variable> vars, Map<String, Element> elements, TransactionOLTP tx) {
        Map<Variable, Concept> map = new HashMap<>();
        for (Variable var : vars) {
            Element element = elements.get(var.symbol());
            if (element == null) {
                throw GraqlQueryException.unexpectedResult(var);
            } else {
                Concept result;
                if (element instanceof Vertex) {
                    result = tx.buildConcept((Vertex) element);
                } else {
                    result = tx.buildConcept((Edge) element);
                }
                Concept concept = result;
                map.put(var, concept);
            }
        }
        return map;
    }

    public String formatMetrics(List<TraversalMetrics> metrics) {
        StringBuilder formatted = new StringBuilder();
        for (TraversalMetrics m : metrics) {
            formatted.append("Metrics: \n");
            for (Metrics subMetrics : m.getMetrics()) {
                formatted
                        .append("    ")
                        .append(subMetrics.getName())
                        .append(" -- ")
                        .append(subMetrics.getDuration(TimeUnit.MILLISECONDS))
                        .append(" ms\n");
            }
            formatted.append("==> Total time: " + m.getDuration(TimeUnit.MILLISECONDS));
        }
        return formatted.toString();
    }

    public void confuse(SessionImpl session) {
        try (TransactionOLTP tx = session.transaction().write()) {
            tx.execute(Graql.parse("match $x isa plant, has name $y; get;").asGet());
            tx.execute(Graql.parse("match $r ($x); $x isa person; get;").asGet());
            tx.execute(Graql.parse("match $p isa person; get;").asGet());
        }
    }
//    @Test
//    public void sameTraversalsNotCached() throws InterruptedException {
          /*
          it seems like
           */
//        SessionImpl session = newSession(2000, 50, 25);
//        for (int i = 0; i < 10; i++) {
//            try (TransactionOLTP tx = session.transaction().read()) {
//                List<TraversalMetrics> notShardVerticesMetrics = tx.getTinkerTraversal().V().or(__.has(LABEL_ID.toString()), __.has(THING_TYPE_LABEL_ID.toString())).profile().toList();
//                System.out.println("Query time for non-shard vertices: " + notShardVerticesMetrics.get(0).getDuration(TimeUnit.MILLISECONDS));
//                Thread.sleep(5000);
//            }
//        }
//    }

    private long sum(List<Long> nums) {
        long sum = 0;
        for (Long l : nums) {
            sum += l;
        }
        return sum;
    }

    private double mean(List<Long> nums) {
        return (double) sum(nums) / nums.size();
    }

    private double stddev(List<Long> nums) {
        // compute population sttdev
        double mean = mean(nums);
        double sqSum = 0;
        for (long l : nums) {
            sqSum += Math.pow((l - mean), 2);
        }
        return Math.sqrt(sqSum / (nums.size() - 1));
    }

    @Test
    public void testNotPropertyVersusHasProperty() {
        List<Long> durations1 = new LinkedList<>();
        List<Long> durations2 = new LinkedList<>();
        int trails = 20;

        SessionImpl session = newSession(4000, 30, 10);
        System.out.println("initialised keysace");
        for (int i = 0; i < trails; i++) {
            try (TransactionOLTP tx = session.transaction().read()) {
                List<TraversalMetrics> notShardMetrics = tx.getTinkerTraversal().V().not(__.hasLabel(SHARD.name())).profile().toList();
//                List<Vertex> notShardVertices = tx.getTinkerTraversal().V().not(__.hasLabel(SHARD.name())).toList();
//            System.out.println("Number of NOT shard vertices: " + notShardVertices.size());
                System.out.println("NOT Shard vertices traversal profile: ");
                System.out.println(formatMetrics(notShardMetrics));
                durations1.add(notShardMetrics.get(0).getDuration(TimeUnit.MILLISECONDS));
            }
            confuse(session);
        }

        SessionImpl sessionr2 = newSession(4000, 30, 10);
        for (int i = 0; i < trails; i++) {
            try (TransactionOLTP tx = session.transaction().read()) {
                List<TraversalMetrics> altNotShardMetrics = tx.getTinkerTraversal().V().or(__.has(LABEL_ID.toString()), __.has(THING_TYPE_LABEL_ID.toString())).profile().toList();
//                List<Vertex> altNotShardVertices = tx.getTinkerTraversal().V().or(__.has(LABEL_ID.toString()), __.has(THING_TYPE_LABEL_ID.toString())).toList();
//            System.out.println("Alt NOT shard - number of vertices: " + altNotShardVertices.size());
                System.out.println("Alt NOT shard - traversal profile: ");
                System.out.println(formatMetrics(altNotShardMetrics));
                durations2.add(altNotShardMetrics.get(0).getDuration(TimeUnit.MILLISECONDS));
            }
        }

        double meanDuration1 = mean(durations1);
        double stddev1 = stddev(durations1);
        double meanDuration2 = mean(durations2);
        double stddev2 = stddev(durations2);

        System.out.println("Mean, stddev time using not(haslabel(SHARD)): " + meanDuration1 + ", " + stddev1);
        System.out.println("Mean, stddev time using has(LabelID) or has(THING_TYPE_LABEL_ID): " + meanDuration2 + ", " + stddev2);

    }

    //                GraphTraversal<Vertex, Map<String, Element>> test = tx.getTinkerTraversal().V()
    //                        .hasId(janusPersonId)
    //                        .in(SHARD.getLabel()).in(ISA.getLabel()).as("§x")
    //                        .as("§x")
    //                        .out(ATTRIBUTE.getLabel())
    //                        .as("§r")
    //                        .out(ISA.getLabel()).out(SHARD.getLabel())
    //                        .hasId(janusIdentifierId)
    //                        .select("§x", "§r");

    /*

    - cost of filter not(has property) vs has(property)
    - cost of native V().id(x) versus a property lookup
    - cost of using a Janus `label` versus a property
    - cost of a filter on a property on janus versus retrieving all vertices and doing filter on application level
        (similarly with filtering on 1 vs 2 properties in janus vs doing second prop on application level)
    - cost of filtering by property vs 1 edge traversal (eg. type label lookup vs 1 isa-edge traversal)
    - cost of filtering vertices on 1 property vs 2, 3, 4...
    - cost of filtering vertices based on string index vs indexed number (not accessible directly for us? could emulate with Type label id)
    - cost of writing 1, 2, 3 properties in one go
    - cost of using `as` and then referring back to prior answer sets that janus found, versus linear chains of evaluation
    - cost of `last`, which we use a lot

    - cost of using Janus-level `skip` or `range` - is this as expensive as the full retrieval and filter on our end (eye toward random selection)
    - cost of deduplicating in Janus versus retrieving all and deduplicating on our level
    - cost of janus-level `order` versus ordering in server

    - using janus `subgraph` functionality versus doing a traversal from a node connecting everything in the subgraph directly

     */
}
