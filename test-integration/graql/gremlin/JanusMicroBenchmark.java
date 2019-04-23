package grakn.core.graql.gremlin;

import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.kb.Schema;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.query.GraqlInsert;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.P;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import java.util.stream.Stream;

import static grakn.core.server.kb.Schema.BaseType.RELATION;
import static grakn.core.server.kb.Schema.EdgeLabel.SHARD;
import static grakn.core.server.kb.Schema.VertexProperty.INDEX;
import static grakn.core.server.kb.Schema.VertexProperty.LABEL_ID;
import static grakn.core.server.kb.Schema.VertexProperty.SCHEMA_LABEL;
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

    private List<String> insert(SessionImpl session, int size, int ownersPerAge, int ownersPerName) throws InterruptedException {
        int numConcurrent = 4;
        ExecutorService pool = Executors.newFixedThreadPool(numConcurrent);
        Long startTime = System.currentTimeMillis();
        List<String> insertedConceptIds = Collections.synchronizedList(new LinkedList<>());
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
                        List<ConceptMap> inserted = tx.execute(insertQuery);
                        for (Concept concept : inserted.get(0).concepts()) {
                            insertedConceptIds.add(concept.id().toString());
                        }
                        tx.commit();
                    }
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(100, TimeUnit.SECONDS);
        System.out.println("Time to populate keyspace: " + (System.currentTimeMillis() - startTime)/1000 + " seconds");
        return insertedConceptIds;
    }

    private Map<String, String> getIdNameMap(SessionImpl session) {
        Map<String, String> idNameMap = new HashMap<>();
        try (TransactionOLTP tx = session.transaction().read()) {
            List<ConceptMap> answer = tx.execute(Graql.match(var("x").isa("name")).get());
            for (ConceptMap map : answer) {
                idNameMap.put(map.get("x").id().toString(), map.get("x").asAttribute().value().toString());
            }
        }
        return idNameMap;
    }

    private SessionImpl newSession() {
        SessionImpl session = server.sessionWithNewKeyspace();
        defineSchema(session);
        return session;
    }

    private List<String> populateKeypsace(SessionImpl session, int initialSize, int ownersPerAge, int ownersPerName) {
        try {
            return insert(session, initialSize, ownersPerAge, ownersPerName);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }
//
//    private List<ConceptMap> traversalToAnswers(GraphTraversal<Vertex, Map<String, Element>> traversal, TransactionOLTP tx, Set<Variable> vars) {
//        return traversal
//                .toStream()
//                .map(elements -> createAnswer(vars, elements, tx))
//                .distinct()
//                .sequential()
//                .map(ConceptMap::new)
//                .collect(Collectors.toList());
//    }
//
//    private Map<Variable, Concept> createAnswer(Set<Variable> vars, Map<String, Element> elements, TransactionOLTP tx) {
//        Map<Variable, Concept> map = new HashMap<>();
//        for (Variable var : vars) {
//            Element element = elements.get(var.symbol());
//            if (element == null) {
//                throw GraqlQueryException.unexpectedResult(var);
//            } else {
//                Concept result;
//                if (element instanceof Vertex) {
//                    result = tx.buildConcept((Vertex) element);
//                } else {
//                    result = tx.buildConcept((Edge) element);
//                }
//                Concept concept = result;
//                map.put(var, concept);
//            }
//        }
//        return map;
//    }

//    public String formatMetrics(List<TraversalMetrics> metrics) {
//        StringBuilder formatted = new StringBuilder();
//        for (TraversalMetrics m : metrics) {
//            formatted.append("Metrics: \n");
//            for (Metrics subMetrics : m.getMetrics()) {
//                formatted
//                        .append("    ")
//                        .append(subMetrics.getName())
//                        .append(" -- ")
//                        .append(subMetrics.getDuration(TimeUnit.MILLISECONDS))
//                        .append(" ms\n");
//            }
//            formatted.append("==> Total time: " + m.getDuration(TimeUnit.MILLISECONDS));
//        }
//        return formatted.toString();
//    }

    public void confuse(SessionImpl session) {
        try (TransactionOLTP tx = session.transaction().write()) {
            tx.execute(Graql.parse("match $x isa plant, has name $y; get;").asGet());
            tx.execute(Graql.parse("match $r ($x); $x isa person; get;").asGet());
            tx.execute(Graql.parse("match $p isa person; get;").asGet());
        }
    }

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
        // compute population stddev
        double mean = mean(nums);
        double sqSum = 0;
        for (long l : nums) {
            sqSum += Math.pow((l - mean), 2);
        }
        return Math.sqrt(sqSum / (nums.size() - 1));
    }

    /**
     * Cost of filter not(has property) vs has(property) in Janus
     * This is applicable to implementation of `NotInternal` fragment
     * where we apply a filter to skip the vertices that are SHARD labeled (very few in comparison to data)
     *
     * Alternative implementations:
     * 1. use not(hasLabel(shard))
     * 2. use or(has(label id), has(thing type label id))
     * 3. don't filter, retrieve all and filter on the application level (harder to do without testing as part of larger query (?)
     * that already contains other filters since retrieving all unfiltered vertices is fast)
     */
    @Test
    public void testNotLabelVersusHasTwoProperties() {
        List<Long> durations1 = new LinkedList<>();
        List<Long> durations2 = new LinkedList<>();
        List<Long> durations3 = new LinkedList<>();
        int trails = 30;

        System.out.println("Initialising and populating keyspace...");
        SessionImpl session1 = newSession();
        populateKeypsace(session1, 5000, 30, 10);
        System.out.println("Finished creating keysace...");
        for (int i = 0; i < trails; i++) {
            try (TransactionOLTP tx = session1.transaction().read()) {
                List<TraversalMetrics> notShardMetrics = tx.getTinkerTraversal().V().not(__.hasLabel(SHARD.name())).profile().toList();
                System.out.println("NOT Shard vertices traversal profile: " + notShardMetrics.get(0).getDuration(TimeUnit.MILLISECONDS));
                durations1.add(notShardMetrics.get(0).getDuration(TimeUnit.MILLISECONDS));
            }
            confuse(session1);
        }

        System.out.println("Initialising and populating keyspace...");
        SessionImpl session2 = newSession();
        populateKeypsace(session2, 5000, 30, 10);
        System.out.println("Finished creating keysace...");
        for (int i = 0; i < trails; i++) {
            try (TransactionOLTP tx = session2.transaction().read()) {
                List<TraversalMetrics> altNotShardMetrics = tx.getTinkerTraversal().V().or(__.has(LABEL_ID.toString()), __.has(THING_TYPE_LABEL_ID.toString())).profile().toList();
                System.out.println("Alt NOT shard - traversal profile: " + altNotShardMetrics.get(0).getDuration(TimeUnit.MILLISECONDS));
                durations2.add(altNotShardMetrics.get(0).getDuration(TimeUnit.MILLISECONDS));
            }
            confuse(session2);
        }

        System.out.println("Initialising and populating keyspace...");
        SessionImpl session3 = newSession();
        populateKeypsace(session3, 5000, 30, 10);
        System.out.println("Finished creating keysace...");
        for (int i = 0; i < trails; i++) {
            try (TransactionOLTP tx = session3.transaction().read()) {
                Long startTime = System.currentTimeMillis();
                Stream<Vertex> allVertices = tx.getTinkerTraversal().V().toStream();
                List<Vertex> filteredVertices = allVertices.filter(v -> !v.label().equals(SHARD.toString())).collect(Collectors.toList());
                Long endTime = System.currentTimeMillis();
                System.out.println("Manual filtering: " + (endTime-startTime));
                durations3.add(endTime - startTime);
            }
            confuse(session3);
        }

        double meanDuration1 = mean(durations1);
        double stddev1 = stddev(durations1);
        double meanDuration2 = mean(durations2);
        double stddev2 = stddev(durations2);
        double meanDuration3 = mean(durations3);
        double stddev3 = stddev(durations3);

        System.out.println("Mean, stddev time using not(haslabel(SHARD)): " + meanDuration1 + ", " + stddev1);
        System.out.println("Mean, stddev time using has(LabelID) or has(THING_TYPE_LABEL_ID): " + meanDuration2 + ", " + stddev2);
        System.out.println("Mean, stddev time using V() and manual filter: " + meanDuration3 + ", " + stddev3);
    }


    /**
     * Cost of looking up batches of native V().id(x) versus property lookup
     */
    @Test
    public void testBatchIdVersusBatchPropertyLookup() {
        List<Long> durations1 = new LinkedList<>();
        List<Long> durations2 = new LinkedList<>();
        int trails = 30;

        System.out.println("Initialising and populating keyspace...");
        SessionImpl session1 = newSession();
        populateKeypsace(session1, 8000, 5, 2);
        Map<String, String> idNameMap = getIdNameMap(session1);
        List<String> indexedNames = idNameMap.values().stream().map(name -> Schema.BaseType.ATTRIBUTE.name() + "-name-" + name).collect(Collectors.toList());
        System.out.println("Finished creating keysace...");
        for (int i = 0; i < trails; i++) {
            try (TransactionOLTP tx = session1.transaction().read()) {
                List<TraversalMetrics> verticesByProperty = tx.getTinkerTraversal().V().has(INDEX.toString(), P.within(indexedNames)).profile().toList();
                System.out.println("Batch vertex by has(within(Attr)): " + verticesByProperty.get(0).getDuration(TimeUnit.MILLISECONDS));
                durations1.add(verticesByProperty.get(0).getDuration(TimeUnit.MILLISECONDS));
            }
            confuse(session1);
        }

        System.out.println("Initialising and populating keyspace...");
        SessionImpl session2 = newSession();
        populateKeypsace(session2, 8000, 5, 2);
        Map<String, String> idNameMap2 = getIdNameMap(session2);
        List<String> janusIds = idNameMap2.keySet().stream().map(id -> Schema.elementId(ConceptId.of(id))).collect(Collectors.toList());
        System.out.println("Finished creating keysace...");
        for (int i = 0; i < trails; i++) {
            try (TransactionOLTP tx = session2.transaction().read()) {
                List<TraversalMetrics> altNotShardMetrics = tx.getTinkerTraversal().V(janusIds).profile().toList();
                System.out.println("Batch vertex by Janus IDs retrieval: " + altNotShardMetrics.get(0).getDuration(TimeUnit.MILLISECONDS));
                durations2.add(altNotShardMetrics.get(0).getDuration(TimeUnit.MILLISECONDS));
            }
            confuse(session2);
        }

        double meanDuration1 = mean(durations1);
        double stddev1 = stddev(durations1);
        double meanDuration2 = mean(durations2);
        double stddev2 = stddev(durations2);

        System.out.println("Mean, stddev time to batch retrieve using has(INDEX, within(attribute-name-1, ...): " + meanDuration1 + ", " + stddev1);
        System.out.println("Mean, stddev time to batch retrieve using id(id1, id2...): " + meanDuration2 + ", " + stddev2);
    }

    /**
     * Cost of looking up sequences of native V().id(x) versus property lookup
     */
    @Test
    public void testSequenceOfIdVersusPropertyLookup() {
        List<Long> durations1 = new LinkedList<>();
        List<Long> durations2 = new LinkedList<>();
        int trails = 20;

        System.out.println("Initialising and populating keyspace...");
        SessionImpl session1 = newSession();
        populateKeypsace(session1, 8000, 5, 2);
        Map<String, String> idNameMap = getIdNameMap(session1);
        List<String> indexedNames = idNameMap.values().stream().map(name -> Schema.BaseType.ATTRIBUTE.name() + "-name-" + name).collect(Collectors.toList());
        System.out.println("Finished creating keysace...");
        for (int i = 0; i < trails; i++) {
            try (TransactionOLTP tx = session1.transaction().read()) {
                Long startTime = System.currentTimeMillis();
                for (String nameIndex : indexedNames) {
                    List<Vertex> vertexByProperty = tx.getTinkerTraversal().V().has(INDEX.toString(), nameIndex).toList();
                }
                Long endTime = System.currentTimeMillis();
                System.out.println("Retrieve sequence of " + indexedNames.size() + " vertices by has(attr): " + (endTime - startTime));
                durations1.add(endTime - startTime);
            }
            confuse(session1);
        }

        System.out.println("Initialising and populating keyspace...");
        SessionImpl session2 = newSession();
        populateKeypsace(session2, 8000, 5, 2);
        Map<String, String> idNameMap2 = getIdNameMap(session2);
        List<String> janusIds = idNameMap2.keySet().stream().map(id -> Schema.elementId(ConceptId.of(id))).collect(Collectors.toList());
        System.out.println("Finished creating keysace...");
        for (int i = 0; i < trails; i++) {
            try (TransactionOLTP tx = session2.transaction().read()) {
                Long startTime = System.currentTimeMillis();
                for (String janusId: janusIds) {
                    List<Vertex> vertexById = tx.getTinkerTraversal().V(janusId).toList();
                }
                Long endTime = System.currentTimeMillis();
                System.out.println("Retreive sequence of " + janusIds.size() + " vertices by V(id): " + (endTime - startTime));
                durations2.add(endTime - startTime);
            }
            confuse(session2);
        }

        double meanDuration1 = mean(durations1);
        double stddev1 = stddev(durations1);
        double meanDuration2 = mean(durations2);
        double stddev2 = stddev(durations2);

        System.out.println("Mean, stddev time retrieving " + indexedNames.size() + " vertices using has(INDEX, attribute-name-x): " + meanDuration1 + ", " + stddev1);
        System.out.println("Mean, stddev time retrieving " + janusIds.size() + " vertices using V.id(id1): " + meanDuration2 + ", " + stddev2);
    }


    /**
     * Cost of using a label versus a property (one warning somewhere stated that labels are not externally indexed,
     * unlike properties)
     */
    @Test
    public void testLabelVersusProperty() {
        List<Long> durations1 = new LinkedList<>();
        List<Long> durations2 = new LinkedList<>();
        int trails = 30;

        System.out.println("Initialising and populating keyspace...");
        SessionImpl session1 = newSession();
        populateKeypsace(session1, 5000, 5, 2);
        System.out.println("Finished creating keysace...");
        for (int i = 0; i < trails; i++) {
            try (TransactionOLTP tx = session1.transaction().read()) {
                Long startTime = System.currentTimeMillis();
                List<Vertex> verticesByLabel = tx.getTinkerTraversal().V().hasLabel(RELATION.name()).toList();
                Long endTime = System.currentTimeMillis();
                System.out.println("Retrieve " + verticesByLabel.size() + " vertices by hasLabel(RELATION): " + (endTime - startTime));
                durations1.add(endTime - startTime);
            }
            confuse(session1);
        }

        System.out.println("Initialising and populating keyspace...");
        SessionImpl session2 = newSession();
        populateKeypsace(session2, 5000, 5, 2);
        System.out.println("Finished creating keysace...");
        for (int i = 0; i < trails; i++) {
            try (TransactionOLTP tx = session2.transaction().read()) {
                int relationLabelId = (int) tx.getTinkerTraversal().V().has(SCHEMA_LABEL.toString(), "someRelation").toList().get(0).property("LABEL_ID").value();

                Long startTime = System.currentTimeMillis();
                List<Vertex> verticesByProperty = tx.getTinkerTraversal().V().has("THING_TYPE_LABEL_ID", relationLabelId).toList();
                Long endTime = System.currentTimeMillis();
                System.out.println("Retrieve " + verticesByProperty.size() + " vertices by has(THING_TYPE_LABEL_ID, 22): " + (endTime - startTime));
                durations2.add(endTime - startTime);
            }
            confuse(session2);
        }

        double meanDuration1 = mean(durations1);
        double stddev1 = stddev(durations1);
        double meanDuration2 = mean(durations2);
        double stddev2 = stddev(durations2);

        System.out.println("Mean, stddev time using labels: hasLabel(RELATION): " + meanDuration1 + ", " + stddev1);
        System.out.println("Mean, stddev time using property: has(THING_TYPE_LABEL_ID, 22): " + meanDuration2 + ", " + stddev2);
    }


    /**
     * Cost of filtering vertices based on indexed int vs indexed string
     */
    @Test
    public void testStringVersusNumberIndex() {
        List<Long> durations1 = new LinkedList<>();
        List<Long> durations2 = new LinkedList<>();
        int trails = 10;

        System.out.println("Initialising and populating keyspace...");
        SessionImpl session1 = newSession();
        populateKeypsace(session1, 2000, 5, 2);
        List<Object> schemaLabels;
        try (TransactionOLTP tx = session1.transaction().read()) {
            schemaLabels = tx.getTinkerTraversal().V().has(SCHEMA_LABEL.toString()).properties(SCHEMA_LABEL.toString()).value().dedup().toList();
        }
        System.out.println("Finished creating keysace...");
        for (int i = 0; i < trails; i++) {
            try (TransactionOLTP tx = session1.transaction().read()) {
                Long startTime = System.currentTimeMillis();
                for (Object label : schemaLabels) {
                    List<Vertex> verticesByStringType = tx.getTinkerTraversal().V().has(SCHEMA_LABEL.toString(), label).toList();
                }
                Long endTime = System.currentTimeMillis();
                System.out.println("Retrieve " + schemaLabels.size() + " vertices by has(indexed string): " + (endTime - startTime));
                durations1.add(endTime - startTime);
            }
            confuse(session1);
        }

        System.out.println("Initialising and populating keyspace...");
        SessionImpl session2 = newSession();
        populateKeypsace(session2, 2000, 5, 2);
        List<Object> schemaLabelIds;
        try (TransactionOLTP tx = session1.transaction().read()) {
            schemaLabelIds = tx.getTinkerTraversal().V().has(LABEL_ID.toString()).properties(LABEL_ID.toString()).value().dedup().toList();
        }
        System.out.println("Finished creating keysace...");
        for (int i = 0; i < trails; i++) {
            try (TransactionOLTP tx = session2.transaction().read()) {
                Long startTime = System.currentTimeMillis();
                for (Object labelId : schemaLabelIds) {
                    List<Vertex> verticesByStringType = tx.getTinkerTraversal().V().has(SCHEMA_LABEL.toString(), labelId).toList();
                }
                Long endTime = System.currentTimeMillis();
                System.out.println("Retrieve " + schemaLabelIds.size() + " vertices by has(indexed int): " + (endTime - startTime));
                durations2.add(endTime - startTime);
            }
            confuse(session2);
        }

        double meanDuration1 = mean(durations1);
        double stddev1 = stddev(durations1);
        double meanDuration2 = mean(durations2);
        double stddev2 = stddev(durations2);

        System.out.println("Mean, stddev time using has(INDEX, within(attribute-name-1, ...): " + meanDuration1 + ", " + stddev1);
        System.out.println("Mean, stddev time using id(id1, id2...): " + meanDuration2 + ", " + stddev2);
    }

    /**
     * Cost of deduplicating in Janus versus retrieving all and deduplicating on our level
     */
    @Test
    public void testJanusDeduplicateVersusServerDeduplicate() {
        List<Long> durations1 = new LinkedList<>();
        List<Long> durations2 = new LinkedList<>();
        int trails = 30;

        System.out.println("Initialising and populating keyspace...");
        SessionImpl session1 = newSession();
        populateKeypsace(session1, 5000, 5, 2);
        System.out.println("Finished creating keysace...");
        for (int i = 0; i < trails; i++) {
            try (TransactionOLTP tx = session1.transaction().read()) {
                Long startTime = System.currentTimeMillis();
                List<Vertex> vertices = tx.getTinkerTraversal().V().has(INDEX.toString()).dedup(INDEX.toString()).toList();
                Long endTime = System.currentTimeMillis();
                System.out.println("Retrieve all " + vertices.size() + " deduplicated attr verticesusing .dedup(): " + (endTime - startTime));
                durations1.add(endTime - startTime);
            }
            confuse(session1);
        }

        System.out.println("Initialising and populating keyspace...");
        SessionImpl session2 = newSession();
        populateKeypsace(session2, 5000, 5, 2);
        Map<String, String> idNameMap2 = getIdNameMap(session2);
        List<String> janusIds = idNameMap2.keySet().stream().map(id -> Schema.elementId(ConceptId.of(id))).collect(Collectors.toList());
        System.out.println("Finished creating keysace...");
        for (int i = 0; i < trails; i++) {
            try (TransactionOLTP tx = session2.transaction().read()) {
                Long startTime = System.currentTimeMillis();
                List<Vertex> vertices = tx.getTinkerTraversal().V().has(INDEX.toString()).toList();
                Collection<Vertex> uniqueVerticesByAttrValue = vertices.stream()
                        .collect(Collectors.toMap(v -> v.property(INDEX.toString()).toString(), p->p, (p,q) -> p)).values();
                Long endTime = System.currentTimeMillis();
                System.out.println("Retreive all " + uniqueVerticesByAttrValue.size() + " deduplicated attr values by stream().distinct(): " + (endTime - startTime));
                durations2.add(endTime - startTime);
            }
            confuse(session2);
        }

        double meanDuration1 = mean(durations1);
        double stddev1 = stddev(durations1);
        double meanDuration2 = mean(durations2);
        double stddev2 = stddev(durations2);

        System.out.println("Mean, stddev time retrieving unique attrs via .dedup(): " + meanDuration1 + ", " + stddev1);
        System.out.println("Mean, stddev time retrieving unique attrs via stream().distinct(): " + meanDuration2 + ", " + stddev2);
    }



    @Test
    public void test() {

    }


    /*

    - time to obtain vertices with a specific indexed property and how it relates to number of matches:
        sometimes it seems matching a property with a small number of hits is much slower than a large number of hits!

    - writing a string property vs an int/long property
    - reading a string property vs an int/long property

    - cost of filtering by property vs 1 edge traversal (eg. type label lookup vs 1 isa-edge traversal)
    - cost of filtering vertices on 1 property vs 2, 3, 4...
    - cost of writing 1, 2, 3 properties in one go (writes, not reads)
    - cost of using `as` and then referring back to prior answer sets that janus found, versus linear chains of evaluation
    - cost of `last` in combination with `as`, which we use a lot

    - cost of janus-level `order` versus ordering in server

    for random sampling:
    - cost of using Janus-level `skip` or `range` - is this as expensive as the full retrieval and filter on our end
    - cost of using Janus-level `sample` (`coin` is only pseudorandom up to 200 elements rather than N apparently!) + is it effective (test distribution)

    for subgraphing
    - using janus `subgraph` functionality versus doing a traversal from a node connecting everything in the subgraph directly
    - retrieving a set of nodes with a property versus doing 1 hop from a highly connected node (best for small subgraphs)

     */
}
