package grakn.core.graql.gremlin;

import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.query.GraqlInsert;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.server.kb.Schema.EdgeLabel.ATTRIBUTE;
import static grakn.core.server.kb.Schema.EdgeLabel.ISA;
import static grakn.core.server.kb.Schema.EdgeLabel.SHARD;
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
    public static final GraknTestServer server = new GraknTestServer();

    private static void defineSchema(SessionImpl session) {
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

    private static String randomString() {
        return java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    }

    private static Long randomLong() {
        Random random = new Random();
        return random.nextLong();
    }

    private static void insert(SessionImpl session, int size, int ownersPerAge, int ownersPerName) {
        String nameAttr = "";
        long ageAttr = 0;
        for (int i = 0; i < size; i++) {
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
    }

    private static SessionImpl newSession(int initialSize, int ownersPerAge, int ownersPerName) {
        SessionImpl session = server.sessionWithNewKeyspace();
        defineSchema(session);
        insert(session, initialSize, ownersPerAge, ownersPerName);
        return session;
    }

    private static List<ConceptMap> traversalToAnswers(GraphTraversal<Vertex, Map<String, Element>> traversal, TransactionOLTP tx, Set<Variable> vars) {
        return traversal
                .toStream()
                .map(elements -> createAnswer(vars, elements, tx))
                .distinct()
                .sequential()
                .map(ConceptMap::new)
                .collect(Collectors.toList());
    }

    private static Map<Variable, Concept> createAnswer(Set<Variable> vars, Map<String, Element> elements, TransactionOLTP tx) {
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

    @Test
    public static void testNotPropertyVersusHasProperty() {
        SessionImpl session = newSession(2000, 50, 25);

        try (TransactionOLTP tx = session.transaction().read()) {
            GraphTraversal<Vertex, Vertex> traversal = tx.getTinkerTraversal().V()
                    .has(SHARD.name());
            traversal.toStream().collect(Collectors.toList());
        }


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
    - cost of a filter on a property on janus versus retrieving all vertices and doing filter on application level
        (similarly with filtering on 1 vs 2 properties in janus vs doing second prop on application level)
    - cost of filtering by property vs 1 edge traversal (eg. type label lookup vs 1 isa-edge traversal)
    - cost of filtering vertices on 1 property vs 2, 3, 4...
    - cost of filtering vertices based on string index vs indexed number (not accessible directly for us? could emulate with Type label id)
    - cost of writing 1, 2, 3 properties in one go
    - cost of using `as` and then referring back to prior answer sets that janus found, versus linear chains of evaluation
    - cost of `last`, which we use a lot

     */
}
