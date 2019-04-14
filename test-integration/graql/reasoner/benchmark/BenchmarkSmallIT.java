/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.reasoner.benchmark;

import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.executor.QueryExecutor;
import grakn.core.graql.gremlin.GreedyTraversalPlan;
import grakn.core.graql.reasoner.DisjunctionIterator;
import grakn.core.graql.reasoner.graph.DiagonalGraph;
import grakn.core.graql.reasoner.graph.LinearTransitivityMatrixGraph;
import grakn.core.graql.reasoner.graph.PathTreeGraph;
import grakn.core.graql.reasoner.graph.TransitivityChainGraph;
import grakn.core.graql.reasoner.graph.TransitivityMatrixGraph;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.kb.concept.AttributeTypeImpl;
import grakn.core.server.kb.concept.ElementFactory;
import grakn.core.server.kb.concept.ThingImpl;
import grakn.core.server.kb.concept.TypeImpl;
import grakn.core.server.kb.structure.AbstractElement;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class BenchmarkSmallIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();


    public static long putEntityTypeTime = 0;
    public static long putAttributeTypeTime = 0;
    public static long createEntityTime = 0;
    public static long createAttributeTime = 0;
    public static long attachAttributeTime = 0;
    public static long commitTime = 0;
    public static long writeTime = 0;
    public static long insertTime = 0;
    public static long openTxTime = 0;
    public static long schemaTime = 0;

    void zeroWriteTimes(){
        putEntityTypeTime = 0;
        putAttributeTypeTime = 0;
        createEntityTime = 0;
        createAttributeTime = 0;
        attachAttributeTime = 0;
        commitTime = 0;
        writeTime = 0;
    }


    public void printWriteTimes(){
        /*
        System.out.println();
        System.out.println("putEntityTypeTime: " + putEntityTypeTime);
        System.out.println("putAttributeTypeTime: " + putAttributeTypeTime);
        System.out.println("createEntityTime: " + createEntityTime);
        System.out.println("createAttributeTime: " + createAttributeTime);
        System.out.println("    AttributeTypeImpl.fetchAttributeTime: " + AttributeTypeImpl.fetchAttributeTime);
        System.out.println("attachAttributeTime: " + attachAttributeTime);
        System.out.println();
*/


        System.out.println("addInstance::instanceAdditions: " + TypeImpl.instanceAdditions);
        System.out.println("addInstance::preCheckTime: " + TypeImpl.preCheckTime);
        System.out.println("addInstance::cacheTime: " + TypeImpl.cacheCheckTime);
        System.out.println("addInstance::addVertexElementTime: " + TypeImpl.addVertexElementTime);
        //System.out.println("    ElementFactory.addVertexTime: " + ElementFactory.addVertexTime);
        //System.out.println("    ElementFactory.assignVertexIdTime: " + ElementFactory.assignVertexIdTime);
        //System.out.println("    ElementFactory.vertexAdditions: " + ElementFactory.vertexAdditions);
        System.out.println("addInstance::produceInstanceTime: " + TypeImpl.produceInstanceTime);
        //System.out.println("    ThingImpl.setTypeTime: " + ThingImpl.setTypeTime);
        //System.out.println("    AbstractElement.getPropertyTime: " + AbstractElement.getPropertyTime);
        System.out.println();


        System.out.println("openTxTime: " + openTxTime);
        System.out.println("schemaTime: " + schemaTime);
        System.out.println("commitTime: " + commitTime);
       // System.out.println("    validateTime: " + TransactionOLTP.validationTime);
       // System.out.println("    cacheFlushTime: " + TransactionOLTP.cacheFlushTime);
        System.out.println("insertTime: " + insertTime);
        System.out.println("writeTime: " + writeTime);
        System.out.println();
    }


    private SessionImpl prepareSession(int N){
        SessionImpl session = server.sessionWithNewKeyspace();

        long start0 = System.currentTimeMillis();
        try(TransactionOLTP tx = session.transaction().write()) {
            AttributeType<String> attributeType = tx.putAttributeType("identifier", AttributeType.DataType.STRING);
            tx.putEntityType("person").has(attributeType);
            tx.commit();
        }
        schemaTime += System.currentTimeMillis() - start0;
        int commitPeriod = 100;
        TransactionOLTP tx = session.transaction().write();
        for (int i = 1 ; i <= N ; i++){
            long start = System.currentTimeMillis();
            long insertStart = System.currentTimeMillis();

            AttributeType<String> attributeType = tx.getAttributeType("identifier");
            putAttributeTypeTime += System.currentTimeMillis() - start;

            start = System.currentTimeMillis();
            EntityType personType = tx.getEntityType("person");
            putEntityTypeTime += System.currentTimeMillis() - start;

            start = System.currentTimeMillis();
            Entity person = personType.create();
            createEntityTime += System.currentTimeMillis() - start;

            start = System.currentTimeMillis();
            Attribute<String> attribute = attributeType.create(String.valueOf(i));
            createAttributeTime += System.currentTimeMillis() - start;

            start = System.currentTimeMillis();
            person.has(attribute);
            attachAttributeTime += System.currentTimeMillis() - start;
            insertTime += System.currentTimeMillis() - insertStart;

            start = System.currentTimeMillis();
            if (i % commitPeriod == 0 || i == N){
                tx.commit();
                tx.close();
                if ( i != N){
                    long start2 = System.currentTimeMillis();
                    tx = session.transaction().write();
                    openTxTime += System.currentTimeMillis() - start2;
                }
            }
            commitTime += System.currentTimeMillis() - start;
        }
        writeTime += System.currentTimeMillis() - start0;
        return session;
    }
    @Test
    public void testAttributes() {
        final int N = 10000;

        Pattern pattern = Graql.parsePattern("{$x isa person; $x has identifier $r;};");
        //Pattern pattern = Graql.parsePattern("{$x isa person;};");
        Set<Variable> vars = pattern.variables();

        try(SessionImpl session = prepareSession(N)) {
            try (TransactionOLTP tx = session.transaction().read()) {
                printWriteTimes();
                //GraqlTraversal traversal = GreedyTraversalPlan.createTraversal(pattern, tx);
                //System.out.println("traversal:\n" + traversal.fragments());
                //System.out.println("traversal:\n" + traversal.getGraphTraversal(tx, vars));

                //generic query

                long start = System.currentTimeMillis();
                List<ConceptMap> answers = tx.execute(Graql.match(pattern), false);
                System.out.println("exec time via graql; " + (System.currentTimeMillis() - start));
                /*
                start = System.currentTimeMillis();
                tx.getEntityType("person").instances()
                .forEach(person -> {
                            Conjunction<?> idPattern = Graql.and(pattern, Graql.var("x").id(person.id().getValue()));
                            List<ConceptMap> ans = tx.execute(Graql.match(idPattern), false);
                            assertEquals(1, ans.size());
                        }
                        );
                System.out.println("id exec time via graql; " + (System.currentTimeMillis() - start));

                printTimes();
                */
                assertEquals(N, answers.size());
            }
        }
/*
        try(SessionImpl session = prepareSession(N)) {
            try (TransactionOLTP tx = session.transaction().read()) {

                zeroTimes();

                GraqlTraversal traversal = GreedyTraversalPlan.createTraversal(pattern, tx);

                long start = System.currentTimeMillis();
                List<ConceptMap> answers =  traversalToAnswers(traversal.getGraphTraversal(tx, vars), tx, vars);
                System.out.println("exec time from graql traversal; " + (System.currentTimeMillis() - start));
                printTimes();
                assertEquals(N, answers.size());
            }
        }


        try(SessionImpl session = prepareSession(N)) {
            try (TransactionOLTP tx = session.transaction().read()) {
                long start = System.currentTimeMillis();

                zeroTimes();
                GraphTraversal<Vertex, Map<String, Element>> test = tx.getTinkerTraversal().V()
                        .has(Schema.VertexProperty.LABEL_ID.name(), tx.convertToId(Label.of("person")).getValue())
                        .in(SHARD.getLabel()).in(ISA.getLabel()).as("§x")
                        .out(ATTRIBUTE.getLabel()).as("§r")
                        .out(ISA.getLabel()).out(SHARD.getLabel())
                        .has(Schema.VertexProperty.LABEL_ID.name(), tx.convertToId(Label.of("identifier")).getValue())
                        .select("§x", "§r");

                System.out.println("manual traversal:\n" + test);
                List<ConceptMap> answers = traversalToAnswers(test, tx, vars);

                System.out.println("shortcut-edge-optimised time; " + (System.currentTimeMillis() - start));
                printTimes();

                assertEquals(N, answers.size());
            }
        }

        try(SessionImpl session = prepareSession(N)) {
            try (TransactionOLTP tx = session.transaction().read()) {
                long start = System.currentTimeMillis();

                zeroTimes();

                String typeName = Schema.VertexProperty.THING_TYPE_LABEL_ID.name();
                Label person = Label.of("person");
                Label identifier = Label.of("identifier");
                Integer personId = tx.convertToId(person).getValue();
                Integer identifierId = tx.convertToId(identifier).getValue();

                GraphTraversal<Vertex, Map<String, Element>> test = tx.getTinkerTraversal().V()
                        .has(typeName, personId)
                        .as("§x")
                        .out(ATTRIBUTE.getLabel())
                        .as("§r")
                        .has(typeName, identifierId)
                        .select("§x", "§r");
                List<ConceptMap> answers = traversalToAnswers(test, tx, vars);

                System.out.println("isa-optimised time; " + (System.currentTimeMillis() - start));
                printTimes();

                assertEquals(N, answers.size());
            }
        }

        try(SessionImpl session = prepareSession(N)) {
            try (TransactionOLTP tx = session.transaction().read()) {
                long start = System.currentTimeMillis();

                zeroTimes();

                String typeName = Schema.VertexProperty.THING_TYPE_LABEL_ID.name();
                Label person = Label.of("person");
                Label identifier = Label.of("identifier");
                //Integer personId = tx.convertToId(Label.of("person")).getValue();
                //Integer identifierId = tx.convertToId(Label.of("identifier")).getValue();
                ConceptId personId = tx.getSchemaConcept(person).id();
                ConceptId identifierId = tx.getSchemaConcept(identifier).id();
                Long janusPersonId = Long.valueOf(personId.getValue().replace("V", ""));
                Long janusIdentifierId = Long.valueOf(identifierId.getValue().replace("V", ""));

                GraphTraversal<Vertex, Map<String, Element>> test = tx.getTinkerTraversal().V()
                        .hasId(janusPersonId)
                        .in(SHARD.getLabel()).in(ISA.getLabel()).as("§x")
                        .as("§x")
                        .out(ATTRIBUTE.getLabel())
                        .as("§r")
                        .out(ISA.getLabel()).out(SHARD.getLabel())
                        .hasId(janusIdentifierId)
                        .select("§x", "§r");
                List<ConceptMap> answers = traversalToAnswers(test, tx, vars);

                System.out.println("janus-id optimised time; " + (System.currentTimeMillis() - start));
                printTimes();

                assertEquals(N, answers.size());
            }
        }
        */
/*

        try(SessionImpl session = prepareSession(N)) {
            try (TransactionOLTP tx = session.transaction().read()) {
                long start = System.currentTimeMillis();
                GraphTraversal<Vertex, Vertex> test = tx.getTinkerTraversal().V()
                        //.in(ATTRIBUTE.getLabel())
                        .has(Schema.VertexProperty.THING_TYPE_LABEL_ID.name())
                        .as("§x")
                        .select("§x");
                List<Concept> answers = test.toStream()
                        .map(vertex -> (Concept) tx.buildConcept(vertex))
                        .collect(Collectors.toList());

                System.out.println("get all vertices time; " + (System.currentTimeMillis() - start));
                printTimes();

                assertEquals(2*N, answers.size());
            }
        }

        try(SessionImpl session = prepareSession(N)) {
            try (TransactionOLTP tx = session.transaction().read()) {
                long start = System.currentTimeMillis();
                GraphTraversal<Vertex, Vertex> test = tx.getTinkerTraversal().V()
                        //.in(ATTRIBUTE.getLabel())
                        //.has(Schema.VertexProperty.THING_TYPE_LABEL_ID.name())
                        .as("§x")
                        .select("§x");
                List<Vertex> answers = test.toStream()
                        .filter(v -> v.property(Schema.VertexProperty.THING_TYPE_LABEL_ID.name()) != null)
                        .collect(Collectors.toList());

                System.out.println("get all vertices time; " + (System.currentTimeMillis() - start));

                assertEquals(2*N, answers.size());
            }
        }
        */
    }

    /*
    private void zeroTimes(){
        zeroWriteTimes();
        DisjunctionIterator.resolvabilityCheckTime = 0;
        QueryExecutor.validateClauseTime = 0;
        GreedyTraversalPlan.planTime = 0;
        TransactionOLTP.buildConceptTime = 0;
        ElementFactory.buildVertexElementTime = 0;
        ElementFactory.buildConceptFromVElementTime = 0;
        ElementFactory.baseTypeRetrieveTime = 0;
        ElementFactory.baseTypeValueOfTime = 0;
        ElementFactory.baseTypeCalls = 0;
        ElementFactory.getCachedConceptTime = 0;
        ElementFactory.getVertexLabel = 0;
        ElementFactory.getConceptIdTime = 0;
    }

    private void printTimes(){
        printWriteTimes();

        System.out.println("DisjunctionIterator.resolvabilityCheckTime: " + DisjunctionIterator.resolvabilityCheckTime);
        System.out.println("QueryExecutor.validateClauseTime: " + QueryExecutor.validateClauseTime);
        System.out.println("GreedyTraversalPlan.planTime: " + GreedyTraversalPlan.planTime);
        System.out.println("TransactionOLTP.buildConceptTime: " + TransactionOLTP.buildConceptTime);
        System.out.println("ElementFactory.buildVertexElementTime: " + ElementFactory.buildVertexElementTime);
        System.out.println("ElementFactory.buildConceptFromVertexTime: " + ElementFactory.buildConceptFromVertexTime);
        System.out.println("ElementFactory.buildConceptFromVElementTime: " + ElementFactory.buildConceptFromVElementTime);
        System.out.println("ElementFactory.getConceptIdTime: " + ElementFactory.getConceptIdTime);
        System.out.println("ElementFactory.getVertexLabel: " + ElementFactory.getVertexLabel);
        System.out.println("ElementFactory.getCachedConceptTime: " + ElementFactory.getCachedConceptTime);
        System.out.println("ElementFactory.baseTypeRetrieveTime: " + ElementFactory.baseTypeRetrieveTime);
        System.out.println("ElementFactory.baseTypeValueOfTime: " + ElementFactory.baseTypeValueOfTime);
        System.out.println("ElementFactory.baseTypeCalls: " + ElementFactory.baseTypeCalls);
        System.out.println();
    }
    */

    private List<ConceptMap> traversalToAnswers(GraphTraversal<Vertex, Map<String, Element>> traversal, TransactionOLTP tx, Set<Variable> vars){
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


    /**
     * Executes a scalability test defined in terms of the number of rules in the system. Creates a simple rule chain:
     *
     * R_i(x, y) := R_{i-1}(x, y);     i e [1, N]
     *
     * with a single initial relation instance R_0(a ,b)
     *
     */
    @Test
    public void nonRecursiveChainOfRules() {
        final int N = 200;
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName());
        SessionImpl session = server.sessionWithNewKeyspace();

        //NB: loading data here as defining it as KB and using graql api leads to circular dependencies
        try(TransactionOLTP tx = session.transaction().write()) {
            Role fromRole = tx.putRole("fromRole");
            Role toRole = tx.putRole("toRole");

            RelationType relation0 = tx.putRelationType("relation0")
                    .relates(fromRole)
                    .relates(toRole);

            for (int i = 1; i <= N; i++) {
                tx.putRelationType("relation" + i)
                        .relates(fromRole)
                        .relates(toRole);
            }
            EntityType genericEntity = tx.putEntityType("genericEntity")
                    .plays(fromRole)
                    .plays(toRole);

            Entity fromEntity = genericEntity.create();
            Entity toEntity = genericEntity.create();

            relation0.create()
                    .assign(fromRole, fromEntity)
                    .assign(toRole, toEntity);

            for (int i = 1; i <= N; i++) {
                Statement fromVar = new Statement(new Variable().asUserDefined());
                Statement toVar = new Statement(new Variable().asUserDefined());
                Statement rulePattern = Graql
                        .type("rule" + i)
                        .when(
                                Graql.and(
                                        Graql.var()
                                                .rel(Graql.type(fromRole.label().getValue()), fromVar)
                                                .rel(Graql.type(toRole.label().getValue()), toVar)
                                                .isa("relation" + (i - 1))
                                )
                        )
                        .then(
                                Graql.and(
                                        Graql.var()
                                                .rel(Graql.type(fromRole.label().getValue()), fromVar)
                                                .rel(Graql.type(toRole.label().getValue()), toVar)
                                                .isa("relation" + i)
                                )
                        );
                tx.execute(Graql.define(rulePattern));
            }
            tx.commit();
        }

        try( TransactionOLTP tx = session.transaction().read()) {
            final long limit = 1;
            String queryPattern = "(fromRole: $x, toRole: $y) isa relation" + N + ";";
            String queryString = "match " + queryPattern + " get;";
            String limitedQueryString = "match " + queryPattern +
                    "get; limit " + limit +  ";";

            assertEquals(executeQuery(queryString, tx, "full").size(), limit);
            assertEquals(executeQuery(limitedQueryString, tx, "limit").size(), limit);
        }
        session.close();
    }

    /**
     * 2-rule transitive test with transitivity expressed in terms of two linear rules
     * The rules are defined as:
     *
     * (Q-from: $x, Q-to: $y) isa Q;
     * ->
     * (P-from: $x, P-to: $y) isa P;
     *
     * (Q-from: $x, Q-to: $z) isa Q;
     * (P-from: $z, P-to: $y) isa P;
     * ->
     * (P-from: $z, P-to: $y) isa P;
     *
     * Each pair of neighbouring grid points is related in the following fashion:
     *
     *  a_{i  , j} -  Q  - a_{i, j + 1}
     *       |                    |
     *       Q                    Q
     *       |                    |
     *  a_{i+1, j} -  Q  - a_{i+1, j+1}
     *
     *  i e [1, N]
     *  j e [1, N]
     */
    @Test
    public void testTransitiveMatrixLinear()  {
        int N = 10;
        int limit = 100;
        SessionImpl session = server.sessionWithNewKeyspace();
        LinearTransitivityMatrixGraph linearGraph = new LinearTransitivityMatrixGraph(session);

        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName());

        //                         DJ       IC     FO
        //results @N = 15 14400   3-5s
        //results @N = 20 44100    15s     8 s      8s
        //results @N = 25 105625   48s    27 s     31s
        //results @N = 30 216225  132s    65 s
        linearGraph.load(N, N);

        String queryString = "match (P-from: $x, P-to: $y) isa P; get;";
        TransactionOLTP tx = session.transaction().write();
        executeQuery(queryString, tx, "full");
        executeQuery(Graql.parse(queryString).asGet().match().get().limit(limit), tx, "limit " + limit);
        tx.close();
        session.close();
    }

    /**
     * single-rule transitivity test with initial data arranged in a chain of length N
     * The rule is given as:
     *
     * (Q-from: $x, Q-to: $z) isa Q;
     * (Q-from: $z, Q-to: $y) isa Q;
     * ->
     * (Q-from: $x, Q-to: $y) isa Q;
     *
     * Each neighbouring grid points are related in the following fashion:
     *
     *  a_{i} -  Q  - a_{i + 1}
     *
     *  i e [0, N)
     */
    @Test
    public void testTransitiveChain()  {
        int N = 400;
        int limit = 10;
        int answers = (N+1)*N/2;
        SessionImpl session = server.sessionWithNewKeyspace();
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName());
        TransitivityChainGraph transitivityChainGraph = new TransitivityChainGraph(session);
        transitivityChainGraph.load(N);
        TransactionOLTP tx = session.transaction().write();

        //String queryString = "match (Q-from: $x, Q-to: $y) isa Q; get;";
        //GraqlGet query = Graql.parse(queryString).asGet();

        String queryString2 = "match (Q-from: $x, Q-to: $y) isa Q;$x has index 'a'; get;";
        GraqlGet query2 = Graql.parse(queryString2).asGet();

        //assertEquals(executeQuery(query, tx, "full").size(), answers);
        assertEquals(executeQuery(query2, tx, "With specific resource").size(), N);

        //executeQuery(query.match().get().limit(limit), tx, "limit " + limit);
        executeQuery(query2.match().get().limit(limit), tx, "limit " + limit);
        tx.close();
        session.close();
    }

    /**
     * single-rule transitivity test with initial data arranged in a N x N square grid.
     * The rule is given as:
     *
     * (Q-from: $x, Q-to: $z) isa Q;
     * (Q-from: $z, Q-to: $y) isa Q;
     * ->
     * (Q-from: $x, Q-to: $y) isa Q;
     *
     * Each pair of neighbouring grid points is related in the following fashion:
     *
     *  a_{i  , j} -  Q  - a_{i, j + 1}
     *       |                    |
     *       Q                    Q
     *       |                    |
     *  a_{i+1, j} -  Q  - a_{i+1, j+1}
     *
     *  i e [0, N)
     *  j e [0, N)
     */
    @Test
    public void testTransitiveMatrix(){
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName());
        int N = 10;
        int limit = 100;

        SessionImpl session = server.sessionWithNewKeyspace();
        TransitivityMatrixGraph transitivityMatrixGraph = new TransitivityMatrixGraph(session);
        //                         DJ       IC     FO
        //results @N = 15 14400     ?
        //results @N = 20 44100     ?       ?     12s     4 s
        //results @N = 25 105625    ?       ?     50s    11 s
        //results @N = 30 216225    ?       ?      ?     30 s
        //results @N = 35 396900   ?        ?      ?     76 s
        transitivityMatrixGraph.load(N, N);
        TransactionOLTP tx = session.transaction().write();


        //full result
        String queryString = "match (Q-from: $x, Q-to: $y) isa Q; get;";
        GraqlGet query = Graql.parse(queryString).asGet();

        //with specific resource
        String queryString2 = "match (Q-from: $x, Q-to: $y) isa Q;$x has index 'a'; get;";
        GraqlGet query2 = Graql.parse(queryString2).asGet();

        //with substitution
        Concept id = tx.execute(Graql.parse("match $x has index 'a'; get;").asGet()).iterator().next().get("x");
        String queryString3 = "match (Q-from: $x, Q-to: $y) isa Q;$x id '" + id.id().getValue() + "'; get;";
        GraqlGet query3 = Graql.parse(queryString3).asGet();

        executeQuery(query, tx, "full");
        executeQuery(query2, tx, "With specific resource");
        executeQuery(query3, tx, "Single argument bound");
        executeQuery(query.match().get().limit(limit), tx, "limit " + limit);
        tx.close();
        session.close();
    }

    /**
     * single-rule mimicking transitivity test rule defined by two-hop relations
     * Initial data arranged in N x N square grid.
     *
     * Rule:
     * (rel-from:$x, rel-to:$y) isa horizontal;
     * (rel-from:$y, rel-to:$z) isa horizontal;
     * (rel-from:$z, rel-to:$u) isa vertical;
     * (rel-from:$u, rel-to:$v) isa vertical;
     * ->
     * (rel-from:$x, rel-to:$v) isa diagonal;
     *
     * Initial data arranged as follows:
     *
     *  a_{i  , j} -  horizontal  - a_{i, j + 1}
     *       |                    |
     *    vertical             vertical
     *       |                    |
     *  a_{i+1, j} -  horizontal  - a_{i+1, j+1}
     *
     *  i e [0, N)
     *  j e [0, N)
     */
    @Test
    public void testDiagonal()  {
        int N = 10; //9604
        int limit = 10;

        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName());

        SessionImpl session = server.sessionWithNewKeyspace();
        DiagonalGraph diagonalGraph = new DiagonalGraph(session);
        diagonalGraph.load(N, N);
        //results @N = 40  1444  3.5s
        //results @N = 50  2304    8s    / 1s
        //results @N = 100 9604  loading takes ages
        TransactionOLTP tx = session.transaction().write();

        String queryString = "match (rel-from: $x, rel-to: $y) isa diagonal; get;";
        GraqlGet query = Graql.parse(queryString).asGet();

        executeQuery(query, tx, "full");
        executeQuery(query.match().get().limit(limit), tx, "limit " + limit);
        tx.close();
        session.close();
    }

    /**
     * single-rule mimicking transitivity test rule defined by two-hop relations
     * Initial data arranged in N x N square grid.
     *
     * Rules:
     * (arc-from: $x, arc-to: $y) isa arc;},
     * ->
     * (path-from: $x, path-to: $y) isa path;};

     *
     * (path-from: $x, path-to: $z) isa path;
     * (path-from: $z, path-to: $y) isa path;},
     * ->
     * (path-from: $x, path-to: $y) isa path;};
     *
     * Initial data arranged as follows:
     *
     * N - tree heights
     * l - number of links per entity
     *
     *                     a0
     *               /     .   \
     *             arc          arc
     *             /       .       \
     *           a1,1     ...    a1,1^l
     *         /   .  \         /    .  \
     *       arc   .  arc     arc    .  arc
     *       /     .   \       /     .    \
     *     a2,1 ...  a2,l  a2,l+1  ...  a2,2^l
     *            .             .
     *            .             .
     *            .             .
     *   aN,1    ...  ...  ...  ...  ... ... aN,N^l
     *
     */
    @Test
    public void testPathTree(){
        int N = 5;
        int linksPerEntity = 4;
        System.out.println(new Object(){}.getClass().getEnclosingMethod().getName());
        SessionImpl session = server.sessionWithNewKeyspace();
        PathTreeGraph pathTreeGraph = new PathTreeGraph(session);
        pathTreeGraph.load(N, linksPerEntity);
        int answers = 0;
        for(int i = 1 ; i <= N ; i++) answers += Math.pow(linksPerEntity, i);

        TransactionOLTP tx = session.transaction().write();

        String queryString = "match (path-from: $x, path-to: $y) isa path;" +
                "$x has index 'a0';" +
                "get $y; limit " + answers + ";";

        assertEquals(executeQuery(queryString, tx, "tree").size(), answers);
        tx.close();
        session.close();
    }

    private List<ConceptMap> executeQuery(String queryString, TransactionOLTP transaction, String msg){
        return executeQuery(Graql.parse(queryString).asGet(), transaction, msg);
    }

    private List<ConceptMap> executeQuery(GraqlGet query, TransactionOLTP transaction, String msg){
        final long startTime = System.currentTimeMillis();
        List<ConceptMap> results = transaction.execute(query);
        final long answerTime = System.currentTimeMillis() - startTime;
        System.out.println(msg + " results = " + results.size() + " answerTime: " + answerTime);
        return results;
    }

}