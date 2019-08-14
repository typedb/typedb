/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graql.reasoner.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.Rule;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.graph.GeoGraph;
import grakn.core.graql.reasoner.rule.InferenceRule;
import grakn.core.graql.reasoner.rule.RuleUtils;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import static graql.lang.Graql.type;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@SuppressWarnings("CheckReturnValue")
public class QueryIT {
    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl geoSession;

    @BeforeClass
    public static void loadContext(){
        geoSession = server.sessionWithNewKeyspace();
        GeoGraph geoGraph = new GeoGraph(geoSession);
        geoGraph.load();
    }

    @AfterClass
    public static void closeSession(){
        geoSession.close();
    }

    @Test
    public void whenTypeDependencyGraphHasCycles_RuleBodiesHaveTypeHierarchies_weReiterate(){
        try (SessionImpl session = server.sessionWithNewKeyspace()) {
            try (TransactionOLTP tx = session.transaction().write()) {

                Role someRole = tx.putRole("someRole");
                EntityType genericEntity = tx.putEntityType("genericEntity")
                        .plays(someRole);

                Entity entity = genericEntity.create();
                Entity anotherEntity = genericEntity.create();
                Entity yetAnotherEntity = genericEntity.create();

                RelationType someRelation = tx.putRelationType("someRelation")
                        .relates(someRole);

                someRelation.create()
                        .assign(someRole, entity)
                        .assign(someRole, anotherEntity);

                RelationType inferredBase = tx.putRelationType("inferredBase")
                        .relates(someRole);

                inferredBase.create()
                        .assign(someRole, anotherEntity)
                        .assign(someRole, yetAnotherEntity);

                tx.putRelationType("inferred")
                        .relates(someRole).sup(inferredBase);

                tx.putRule("rule1",
                        Graql.parsePattern(
                                "{" +
                                        "($x, $y) isa someRelation; " +
                                        "($y, $z) isa inferredBase;" +
                                        "};"
                        ),
                        Graql.parsePattern("{ (someRole: $x, someRole: $z) isa inferred; };"));

                tx.commit();
            }
            try (TransactionOLTP tx = session.transaction().write()) {
                String patternString = "{ ($x, $y) isa inferred; };";
                ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, tx), tx);
                Set<InferenceRule> rules = tx.ruleCache().getRules().map(r -> new InferenceRule(r, tx)).collect(toSet());

                //with cache empty no loops are found
                assertFalse(RuleUtils.subGraphIsCyclical(rules, tx));
                assertFalse(query.requiresReiteration());
                query.resolve().collect(Collectors.toList());

                //with populated cache we find a loop
                assertTrue(RuleUtils.subGraphIsCyclical(rules, tx));
            }
        }
    }

    @Test
    public void whenTypeDependencyGraphHasCycles_instancesHaveNonTrivialCycles_weReiterate(){
        try (SessionImpl session = server.sessionWithNewKeyspace()) {
            try (TransactionOLTP tx = session.transaction().write()) {
                Role fromRole = tx.putRole("fromRole");
                Role toRole = tx.putRole("toRole");
                EntityType someEntity = tx.putEntityType("someEntity")
                        .plays(fromRole)
                        .plays(toRole);

                RelationType someRelation = tx.putRelationType("someRelation")
                        .relates(fromRole)
                        .relates(toRole);
                RelationType transRelation = tx.putRelationType("transRelation")
                        .relates(fromRole)
                        .relates(toRole);

                Rule rule1 = tx.putRule("transRule",
                        Graql.and(
                                Graql.var()
                                        .rel(type(fromRole.label().getValue()), Graql.var("x"))
                                        .rel(type(toRole.label().getValue()), Graql.var("y"))
                                        .isa(type(transRelation.label().getValue())),
                                Graql.var()
                                        .rel(type(fromRole.label().getValue()), Graql.var("y"))
                                        .rel(type(toRole.label().getValue()), Graql.var("z"))
                                        .isa(type(transRelation.label().getValue()))
                        ),
                        Graql.var()
                                .rel(type(fromRole.label().getValue()), Graql.var("x"))
                                .rel(type(toRole.label().getValue()), Graql.var("z"))
                                .isa(type(transRelation.label().getValue()))
                );

                Rule rule = tx.putRule("equivRule",
                        Graql.var()
                                .rel(type(fromRole.label().getValue()), Graql.var("x"))
                                .rel(type(toRole.label().getValue()), Graql.var("z"))
                                .isa(type(someRelation.label().getValue())),
                        Graql.var()
                                .rel(type(fromRole.label().getValue()), Graql.var("x"))
                                .rel(type(toRole.label().getValue()), Graql.var("z"))
                                .isa(type(transRelation.label().getValue()))
                );

                Entity entityA = someEntity.create();
                Entity entityB = someEntity.create();
                Entity entityC = someEntity.create();
                Entity entityD = someEntity.create();
                Entity entityE = someEntity.create();
                Entity entityF = someEntity.create();
                Entity entityG = someEntity.create();

                someRelation.create().assign(fromRole, entityA).assign(toRole, entityB);
                someRelation.create().assign(fromRole, entityA).assign(toRole, entityC);
                someRelation.create().assign(fromRole, entityA).assign(toRole, entityD);
                someRelation.create().assign(fromRole, entityD).assign(toRole, entityE);
                someRelation.create().assign(fromRole, entityE).assign(toRole, entityF);
                someRelation.create().assign(fromRole, entityF).assign(toRole, entityA);
                someRelation.create().assign(fromRole, entityF).assign(toRole, entityG);
                tx.commit();
            }
            try (TransactionOLTP tx = session.transaction().write()) {
                String patternString = "{ (fromRole: $x, toRole: $y) isa transRelation; };";
                ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, tx), tx);
                Set<InferenceRule> rules = tx.ruleCache().getRules().map(r -> new InferenceRule(r, tx)).collect(toSet());

                //with cache empty no loops are found
                assertFalse(RuleUtils.subGraphIsCyclical(rules, tx));
                assertFalse(query.requiresReiteration());
                query.resolve().collect(Collectors.toList());

                //with populated cache we find a loop
                assertTrue(RuleUtils.subGraphIsCyclical(rules, tx));
            }
        }
    }

    @Test
    public void whenQueryHasMultipleDisconnectedInferrableAtoms_weReiterate(){
        try (SessionImpl session = server.sessionWithNewKeyspace()) {
            try(TransactionOLTP tx = session.transaction().write()) {
                tx.execute(Graql.parse("define " +
                        "someEntity sub entity," +
                        "has derivedResource;" +
                        "derivedResource sub attribute, datatype long;" +
                        "rule1 sub rule, when{ $x isa someEntity;}, then { $x has derivedResource 1337;};"
                ).asDefine());
                tx.execute(Graql.parse("insert " +
                        "$x isa someEntity;" +
                        "$y isa someEntity;"
                ).asInsert());
                tx.commit();
            }
            Pattern pattern = Graql.and(
                    Graql.var("x").has("derivedResource", Graql.var("value")),
                    Graql.var("y").has("derivedResource", Graql.var("anotherValue"))
            );
            try (TransactionOLTP tx = session.transaction().write()) {
                ReasonerQueryImpl query = ReasonerQueries.create(conjunction(pattern.toString(), tx), tx);
                assertTrue(query.requiresReiteration());
            }
        }
    }


    @Test
    public void whenRetrievingVariablesFromQueryWithComparisons_variablesFromValuePredicatesAreFetched(){
        try (SessionImpl session = server.sessionWithNewKeyspace()) {
            try (TransactionOLTP tx = session.transaction().write()) {

                AttributeType<Long> resource = tx.putAttributeType("resource", AttributeType.DataType.LONG);
                resource.create(1337L);
                resource.create(1667L);
                tx.putEntityType("someEntity")
                        .has(resource);
                tx.commit();
            }
            try (TransactionOLTP tx = session.transaction().write()) {
                Attribute<Long> attribute = tx.getAttributesByValue(1337L).iterator().next();
                String basePattern = "{" +
                        "$x isa someEntity;" +
                        "$x has resource $value;" +
                        "$value > $anotherValue;" +
                        "};";
                ReasonerQueryImpl query = ReasonerQueries.create(conjunction(basePattern, tx), tx);
                assertEquals(
                        Sets.newHashSet(new Variable("x"), new Variable("value"), new Variable("anotherValue")),
                        query.getVarNames());
                ConceptMap sub = new ConceptMap(ImmutableMap.of(new Variable("anotherValue"), attribute));
                ReasonerQueryImpl subbedQuery = ReasonerQueries.create(query, sub);
                assertTrue(subbedQuery.getAtoms(IdPredicate.class).findAny().isPresent());
            }
        }
    }

    @Test
    public void testAlphaEquivalence_simpleChainWithAttributeAndTypeGuards() {
        try(TransactionOLTP tx = geoSession.transaction().write()) {
            String patternString = "{ " +
                    "$x isa city, has name 'Warsaw';" +
                    "$y isa region;" +
                    "($x, $y) isa is-located-in;" +
                    "($y, $z) isa is-located-in;" +
                    "$z isa country, has name 'Poland'; };";

            String patternString2 = "{ " +
                    "($r, $ctr) isa is-located-in;" +
                    "($c, $r) isa is-located-in;" +
                    "$c isa city, has name 'Warsaw';" +
                    "$r isa region;" +
                    "$ctr isa country, has name 'Poland'; };";

            ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, tx), tx);
            ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, tx), tx);
            queryEquivalence(query, query2, true);
        }
    }

    @Ignore ("we currently do not fully support equivalence checks for non-atomic queries")
    @Test
    public void testAlphaEquivalence_chainTreeAndLoopStructure() {
        try(TransactionOLTP tx = geoSession.transaction().write()) {
            String chainString = "{" +
                    "($x, $y) isa is-located-in;" +
                    "($y, $z) isa is-located-in;" +
                    "($z, $u) isa is-located-in;" +
                    "}";

            String treeString = "{" +
                    "($x, $y) isa is-located-in;" +
                    "($y, $z) isa is-located-in;" +
                    "($y, $u) isa is-located-in;" +
                    "}";

            String loopString = "{" +
                    "($x, $y) isa is-located-in;" +
                    "($y, $z) isa is-located-in;" +
                    "($z, $x) isa is-located-in;" +
                    "}";

            ReasonerQueryImpl chainQuery = ReasonerQueries.create(conjunction(chainString, tx), tx);
            ReasonerQueryImpl treeQuery = ReasonerQueries.create(conjunction(treeString, tx), tx);
            ReasonerQueryImpl loopQuery = ReasonerQueries.create(conjunction(loopString, tx), tx);
            queryEquivalence(chainQuery, treeQuery, false);
            queryEquivalence(chainQuery, loopQuery, false);
            queryEquivalence(treeQuery, loopQuery, false);
        }
    }

    @Test //tests various configurations of alpha-equivalence with extra type atoms present
    public void testAlphaEquivalence_nonMatchingTypes() {
        try(TransactionOLTP tx = geoSession.transaction().write()) {
            String polandId = getConcept(tx, "name", "Poland").id().getValue();
            String patternString = "{ $y id " + polandId + "; $y isa country; (geo-entity: $y1, entity-location: $y) isa is-located-in; };";
            String patternString2 = "{ $x1 id " + polandId + "; $y isa country; (geo-entity: $x1, entity-location: $x2) isa is-located-in; };";
            String patternString3 = "{ $y id " + polandId + "; $x isa city; (geo-entity: $x, entity-location: $y) isa is-located-in; $y isa country; };";
            String patternString4 = "{ $x isa city; (entity-location: $y1, geo-entity: $x) isa is-located-in; };";
            String patternString5 = "{ (geo-entity: $y1, entity-location: $y2) isa is-located-in; };";

            ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, tx), tx);
            ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, tx), tx);
            ReasonerQueryImpl query3 = ReasonerQueries.create(conjunction(patternString3, tx), tx);
            ReasonerQueryImpl query4 = ReasonerQueries.create(conjunction(patternString4, tx), tx);
            ReasonerQueryImpl query5 = ReasonerQueries.create(conjunction(patternString5, tx), tx);

            queryEquivalence(query, query2, false);
            queryEquivalence(query, query3, false);
            queryEquivalence(query2, query3, false);
            queryEquivalence(query, query4, false);
            queryEquivalence(query, query5, false);
            queryEquivalence(query3, query4, false);
            queryEquivalence(query3, query5, false);
            queryEquivalence(query4, query5, false);
        }
    }

    @Test //tests alpha-equivalence of queries with indirect types
    public void testAlphaEquivalence_indirectTypes(){
        try(TransactionOLTP tx = geoSession.transaction().write()) {
            String patternString = "{ (entity-location: $x2, geo-entity: $x1) isa is-located-in;" +
                    "$x1 isa $t1; $t1 sub geoObject; };";
            String patternString2 = "{ (geo-entity: $y1, entity-location: $y2) isa is-located-in;" +
                    "$y1 isa $t2; $t2 sub geoObject; };";

            ReasonerQueryImpl query = ReasonerQueries.create(conjunction(patternString, tx), tx);
            ReasonerQueryImpl query2 = ReasonerQueries.create(conjunction(patternString2, tx), tx);
            queryEquivalence(query, query2, true);
        }
    }

    @Test
    public void testAlphaEquivalence_RelationsWithSubstitution(){
        try(TransactionOLTP tx = geoSession.transaction().write()) {
            String patternString = "{ (role: $x, role: $y);$x id V666; };";
            String patternString2 = "{ (role: $x, role: $y);$y id V666; };";
            String patternString3 = "{ (role: $x, role: $y);$x id V666;$y id V667; };";
            String patternString4 = "{ (role: $x, role: $y);$y id V666;$x id V667; };";
            String patternString5 = "{ (entity-location: $x, geo-entity: $y);$x id V666;$y id V667; };";
            String patternString6 = "{ (entity-location: $x, geo-entity: $y);$y id V666;$x id V667; };";
            String patternString7 = "{ (role: $x, role: $y);$x id V666;$y id V666; };";
            Conjunction<Statement> pattern = conjunction(patternString, tx);
            Conjunction<Statement> pattern2 = conjunction(patternString2, tx);
            Conjunction<Statement> pattern3 = conjunction(patternString3, tx);
            Conjunction<Statement> pattern4 = conjunction(patternString4, tx);
            Conjunction<Statement> pattern5 = conjunction(patternString5, tx);
            Conjunction<Statement> pattern6 = conjunction(patternString6, tx);
            Conjunction<Statement> pattern7 = conjunction(patternString7, tx);

            ReasonerAtomicQuery query = ReasonerQueries.atomic(pattern, tx);
            ReasonerAtomicQuery query2 = ReasonerQueries.atomic(pattern2, tx);
            ReasonerAtomicQuery query3 = ReasonerQueries.atomic(pattern3, tx);
            ReasonerAtomicQuery query4 = ReasonerQueries.atomic(pattern4, tx);
            ReasonerAtomicQuery query5 = ReasonerQueries.atomic(pattern5, tx);
            ReasonerAtomicQuery query6 = ReasonerQueries.atomic(pattern6, tx);
            ReasonerAtomicQuery query7 = ReasonerQueries.atomic(pattern7, tx);

            queryEquivalence(query, query2, true);
            queryEquivalence(query, query3, false);
            queryEquivalence(query, query4, false);
            queryEquivalence(query, query5, false);
            queryEquivalence(query, query6, false);
            queryEquivalence(query, query7, false);

            queryEquivalence(query2, query3, false);
            queryEquivalence(query2, query4, false);
            queryEquivalence(query2, query5, false);
            queryEquivalence(query2, query6, false);
            queryEquivalence(query2, query7, false);

            queryEquivalence(query3, query4, true);
            queryEquivalence(query3, query5, false);
            queryEquivalence(query3, query6, false);
            queryEquivalence(query3, query7, false);

            queryEquivalence(query4, query5, false);
            queryEquivalence(query4, query6, false);
            queryEquivalence(query4, query7, false);

            queryEquivalence(query5, query6, false);
            queryEquivalence(query5, query7, false);

            queryEquivalence(query6, query7, false);
        }
    }

    @Test
    public void whenReifyingRelation_extraAtomIsCreatedWithUserDefinedName(){
        try(TransactionOLTP tx = geoSession.transaction().write()) {
            String patternString = "{ (geo-entity: $x, entity-location: $y) isa is-located-in; };";
            String patternString2 = "{ ($x, $y) has name 'Poland'; };";

            Conjunction<Statement> pattern = conjunction(patternString, tx);
            Conjunction<Statement> pattern2 = conjunction(patternString2, tx);
            ReasonerQueryImpl query = ReasonerQueries.create(pattern, tx);
            ReasonerQueryImpl query2 = ReasonerQueries.create(pattern2, tx);
            assertFalse(query.getAtoms(RelationAtom.class).findFirst().orElse(null).isUserDefined());
            assertTrue(query2.getAtoms(RelationAtom.class).findFirst().orElse(null).isUserDefined());
            assertEquals(query.getAtoms().size(), 1);
            assertEquals(query2.getAtoms().size(), 2);
        }
    }

    private void queryEquivalence(ReasonerQueryImpl a, ReasonerQueryImpl b, boolean expectation){
        assertEquals(a.toString() + " =? " + b.toString(), a.equals(b), expectation);
        assertEquals(b.toString() + " =? " + a.toString(), b.equals(a), expectation);
        //check hash additionally if need to be equal
        if (expectation) {
            assertTrue(a.toString() + " hash=? " + b.toString(), a.hashCode() == b.hashCode());
        }
    }

    private Conjunction<Statement> conjunction(String patternString, TransactionOLTP tx){
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }

    private static Concept getConcept(TransactionOLTP tx, String typeLabel, String val){
        return tx.stream(Graql.match((Pattern) Graql.var("x").has(typeLabel, val)).get("x"))
                .map(ans -> ans.get("x")).findAny().get();
    }
}
