/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.reasoner.cache;

import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestStorage;
import grakn.core.test.rule.SessionUtil;
import grakn.core.test.rule.TestTransactionProvider;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static graql.lang.Graql.type;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.assertFalse;

@SuppressWarnings("CheckReturnValue")
public class RuleCacheIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session session;

    @BeforeClass
    public static void loadContext() {
        session = SessionUtil.serverlessSessionWithNewKeyspace(storage.createCompatibleServerConfig());
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            String schema = "define " +
                    "noInstanceEntity sub entity, plays symmetricRole;" +
                    "baseEntity sub entity," +
                    "    plays symmetricRole," +
                    "    plays someRole," +
                    "    plays anotherRole;" +
                    "specialisedEntity sub baseEntity;" +
                    "baseRelation sub relation," +
                    "    relates someRole," +
                    "    relates anotherRole," +
                    "    relates symmetricRole;" +
                    "transitiveRelation sub baseRelation," +
                    "    relates someRole," +
                    "    relates anotherRole," +
                    "    relates symmetricRole;" +
                    "symmetricRelation sub baseRelation," +
                    "    relates someRole," +
                    "    relates anotherRole," +
                    "    relates symmetricRole;" +
                    "transitivityRule sub rule," +
                    "when {" +
                    "    (someRole: $x, anotherRole: $y) isa transitiveRelation;" +
                    "    (someRole: $y, anotherRole: $z) isa transitiveRelation;" +
                    "}," +
                    "then {" +
                    "    (someRole: $x, anotherRole: $z) isa transitiveRelation;" +
                    "};" +
                    "" +
                    "fruitlessRule sub rule," +
                    "when {" +
                    "    $x isa noInstanceEntity;" +
                    "    $y isa noInstanceEntity;" +
                    "}," +
                    "then {" +
                    "    (symmetricRole: $x, symmetricRole: $y) isa symmetricRelation;" +
                    "};" +
                    "" +
                    "fruitfulRule sub rule," +
                    "when {" +
                    "    $x isa specialisedEntity;" +
                    "    $y isa specialisedEntity;" +
                    "}," +
                    "then {" +
                    "    (symmetricRole: $x, symmetricRole: $y) isa symmetricRelation;" +
                    "};";
            tx.execute(Graql.parse(schema).asDefine());
            String data = "insert" +
                    "$x isa specialisedEntity;" +
                    "$y isa specialisedEntity;" +
                    "$z isa specialisedEntity;" +
                    "(someRole: $x, anotherRole: $y) isa transitiveRelation;" +
                    "(someRole: $y, anotherRole: $z) isa transitiveRelation;";
            tx.execute(Graql.parse(data).asInsert());
            tx.commit();
        }
    }

    @AfterClass
    public static void closeSession() {
        session.close();
    }

    @Test
    public void whenGettingRulesWithType_correctRulesAreObtained(){
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;
            RuleCacheImpl ruleCache = testTx.ruleCache();

            Type transitiveRelation = tx.getType(Label.of("transitiveRelation"));
            Set<Rule> rulesWithSpecialisedRelation = ruleCache.getRulesWithType(transitiveRelation).collect(toSet());

            assertEquals(transitiveRelation.thenRules().collect(toSet()), rulesWithSpecialisedRelation);
            rulesWithSpecialisedRelation.stream()
                    .map(ruleCache::getRule)
                    .forEach(r -> assertEquals(transitiveRelation, r.getHead().getAtom().getSchemaConcept()));
        }
    }

    @Test
    public void whenAddingARule_cacheContainsUpdatedEntry(){
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            Pattern when = Graql.parsePattern("{ $x isa entity;$y isa entity; };");
            Pattern then = Graql.parsePattern("{ (someRole: $x, anotherRole: $y) isa baseRelation; };");
            Rule dummyRule = tx.putRule("dummyRule", when, then);
            Type baseRelation = tx.getType(Label.of("baseRelation"));

            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;
            Set<Rule> cachedRules = testTx.ruleCache().getRulesWithType(baseRelation).collect(Collectors.toSet());
            assertTrue(cachedRules.contains(dummyRule));
        }
    }

    @Test
    public void whenDeletingARule_cacheContainsUpdatedEntry(){
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.undefine(type("transitivityRule").sub("rule")));
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;

            Type relation = tx.getType(Label.of("transitiveRelation"));
            Set<Rule> rules = testTx.ruleCache().getRulesWithType(relation).collect(Collectors.toSet());
            assertTrue(rules.isEmpty());
        }
    }

    @Test
    public void whenFetchingRules_fruitlessRulesAreNotReturned(){
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;

            Type symmetricRelation = tx.getType(Label.of("symmetricRelation"));
            Set<Rule> rulesWithInstances = symmetricRelation.thenRules()
                    .filter(r -> tx.stream(Graql.match(r.when())).findFirst().isPresent())
                    .collect(Collectors.toSet());
            Set<Rule> fetchedRules = testTx.ruleCache().getRulesWithType(symmetricRelation).collect(Collectors.toSet());
            //NB:db lookup filters more aggressively, hence we check for containment
            assertTrue(fetchedRules.containsAll(rulesWithInstances));

            //even though rules are filtered, the type has instances
            assertFalse(fetchedRules.isEmpty());
        }
    }

    @Test
    public void whenTypeHasDirectInstances_itIsNotAbsent(){
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;

            EntityType anotherNoRoleEntity = tx.getEntityType("specialisedEntity");
            assertFalse(testTx.ruleCache().absentTypes(Collections.singleton(anotherNoRoleEntity)));
        }
    }

    @Test
    public void whenTypeHasIndirectInstances_itIsNotAbsent(){
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;

            //no direct instances present, however specialisedEntity subs baseEntity and has instances
            EntityType anotherSingleRoleEntity = tx.getEntityType("baseEntity");
            assertFalse(testTx.ruleCache().absentTypes(Collections.singleton(anotherSingleRoleEntity)));
        }
    }

    @Test
    public void whenTypeHasFruitfulRulesButNotDirectInstances_itIsNotAbsent(){
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;

            Type symmetricRelation = tx.getType(Label.of("symmetricRelation"));
            assertFalse(symmetricRelation.instances().findFirst().isPresent());
            assertFalse(testTx.ruleCache().absentTypes(Collections.singleton(symmetricRelation)));
        }
    }

    @Test
    public void whenTypeSubTypeHasFruitfulRulesButNotDirectInstances_itIsNotAbsent(){
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;

            //although baseRelation doesn't have direct instances, it's subtypes either have instances (transitiveRelation)
            //or are inferrable (symmetricRelation)
            Type baseRelation = tx.getRelationType("baseRelation");
            assertFalse(tx.stream(Graql.match(Graql.var().isaX(baseRelation.label().getValue()))).findFirst().isPresent());

            RelationType transitiveRelation = tx.getRelationType("transitiveRelation");
            //now we delete the subtype instances and we are left with only inferrable subtype instances
            transitiveRelation.instances().forEach(Concept::delete);
            assertFalse(testTx.ruleCache().absentTypes(Collections.singleton(baseRelation)));
        }
    }

    @Test
    public void whenInsertHappensDuringTransaction_extraInstanceIsAcknowledged(){
        try(Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;

            EntityType baseEntity = tx.getEntityType("baseEntity");
            baseEntity.create();
            assertFalse(testTx.ruleCache().absentTypes(Collections.singleton(baseEntity)));
        }
    }

    @Test
    public void whenRulesWithPositiveAndNegativePremiseArePresent_lackOfInstancesOnlyPrunesOne(){
        Session session = SessionUtil.serverlessSessionWithNewKeyspace(storage.createCompatibleServerConfig());
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)){
            AttributeType<String> resource = tx.putAttributeType("resource", AttributeType.ValueType.STRING);
            AttributeType<String> derivedResource = tx.putAttributeType("derivedResource", AttributeType.ValueType.STRING);
            tx.putEntityType("someEntity").has(resource).has(derivedResource);
            tx.putEntityType("derivedEntity");
            tx.putRule("positiveRule",
                    Graql.and(
                            Graql.var("x").has("resource", Graql.var("r")),
                            Graql.var("x").isa("someEntity")),
                    Graql.var("x").has("derivedResource", Graql.var("r"))
            );
            tx.putRule("negativeRule",
                    Graql.and(
                            Graql.var("x").has("resource", Graql.var("r")),
                            Graql.not(Graql.var("x").isa("someEntity"))),
                    Graql.var("x").has("derivedResource", Graql.var("r"))
            );

            resource.create("banana");
            tx.commit();
        }
        try(Transaction tx = session.transaction(Transaction.Type.READ)) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;

            EntityType someEntity = tx.getEntityType("someEntity");
            AttributeType<?> derivedResource = tx.getAttributeType("derivedResource");
            assertTrue(testTx.ruleCache().absentTypes(Collections.singleton(someEntity)));

            Rule positiveRule = tx.getRule("positiveRule");
            Rule negativeRule = tx.getRule("negativeRule");

            Set<Rule> rules = testTx.ruleCache().getRulesWithType(derivedResource).collect(toSet());
            assertTrue(rules.contains(negativeRule));
            assertFalse(rules.contains(positiveRule));
        }
    }
}
