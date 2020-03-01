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

import com.google.common.collect.Sets;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static graql.lang.Graql.type;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
public class RuleCacheIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session ruleApplicabilitySession;

    @BeforeClass
    public static void loadContext() {
        ruleApplicabilitySession = SessionUtil.serverlessSessionWithNewKeyspace(storage.createCompatibleServerConfig());
        String resourcePath = "test-integration/graql/reasoner/resources/";
        loadFromFileAndCommit(resourcePath, "ruleApplicabilityTest.gql", ruleApplicabilitySession);
    }

    @AfterClass
    public static void closeSession() {
        ruleApplicabilitySession.close();
    }

    @Test
    public void whenGettingRulesWithType_correctRulesAreObtained(){
        try(Transaction tx = ruleApplicabilitySession.writeTransaction()) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;
            RuleCacheImpl ruleCache = testTx.ruleCache();

            Type reifyingRelation = tx.getType(Label.of("reifying-relation"));
            Type ternary = tx.getType(Label.of("ternary"));
            Set<Rule> rulesWithBinary = ruleCache.getRulesWithType(reifyingRelation).collect(toSet());
            Set<Rule> rulesWithTernary = ruleCache.getRulesWithType(ternary).collect(toSet());

            assertEquals(2, rulesWithBinary.size());
            assertEquals(2, rulesWithTernary.size());

            rulesWithBinary.stream()
                    .map(ruleCache::getRule)
                    .forEach(r -> assertEquals(reifyingRelation, r.getHead().getAtom().getSchemaConcept()));
            rulesWithTernary.stream()
                    .map(ruleCache::getRule)
                    .forEach(r -> assertEquals(ternary, r.getHead().getAtom().getSchemaConcept()));
        }
    }

    @Test
    public void whenAddingARule_cacheContainsUpdatedEntry(){
        try(Transaction tx = ruleApplicabilitySession.writeTransaction()) {
            Pattern when = Graql.parsePattern("{ $x isa entity;$y isa entity; };");
            Pattern then = Graql.parsePattern("{ (someRole: $x, subRole: $y) isa binary; };");
            Rule dummyRule = tx.putRule("dummyRule", when, then);

            Type binary = tx.getType(Label.of("binary"));

            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;
            Set<Rule> cachedRules = testTx.ruleCache().getRulesWithType(binary).collect(Collectors.toSet());
            assertTrue(cachedRules.contains(dummyRule));
        }
    }

    @Test
    public void whenAddingARuleAfterClosingTx_cacheContainsConsistentEntry(){
        try(Transaction tx = ruleApplicabilitySession.writeTransaction()) {

            Pattern when = Graql.parsePattern("{ $x isa entity;$y isa entity; };");
            Pattern then = Graql.parsePattern("{ (someRole: $x, subRole: $y) isa binary; };");
            Rule dummyRule = tx.putRule("dummyRule", when, then);

            Type binary = tx.getType(Label.of("binary"));
            Set<Rule> commitedRules = binary.thenRules().collect(Collectors.toSet());

            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;
            Set<Rule> cachedRules = testTx.ruleCache().getRulesWithType(binary).collect(toSet());
            assertEquals(Sets.union(commitedRules, Sets.newHashSet(dummyRule)), cachedRules);
        }
    }

    @Test
    public void whenDeletingARule_cacheContainsUpdatedEntry(){
        try(Transaction tx = ruleApplicabilitySession.writeTransaction()) {
            tx.execute(Graql.undefine(type("binary-transitivity").sub("rule")));
            tx.commit();
        }
        try(Transaction tx = ruleApplicabilitySession.writeTransaction()) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;

            Type binary = tx.getType(Label.of("binary"));
            Set<Rule> rules = testTx.ruleCache().getRulesWithType(binary).collect(Collectors.toSet());
            assertTrue(rules.isEmpty());
        }
    }

    @Test
    public void whenFetchingRules_fruitlessRulesAreNotReturned(){
        try(Transaction tx = ruleApplicabilitySession.writeTransaction()) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;

            Type description = tx.getType(Label.of("description"));
            Set<Rule> rulesWithInstances = description.thenRules()
                    .filter(r -> tx.stream(Graql.match(r.when())).findFirst().isPresent())
                    .collect(Collectors.toSet());
            Set<Rule> fetchedRules = testTx.ruleCache().getRulesWithType(description).collect(Collectors.toSet());
            //NB:db lookup filters more aggressively, hence we check for containment
            assertTrue(fetchedRules.containsAll(rulesWithInstances));

            //even though rules are filtered, the type has instances
            assertFalse(fetchedRules.isEmpty());
            assertFalse(testTx.ruleCache().absentTypes(Collections.singleton(description)));
        }
    }

    @Test
    public void whenTypeHasDirectInstances_itIsNotAbsent(){
        try(Transaction tx = ruleApplicabilitySession.writeTransaction()) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;

            EntityType anotherNoRoleEntity = tx.getEntityType("anotherNoRoleEntity");
            assertFalse(testTx.ruleCache().absentTypes(Collections.singleton(anotherNoRoleEntity)));
        }
    }

    @Test
    public void whenTypeHasIndirectInstances_itIsNotAbsent(){
        try(Transaction tx = ruleApplicabilitySession.writeTransaction()) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;

            //no direct instances present, however anotherTwoRoleEntity subs anotherSingleRoleEntity and has instances
            EntityType anotherSingleRoleEntity = tx.getEntityType("anotherSingleRoleEntity");
            assertFalse(testTx.ruleCache().absentTypes(Collections.singleton(anotherSingleRoleEntity)));
        }
    }

    @Test
    public void whenTypeHasFruitfulRulesButNotDirectInstances_itIsNotAbsent(){
        try(Transaction tx = ruleApplicabilitySession.writeTransaction()) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;

            Type description = tx.getType(Label.of("description"));
            assertFalse(description.instances().findFirst().isPresent());
            assertFalse(testTx.ruleCache().absentTypes(Collections.singleton(description)));
        }
    }

    @Test
    public void whenTypeSubTypeHasFruitfulRulesButNotDirectInstances_itIsNotAbsent(){
        try(Transaction tx = ruleApplicabilitySession.writeTransaction()) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;

            RelationType binary = tx.getRelationType("binary");
            binary.instances().forEach(Concept::delete);

            RelationType reifiableRelation = tx.getRelationType("reifiable-relation");
            assertFalse(reifiableRelation.instances().findFirst().isPresent());
            assertFalse(testTx.ruleCache().absentTypes(Collections.singleton(reifiableRelation)));
        }
    }

    @Test
    public void whenInsertHappensDuringTransaction_extraInstanceIsAcknowledged(){
        try(Transaction tx = ruleApplicabilitySession.writeTransaction()) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;

            EntityType singleRoleEntity = tx.getEntityType("singleRoleEntity");
            singleRoleEntity.instances().forEach(Concept::delete);
            assertTrue(testTx.ruleCache().absentTypes(Collections.singleton(singleRoleEntity)));

            singleRoleEntity.create();
            assertFalse(testTx.ruleCache().absentTypes(Collections.singleton(singleRoleEntity)));
        }
    }

    @Test
    public void whenRulesWithPositiveAndNegativePremiseArePresent_lackOfInstancesOnlyPrunesOne(){
        Session session = SessionUtil.serverlessSessionWithNewKeyspace(storage.createCompatibleServerConfig());
        try (Transaction tx = session.writeTransaction()){
            AttributeType<String> resource = tx.putAttributeType("resource", AttributeType.DataType.STRING);
            AttributeType<String> derivedResource = tx.putAttributeType("derivedResource", AttributeType.DataType.STRING);
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
        try(Transaction tx = session.readTransaction()) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;

            EntityType someEntity = tx.getEntityType("someEntity");
            AttributeType derivedResource = tx.getAttributeType("derivedResource");
            assertTrue(testTx.ruleCache().absentTypes(Collections.singleton(someEntity)));

            Rule positiveRule = tx.getRule("positiveRule");
            Rule negativeRule = tx.getRule("negativeRule");

            Set<Rule> rules = testTx.ruleCache().getRulesWithType(derivedResource).collect(toSet());
            assertTrue(rules.contains(negativeRule));
            assertFalse(rules.contains(positiveRule));
        }
    }
}
