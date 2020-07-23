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
 */

package grakn.core.graql.reasoner.atomic;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import grakn.core.common.config.Config;
import grakn.core.core.Schema;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.graql.reasoner.rule.InferenceRule;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestStorage;
import grakn.core.test.rule.SessionUtil;
import grakn.core.test.rule.TestTransactionProvider;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.test.common.GraqlTestUtil.loadFromFileAndCommit;
import static graql.lang.Graql.var;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
public class RuleApplicabilityIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session ruleApplicabilitySession;
    private static Session resourceApplicabilitySession;
    private static Session explicitOwnershipRelationSession;

    private static Thing putEntityWithResource(Transaction tx, EntityType type, Label resource, Object value) {
        Thing inst = type.create();
        putResource(inst, tx.getSchemaConcept(resource), value);
        return inst;
    }

    private static <T> void putResource(Thing thing, AttributeType<T> attributeType, T value) {
        Attribute attributeInstance = attributeType.create(value);
        thing.has(attributeInstance);
    }

    @BeforeClass
    public static void loadContext(){
        Config mockServerConfig = storage.createCompatibleServerConfig();
        resourceApplicabilitySession = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        String resourcePath = "test/integration/graql/reasoner/resources/";
        loadFromFileAndCommit(resourcePath,"resourceApplicabilityTest.gql", resourceApplicabilitySession);
        explicitOwnershipRelationSession =  SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        loadFromFileAndCommit(resourcePath,"explicitOwnershipRelationApplicabilityTest.gql", explicitOwnershipRelationSession);
        ruleApplicabilitySession =  SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        loadFromFileAndCommit(resourcePath,"ruleApplicabilityTest2.gql", ruleApplicabilitySession);

//        //add extra data so that all rules can be possibly triggered
//        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
//            EntityType singleRoleEntity = tx.getEntityType("singleRoleEntity");
//            EntityType twoRoleEntity = tx.getEntityType("twoRoleEntity");
//            Thing x = putEntityWithResource(tx, singleRoleEntity, Label.of("resource-string"), "someValue");
//            Thing y = putEntityWithResource(tx, twoRoleEntity, Label.of("resource-long"), 2000L);
//
//            Role someRole = tx.getRole("upper-connected");
//            Role subRole = tx.getRole("lower-connected");
//            tx.getRelationType("ranked-connection")
//                    .create()
//                    .assign(someRole, x)
//                    .assign(subRole, y)
//                    .has(tx.getAttributeType("description").create("someDescription"));
//            tx.commit();
//        }
    }

    @AfterClass
    public static void closeSession(){
        ruleApplicabilitySession.close();
        resourceApplicabilitySession.close();
        explicitOwnershipRelationSession.close();
    }

    @Test
    public void metaAttributeMatchesAllAttributeRules(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;
            ReasonerQueryFactory reasonerQueryFactory = testTx.reasonerQueryFactory();
            Atom metaAttribute = reasonerQueryFactory.atomic(conjunction("{ $x isa attribute;};")).getAtom();
            Set<InferenceRule> rules = testTx.ruleCache().getRules().map(r -> new InferenceRule(r, reasonerQueryFactory)).collect(Collectors.toSet());

            assertEquals(
                    rules.stream().filter(r -> r.getHead().getAtom().getSchemaConcept().isAttributeType()).collect(Collectors.toSet()),
                    metaAttribute.getApplicableRules().collect(toSet()));
        }
    }

    @Test
    public void ontologicalAtomsDoNotMatchAnyRules(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            Atom subAtom = reasonerQueryFactory.atomic(conjunction("{ $x sub " + Schema.MetaSchema.RELATION.getLabel() + "; };")).getAtom();
            Atom hasAtom = reasonerQueryFactory.atomic(conjunction("{ $x has description; };")).getAtom();
            Atom relatesAtom = reasonerQueryFactory.atomic(conjunction("{ path relates $x; };")).getAtom();
            Atom relatesAtom2 = reasonerQueryFactory.atomic(conjunction("{ $x relates path_end; };")).getAtom();
            Atom playsAtom = reasonerQueryFactory.atomic(conjunction("{ $x plays path_end; };")).getAtom();

            assertThat(subAtom.getApplicableRules().collect(toSet()), empty());
            assertThat(hasAtom.getApplicableRules().collect(toSet()), empty());
            assertThat(relatesAtom.getApplicableRules().collect(toSet()), empty());
            assertThat(relatesAtom2.getApplicableRules().collect(toSet()), empty());
            assertThat(playsAtom.getApplicableRules().collect(toSet()), empty());
        }
    }

    @Test
    public void typeRelationMatchesAllRulesWithCorrespondingRelationType(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;
            ReasonerQueryFactory reasonerQueryFactory = testTx.reasonerQueryFactory();

            String typeString = "{ $x isa elevation-ranking; };";
            String typeString2 = "{ $x isa ternary-steep-path; };";
            String typeString3 = "{ $x isa located; };";
            Atom rankingAtom = reasonerQueryFactory.atomic(conjunction(typeString)).getAtom();
            Atom ternarySteepPathAtom = reasonerQueryFactory.atomic(conjunction(typeString2)).getAtom();
            Atom locatedAtom = reasonerQueryFactory.atomic(conjunction(typeString3)).getAtom();

            Type ternarySteepPath = testTx.getRelationType("ternary-steep-path");
            Type located = testTx.getRelationType("located");
            Type ranking = testTx.getRelationType("elevation-ranking");

            Set<InferenceRule> rules = testTx.ruleCache().getRules().map(r -> new InferenceRule(r, reasonerQueryFactory)).collect(Collectors.toSet());
            Set<InferenceRule> ternarySteepPathRules = rules.stream().filter(rule -> rule.getHead().getAtom().getSchemaConcept().equals(ternarySteepPath)).collect(toSet());
            Set<InferenceRule> locatedRules = rules.stream().filter(rule -> rule.getHead().getAtom().getSchemaConcept().equals(located)).collect(toSet());
            Set<InferenceRule> rankingRules = rules.stream().filter(rule -> rule.getHead().getAtom().getSchemaConcept().equals(ranking)).collect(toSet());

            assertEquals(
                    rankingRules.stream().map(InferenceRule::getRule).collect(toSet()),
                    rankingAtom.getApplicableRules().map(InferenceRule::getRule).collect(toSet()));
            assertEquals(
                    ternarySteepPathRules.stream().map(InferenceRule::getRule).collect(toSet()),
                    ternarySteepPathAtom.getApplicableRules().map(InferenceRule::getRule).collect(toSet()));
            assertEquals(
                    locatedRules.stream().map(InferenceRule::getRule).collect(toSet()),
                    locatedAtom.getApplicableRules().map(InferenceRule::getRule).collect(toSet()));
        }
    }

    @Test
    public void typeAttributeMatchesAllRulesWithCorrespondingAttributeType(){
        try(Transaction tx = resourceApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;
            ReasonerQueryFactory reasonerQueryFactory = testTx.reasonerQueryFactory();

            String typeString = "{ $x isa resource; };";
            Atom descriptionAtom = reasonerQueryFactory.atomic(conjunction(typeString)).getAtom();
            Type descriptionType = testTx.getAttributeType("resource");

            Set<InferenceRule> resourceRules = testTx.ruleCache().getRules()
                    .map(r -> new InferenceRule(r, reasonerQueryFactory))
                    .filter(rule -> rule.getHead().getAtom().getSchemaConcept().equals(descriptionType))
                    .collect(Collectors.toSet());

            assertEquals(resourceRules, descriptionAtom.getApplicableRules().collect(toSet()));
        }
    }

    @Test
    public void relationWithUnspecificRoles_typedRoleplayers_ambiguousRoleMapping(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String relationString = "{ ($x, $y, attributed:$z); $x isa monument; $y isa discoverer; $z isa ranked-hill-city; };";
            RelationAtom relation = (RelationAtom) reasonerQueryFactory.atomic(conjunction(relationString)).getAtom();
            // to obtain a partial ambiguity, we have to induce the following:
            // one type should pin a role to an unambiguous super-relation -- the role will be known
            // both other role players need to be able to play two _mutually exclusive_ roles from the relation
            // or its children
            // we also use a role to restrict possible relations
            ImmutableSetMultimap<Role, Variable> roleMap = ImmutableSetMultimap.of(
                    tx.getRole("role"), new Variable("x"),
                    tx.getRole("role"), new Variable("y"),
                    tx.getRole("attributed"), new Variable("z"));
            assertEquals(roleMap, roleSetMap((relation.getRoleVarMap())));
            assertEquals(2, relation.getApplicableRules().count());
        }
    }
//
    // TODO what is this test meant to do?
//    @Test
//    public void relationWithUnspecifiedRoles_typedRoleplayers_rolePlayerTypeMismatch(){
//        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)){
//            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();
//            String relationString = "{ ($x, $y, $z);$x isa singleRoleEntity; $y isa twoRoleEntity; $z isa threeRoleEntity; };";
//            RelationAtom relation = (RelationAtom) reasonerQueryFactory.atomic(conjunction(relationString)).getAtom();
//            ImmutableSetMultimap<Role, Variable> roleMap = ImmutableSetMultimap.of(
//                    tx.getRole("someRole"), new Variable("x"),
//                    tx.getRole("role"), new Variable("y"),
//                    tx.getRole("role"), new Variable("z"));
//            assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
//            assertEquals(2, relation.getApplicableRules().count());
//        }
//    }

    @Test //threeRoleEntity subs twoRoleEntity -> (role, role, role)
    public void relationWithUnspecifiedRoles_typedRoleplayers_typeHierarchyEnablesExtraRule(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)){
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String relationString = "{ ($x, $y, $z);$x isa monument; $y isa discoverer; $z isa ranked-hill-city; };";
            RelationAtom relation = (RelationAtom) reasonerQueryFactory.atomic(conjunction(relationString)).getAtom();
            relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().label())));
            // long-attributed, mixed-attributed, located-in-all-combinations
            assertEquals(3, relation.getApplicableRules().count());
        }
    }

    @Test
    public void relationWithUnspecifiedRoles_roleplayersBoundByAttributes(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)){
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;
            ReasonerQueryFactory reasonerQueryFactory = testTx.reasonerQueryFactory();

            //attribute mismatch with the rule
            String relationString = "{ ($x, $y) isa entities-with-attributes; $x has description 'nonexistant'; $y has name 'Big Rock'; };";

            //attributes matching with the ones from rule
            String relationString2 = "{ ($x, $y) isa entities-with-attributes; $x has description 'discovery is cool'; $y has name 'Big Rock'; };";

            //attributes matching with the ones from rule using 'contains'
            String relationString3 = "{ ($x, $y) isa entities-with-attributes; $x has description 'discovery is cool'; $y has name contains 'Rock'; };";

            //generic relation with attributes not matching the rule ones
            String relationString4 = "{ ($x, $y); $x has description 'nonexistant'; $y has name 'missing'; };";

            Atom relation = reasonerQueryFactory.create(conjunction(relationString)).getAtoms(RelationAtom.class).findFirst().orElse(null);
            Atom relation2 = reasonerQueryFactory.create(conjunction(relationString2)).getAtoms(RelationAtom.class).findFirst().orElse(null);
            Atom relation3 = reasonerQueryFactory.create(conjunction(relationString3)).getAtoms(RelationAtom.class).findFirst().orElse(null);
            Atom relation4 = reasonerQueryFactory.create(conjunction(relationString4)).getAtoms(RelationAtom.class).findFirst().orElse(null);
            Set<InferenceRule> rules = testTx.ruleCache().getRules().map(r -> new InferenceRule(r, reasonerQueryFactory)).collect(Collectors.toSet());

            // it is possible to match the rule that requires numerical values, when we only specify strings
            assertEquals(
                    rules.stream().filter(r -> r.getRule().label().getValue().contains("attributed-entities-relation")).collect(Collectors.toSet()),
                    relation.getApplicableRules().collect(toSet()));
            // with strings, we can confirm a rule matches, but also still may match the numerical valued rules
            assertEquals(
                    rules.stream().filter(r -> r.getRule().label().getValue().contains("attributed-entities-relation")).collect(toSet()),
                    relation2.getApplicableRules().collect(toSet()));
            assertEquals(
                    rules.stream().filter(r -> r.getRule().label().getValue().contains("attributed-entities-relation")).collect(toSet()),
                    relation3.getApplicableRules().collect(toSet()));


            // we can exclude the rules that infer attributes, relations should not trigger them
            int rulesInferringAttributeOwnerships = 2;
            assertEquals(
                    testTx.ruleCache().getRules().count() - rulesInferringAttributeOwnerships,
                    relation4.getApplicableRules().count());
        }
    }

    @Test
    public void typedAttributes(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;
            ReasonerQueryFactory reasonerQueryFactory = testTx.reasonerQueryFactory();

            String relationString = "{ $x isa located; $x has description $d; };";
            String relationString2 = "{ $x isa steep-path; $x has description $d; };";
            String relationString3 = "{ $x isa relation; $x has description $d; };";
            Atom attribute = reasonerQueryFactory.create(conjunction(relationString)).getAtoms(AttributeAtom.class).findFirst().orElse(null);
            Atom attribute2 = reasonerQueryFactory.create(conjunction(relationString2)).getAtoms(AttributeAtom.class).findFirst().orElse(null);
            Atom attribute3 = reasonerQueryFactory.create(conjunction(relationString3)).getAtoms(AttributeAtom.class).findFirst().orElse(null);
            Set<InferenceRule> attributeRules = testTx.ruleCache().getRules()
                    .map(r -> new InferenceRule(r, reasonerQueryFactory))
                    .filter(r -> r.getHead().getAtom().isAttributeAtom())
                    .collect(Collectors.toSet());

            assertEquals(
                    attributeRules.stream().filter(r -> !r.getRule().label().equals(Label.of("elevation-ranking-description"))).collect(toSet()),
                    attribute.getApplicableRules().collect(toSet()));
            assertEquals(
                    attributeRules.stream().filter(r -> !r.getRule().label().equals(Label.of("located-description"))).collect(toSet()),
                    attribute2.getApplicableRules().collect(toSet()));
            assertEquals(attributeRules, attribute3.getApplicableRules().collect(toSet()));
        }
    }

    @Test
    public void inferrableTypes(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;
            ReasonerQueryFactory reasonerQueryFactory = testTx.reasonerQueryFactory();

            String typeString = "{ $x isa elevation-ranking; };";
            String typeString2 = "{ $x isa located; };";
            String typeString3 = "{ $x isa description; };";
            String typeString4 = "{ $x isa attribute; };";
            String typeString5 = "{ $x isa relation; };";
            Atom type = reasonerQueryFactory.atomic(conjunction(typeString)).getAtom();
            Atom type2 = reasonerQueryFactory.atomic(conjunction(typeString2)).getAtom();
            Atom type3 = reasonerQueryFactory.atomic(conjunction(typeString3)).getAtom();
            Atom type4 = reasonerQueryFactory.atomic(conjunction(typeString4)).getAtom();
            Atom type5 = reasonerQueryFactory.atomic(conjunction(typeString5)).getAtom();

            List<InferenceRule> rules = testTx.ruleCache().getRules().map(r -> new InferenceRule(r, reasonerQueryFactory)).collect(Collectors.toList());
            assertEquals(2, type.getApplicableRules().count());
            assertEquals(1, type2.getApplicableRules().count());
            assertEquals(2, type3.getApplicableRules().count());
            assertEquals(rules.stream().filter(r -> r.getHead().getAtom().isAttributeAtom()).count(), type4.getApplicableRules().count());
            assertEquals(rules.stream().filter(r -> r.getHead().getAtom().isRelationAtom()).count(), type5.getApplicableRules().count());
        }
    }

    @Test //should assign (role: $x, role: $y) which is compatible with all rules
    public void genericRelation(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;
            ReasonerQueryFactory reasonerQueryFactory = testTx.reasonerQueryFactory();

            String relationString = "{ ($x, $y); };";
            Atom relation = reasonerQueryFactory.atomic(conjunction(relationString)).getAtom();
            Set<Rule> rulesInferringRelations = testTx.ruleCache().getRules()
                    .filter(rule -> rule.thenTypes().anyMatch(type -> type.isRelationType()))
                    .collect(toSet());
            assertEquals(
                    rulesInferringRelations.size(),
                    relation.getApplicableRules().count()
            );
        }
    }

    @Test
    public void genericTypeWithPossibleBounds(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;
            ReasonerQueryFactory reasonerQueryFactory = testTx.reasonerQueryFactory();

            String typeString = "{ $x isa $type; };";
            String typeString2 = "{ $x isa $type;$x isa entity; };";
            String typeString3 = "{ $x isa $type;$x isa relation; };";
            Atom type = reasonerQueryFactory.atomic(conjunction(typeString)).getAtom();
            Atom type2 = reasonerQueryFactory.atomic(conjunction(typeString2)).getAtom();
            Atom type3 = reasonerQueryFactory.create(conjunction(typeString3)).getAtoms(IsaAtom.class).filter(at -> at.getSchemaConcept() == null).findFirst().orElse(null);

            assertEquals(testTx.ruleCache().getRules().count(), type.getApplicableRules().count());
            assertThat(type2.getApplicableRules().collect(toSet()), empty());
            assertEquals(testTx.ruleCache().getRules().filter(r -> r.thenTypes().allMatch(Concept::isRelationType)).count(), type3.getApplicableRules().count());
        }
    }


    @Test
    public void genericTypeAsARoleplayer(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;
            ReasonerQueryFactory reasonerQueryFactory = testTx.reasonerQueryFactory();

            String typeString = "{ (ternary-steep-path_middle-end: $x); $x isa $type; };";
            String typeString2 = "{ (elevation-ranking_lower-path: $x); $x isa $type; };";
            // get Atoms of $type
            Atom type = reasonerQueryFactory.create(conjunction(typeString)).getAtoms(IsaAtom.class).findFirst().orElse(null);
            Atom type2 = reasonerQueryFactory.create(conjunction(typeString2)).getAtoms(IsaAtom.class).findFirst().orElse(null);
            // $type can only be an entity, so no rules derive it
            assertThat(type.getApplicableRules().collect(toSet()), empty());
            // $type can be relation or an entity, so some rules may apply
            assertEquals(
                    testTx.ruleCache().getRules().filter(r -> r.thenTypes().anyMatch(c ->
                            c.label().equals(Label.of("steep-path")) || c.label().equals(Label.of("ternary-steep-path")))
                    ).count(),
                    type2.getApplicableRules().count()
            );
        }
    }


    @Test
    public void relationWithUnspecifiedRoles_someRoleplayersTyped(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String relationString = "{ ($x, $y, attributed:$z); $y isa monument; $z isa ranked-hill-city; };";
            RelationAtom relation = (RelationAtom) reasonerQueryFactory.atomic(conjunction(relationString)).getAtom();
            ImmutableSetMultimap<Role, Variable> roleMap = ImmutableSetMultimap.of(
                    tx.getRole("role"), new Variable("x"),
                    tx.getRole("role"), new Variable("y"),
                    tx.getRole("attributed"), new Variable("z"));
            assertEquals(roleMap, roleSetMap((relation.getRoleVarMap())));
            assertEquals(2, relation.getApplicableRules().count());;
        }
    }

    @Test //NB: subRole sub someRole
    public void relationWithRepeatingRoles_roleHierarchy(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String relationString = "{ (path_end: $x1, path_end: $x2, steep-path_lower-end: $x3); };";
            String relationString2 = "{ (path_end: $x1, steep-path_lower-end: $x2, steep-path_lower-end: $x3); };";
            String relationString3 = "{ (steep-path_lower-end: $x1, steep-path_lower-end: $x2, steep-path_lower-end: $x3); };";
            Atom relation = reasonerQueryFactory.atomic(conjunction(relationString)).getAtom();
            Atom relation2 = reasonerQueryFactory.atomic(conjunction(relationString2)).getAtom();
            Atom relation3 = reasonerQueryFactory.atomic(conjunction(relationString3)).getAtom();
            assertEquals(2, relation.getApplicableRules().count());
            assertEquals(1, relation2.getApplicableRules().count());
            assertThat(relation3.getApplicableRules().collect(toSet()), empty());
        }
    }

    @Test
    public void genericRelationWithGenericallyTypedRolePlayer(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            TestTransactionProvider.TestTransaction testTx = (TestTransactionProvider.TestTransaction)tx;
            ReasonerQueryFactory reasonerQueryFactory = testTx.reasonerQueryFactory();

            String relationString = "{ ($x, $y);$x isa root; };";
            String relationString2 = "{ ($x, $y);$x isa entity; };";
            String relationString3 = "{ ($x, $y);$x isa relation; };";
            Atom relation = reasonerQueryFactory.atomic(conjunction(relationString)).getAtom();
            Atom relation2 = reasonerQueryFactory.atomic(conjunction(relationString2)).getAtom();
            Atom relation3 = reasonerQueryFactory.create(conjunction(relationString3)).getAtoms(RelationAtom.class).findFirst().orElse(null);


            assertEquals(7, relation.getApplicableRules().count());
            assertEquals(7, relation2.getApplicableRules().count());
            assertEquals(1, relation3.getApplicableRules().count());
        }
    }

    @Test
    public void relationsWithTypedRolePlayer(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String relationString = "{ (ranked: $x, ranked: $y) isa elevation-ranking; };";
            String relationString2 = "{ $x isa entity;(ranked: $x, ranked: $y) isa elevation-ranking; };";
            String relationString3 = "{ $x isa city;(ranked: $x, ranked: $y) isa elevation-ranking; };";
            String relationString4 = "{ $x isa ranked-hill-city;(ranked: $x, ranked: $y) isa elevation-ranking; };";

            Atom relation = reasonerQueryFactory.atomic(conjunction(relationString)).getAtom();
            Atom relation2 = reasonerQueryFactory.atomic(conjunction(relationString2)).getAtom();
            Atom relation3 = reasonerQueryFactory.atomic(conjunction(relationString3)).getAtom();
            Atom relation4 = reasonerQueryFactory.atomic(conjunction(relationString4)).getAtom();
            assertEquals(2, relation.getApplicableRules().count());
            assertEquals(1, relation2.getApplicableRules().count());
            assertEquals(1, relation3.getApplicableRules().count());
            assertEquals(1, relation4.getApplicableRules().count());
        }
    }

    @Test
    public void relationWithUnspecifiedRoles_typedRoleplayers_typePlayabilityDeterminesApplicability(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            TestTransactionProvider.TestTransaction testTx = ((TestTransactionProvider.TestTransaction)tx);
            ReasonerQueryFactory reasonerQueryFactory = testTx.reasonerQueryFactory();

            //inferred relation (role {steep-path_lower-end, located-item, string-attributed, long-attributed} : $x, role {located-by, string-attributed, long-attributed} : $y)
            String relationString = "{ ($x, $y);$x isa monument; $y isa discoverer; };";

            //inferred relation: (string-attributed: $x, string-attributed: $y) -- no rules can also satisfy with-string
            String relationString2 = "{ ($x, $y);$x isa with-string; $y isa with-string; };";

            //inferred relation: (someRole: $x, role {located-by, string-attributed, long-attributed}: $y)
            String relationString3 = "{ ($x, $y);$x isa with-string; $y isa discoverer; };";

            //inferred relation: (role {located-by, string-attributed, long-attributed}: $x, role {located-by, string-attributed, long-attributed}: $y)
            String relationString4 = "{ ($x, $y);$x isa discoverer; $y isa discoverer; };";

            //won't match any rules because no rules allow trigger with any combination of the roles monument can play
            String relationString5 = "{ ($x, $y, $z);$x isa monument; $y isa monument; $z isa monument;};";


            Atom relation = reasonerQueryFactory.atomic(conjunction(relationString)).getAtom();
            Atom relation2 = reasonerQueryFactory.atomic(conjunction(relationString2)).getAtom();
            Atom relation3 = reasonerQueryFactory.atomic(conjunction(relationString3)).getAtom();
            Atom relation4 = reasonerQueryFactory.atomic(conjunction(relationString4)).getAtom();
            Atom relation5 = reasonerQueryFactory.atomic(conjunction(relationString5)).getAtom();

            assertEquals(3, relation.getApplicableRules().count());
            assertEquals(0, relation2.getApplicableRules().count());
            assertEquals(1, relation3.getApplicableRules().count());
            assertEquals(2, relation4.getApplicableRules().count());
            assertEquals(0, relation5.getApplicableRules().count());
        }
    }

    @Test
    public void typePlayabilityDeterminesRuleApplicability(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String relationString = "{ $y isa city;(steep-path_lower-end:$x, role:$y, path_end: $z) isa ternary-steep-path; };";

            String relationString2 = "{ $y isa hill-city;(steep-path_lower-end:$x, ternary-steep-path_middle-end:$y, path_end: $z) isa ternary-steep-path; };";

            String relationString3 = "{ $y isa ranked-hill-city;(steep-path_lower-end:$x, ternary-steep-path_middle-end:$y, path_end: $z) isa ternary-steep-path; };";

            String relationString4 = "{ $y isa root;(steep-path_lower-end:$x, ternary-steep-path_middle-end:$y, path_end: $z) isa ternary-steep-path; };";

            String relationString5 = "{ $y isa entity;(steep-path_lower-end:$x, ternary-steep-path_middle-end:$y, path_end: $z) isa ternary-steep-path; };";
            Atom relation = reasonerQueryFactory.atomic(conjunction(relationString)).getAtom();
            Atom relation2 = reasonerQueryFactory.atomic(conjunction(relationString2)).getAtom();
            Atom relation3 = reasonerQueryFactory.atomic(conjunction(relationString3)).getAtom();
            Atom relation4 = reasonerQueryFactory.atomic(conjunction(relationString4)).getAtom();
            Atom relation5 = reasonerQueryFactory.atomic(conjunction(relationString5)).getAtom();

            Rule ternaryRule1 = tx.getRule("ternary-steep-path-from-steep-paths");
            Rule ternaryRule2 = tx.getRule("flat-ternary-steep-path");
            assertEquals(new HashSet<>(Arrays.asList(ternaryRule1, ternaryRule2)), relation.getApplicableRules().map(r -> r.getRule()).collect(toSet()));
            assertEquals(ternaryRule1, Iterables.getOnlyElement(relation2.getApplicableRules().collect(toSet())).getRule());
            assertEquals(ternaryRule1, Iterables.getOnlyElement(relation3.getApplicableRules().collect(toSet())).getRule());
            assertEquals(ternaryRule1, Iterables.getOnlyElement(relation4.getApplicableRules().collect(toSet())).getRule());
            assertEquals(ternaryRule1, Iterables.getOnlyElement(relation5.getApplicableRules().collect(toSet())).getRule());
        }
    }

    @Test
    public void typePlayabilityDeterminesRuleApplicability_matchSemantics() {
        Session session = SessionUtil.serverlessSessionWithNewKeyspace(storage.createCompatibleServerConfig());
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            String schema = "define " +
                    "sourceEntity sub entity, plays sourceSubRole, plays symmetricRole;" +
                    "targetEntity sub entity, plays targetSubRole;" +
                    "baseRelation sub relation, relates sourceBaseRole, relates targetBaseRole;" +
                    "specialisedRelation sub baseRelation, relates sourceSubRole as sourceBaseRole, relates targetSubRole as targetBaseRole;" +
                    "derivedRelation sub relation, relates symmetricRole;" +
                    "someRule sub rule," +
                    "when {" +
                    "    (sourceBaseRole: $x, targetBaseRole: $o);" +
                    "    (sourceBaseRole: $y, targetBaseRole: $o);" +
                    "    $x != $y;" +
                    "}," +
                    "then {" +
                    "    (symmetricRole: $x, symmetricRole: $y) isa derivedRelation;" +
                    "};";
            String data = "insert " +
                    "$o isa targetEntity;" +
                    "$a isa sourceEntity;" +
                    "$b isa sourceEntity;" +
                    "(sourceSubRole: $a, targetSubRole: $o) isa specialisedRelation;" +
                    "(sourceSubRole: $b, targetSubRole: $o) isa specialisedRelation;";
            tx.execute(Graql.parse(schema).asDefine());
            tx.execute(Graql.parse(data).asInsert());
            tx.commit();
        }
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();
            String id = tx.getEntityType("sourceEntity").instances().iterator().next().id().getValue();
            String basePattern = "{ (symmetricRole: $x, symmetricRole: $y) isa derivedRelation; };";
            String typedPattern = "{ $x isa sourceEntity; (symmetricRole: $x, symmetricRole: $y) isa derivedRelation; };";
            String subbedPattern = "{ $x id " + id + "; (symmetricRole: $x, symmetricRole: $y) isa derivedRelation; };";
            Atom relation = reasonerQueryFactory.atomic(conjunction(basePattern)).getAtom();
            Atom typedRelation = reasonerQueryFactory.atomic(conjunction(typedPattern)).getAtom();
            Atom subbedRelation = reasonerQueryFactory.atomic(conjunction(subbedPattern)).getAtom();

            Rule rule = tx.getRule("someRule");
            assertEquals(rule, Iterables.getOnlyElement(relation.getApplicableRules().collect(toSet())).getRule());
            assertEquals(rule, Iterables.getOnlyElement(typedRelation.getApplicableRules().collect(toSet())).getRule());
            assertEquals(rule, Iterables.getOnlyElement(subbedRelation.getApplicableRules().collect(toSet())).getRule());
        }
    }

    /*
    when we have a specific instance, we only need to check compatibility of rules based on inheritance
     */
    @Test
    public void relationWithUnspecifiedRoles_specifyingRolePlayerMakesRuleInapplicable(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            Concept concept = tx.getType(Label.of("root")).instancesDirect().findAny().get();
            String relationString = "{" +
                    "($x, $y) isa ternary-steep-path;" +
                    "$x id " + concept.id().getValue() + ";" +
                    "};";
            Atom relation = reasonerQueryFactory.atomic(conjunction(relationString)).getAtom();
            assertThat(relation.getApplicableRules().collect(toSet()), empty());
        }
    }

    /*
    when we have a specific instance, we only need to check compatibility of rules based on inheritance
    */
    @Test
    public void relationWithUnspecifiedRoles_specifyingRolePlayerMakesRuleInapplicable_noRelationType(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            Concept concept = tx.getType(Label.of("root")).instancesDirect().findAny().get();
            String relationString = "{" +
                    "($x, $y);" +
                    "$x id " + concept.id().getValue() + ";" +
                    "};";

            Atom relation = reasonerQueryFactory.atomic(conjunction(relationString)).getAtom();
            assertThat(relation.getApplicableRules().collect(toSet()), empty());
        }
    }

    @Test
    public void whenMatchingDoubleAttributesWithDifferentValueRanges_rulesWithCompatibleRangesAreMatched(){
        try(Transaction tx = resourceApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String resourceString = "{ $x has res-double > 3.0; };";
            String resourceString2 = "{ $x has res-double > 4.0; };";
            String resourceString3 = "{ $x has res-double < 3.0; };";
            String resourceString4 = "{ $x has res-double < 4.0; };";
            String resourceString5 = "{ $x has res-double >= 5.0; };";
            String resourceString6 = "{ $x has res-double <= 5.0; };";
            String resourceString7 = "{ $x has res-double == 3.14; };";
            String resourceString8 = "{ $x has res-double !== 5.0; };";

            Atom resource = reasonerQueryFactory.atomic(conjunction(resourceString)).getAtom();
            Atom resource2 = reasonerQueryFactory.atomic(conjunction(resourceString2)).getAtom();
            Atom resource3 = reasonerQueryFactory.atomic(conjunction(resourceString3)).getAtom();
            Atom resource4 = reasonerQueryFactory.atomic(conjunction(resourceString4)).getAtom();
            Atom resource5 = reasonerQueryFactory.atomic(conjunction(resourceString5)).getAtom();
            Atom resource6 = reasonerQueryFactory.atomic(conjunction(resourceString6)).getAtom();
            Atom resource7 = reasonerQueryFactory.atomic(conjunction(resourceString7)).getAtom();
            Atom resource8 = reasonerQueryFactory.atomic(conjunction(resourceString8)).getAtom();

            assertEquals(1, resource.getApplicableRules().count());
            assertThat(resource2.getApplicableRules().collect(toSet()), empty());
            assertThat(resource3.getApplicableRules().collect(toSet()), empty());
            assertEquals(1, resource4.getApplicableRules().count());
            assertThat(resource5.getApplicableRules().collect(toSet()), empty());
            assertEquals(1, resource6.getApplicableRules().count());
            assertEquals(1, resource7.getApplicableRules().count());
            assertEquals(1, resource8.getApplicableRules().count());
        }
    }

    @Test
    public void whenMatchingLongAttributesWithDifferentValueRanges_rulesWithCompatibleRangesAreMatched(){
        try(Transaction tx = resourceApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String resourceString = "{ $x has res-long > 100; };";
            String resourceString2 = "{ $x has res-long > 150; };";
            String resourceString3 = "{ $x has res-long < 100; };";
            String resourceString4 = "{ $x has res-long < 200; };";
            String resourceString5 = "{ $x has res-long >= 130; };";
            String resourceString6 = "{ $x has res-long <= 130; };";
            String resourceString7 = "{ $x has res-long == 123; };";
            String resourceString8 = "{ $x has res-long !== 200; };";

            Atom resource = reasonerQueryFactory.atomic(conjunction(resourceString)).getAtom();
            Atom resource2 = reasonerQueryFactory.atomic(conjunction(resourceString2)).getAtom();
            Atom resource3 = reasonerQueryFactory.atomic(conjunction(resourceString3)).getAtom();
            Atom resource4 = reasonerQueryFactory.atomic(conjunction(resourceString4)).getAtom();
            Atom resource5 = reasonerQueryFactory.atomic(conjunction(resourceString5)).getAtom();
            Atom resource6 = reasonerQueryFactory.atomic(conjunction(resourceString6)).getAtom();
            Atom resource7 = reasonerQueryFactory.atomic(conjunction(resourceString7)).getAtom();
            Atom resource8 = reasonerQueryFactory.atomic(conjunction(resourceString8)).getAtom();

            assertEquals(1, resource.getApplicableRules().count());
            assertThat(resource2.getApplicableRules().collect(toSet()), empty());
            assertThat(resource3.getApplicableRules().collect(toSet()), empty());
            assertEquals(1, resource4.getApplicableRules().count());
            assertThat(resource5.getApplicableRules().collect(toSet()), empty());
            assertEquals(1, resource6.getApplicableRules().count());
            assertEquals(1, resource7.getApplicableRules().count());
            assertEquals(1, resource8.getApplicableRules().count());
        }
    }

    @Test
    public void whenMatchingStringAttributesWithDifferentValueDefinitions_rulesWithCompatibleValuesAreMatched(){
        try(Transaction tx = resourceApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String resourceString = "{ $x has res-string contains 'ing'; };";
            String resourceString2 = "{ $x has res-string 'test' ; };";
            String resourceString3 = "{ $x has res-string like \".*(fast|string).*\"; };";
            String resourceString4 = "{ $x has res-string like \".*\"; };";

            Atom resource = reasonerQueryFactory.atomic(conjunction(resourceString)).getAtom();
            Atom resource2 = reasonerQueryFactory.atomic(conjunction(resourceString2)).getAtom();
            Atom resource3 = reasonerQueryFactory.atomic(conjunction(resourceString3)).getAtom();
            Atom resource4 = reasonerQueryFactory.atomic(conjunction(resourceString4)).getAtom();

            assertEquals(1, resource.getApplicableRules().count());
            assertThat(resource2.getApplicableRules().collect(toSet()), empty());
            assertEquals(1, resource3.getApplicableRules().count());
            assertEquals(1, resource4.getApplicableRules().count());
        }
    }

    @Test
    public void whenMatchingBooleanAttributes_rulesWithCompatibleValuesAreMatched(){
        try(Transaction tx = resourceApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String resourceString = "{ $x has res-boolean 'true'; };";
            String resourceString2 = "{ $x has res-boolean 'false'; };";

            Atom resource = reasonerQueryFactory.atomic(conjunction(resourceString)).getAtom();
            Atom resource2 = reasonerQueryFactory.atomic(conjunction(resourceString2)).getAtom();

            assertEquals(1, resource.getApplicableRules().count());
            assertThat(resource2.getApplicableRules().collect(toSet()), empty());
        }
    }

    @Test
    public void whenMatchingTypesWithIllegalAttributes_noRulesAreMatched(){
        try(Transaction tx = resourceApplicabilitySession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String resourceString = "{ $x isa someEntity, has resource $r; };";
            String resourceString2 = "{ $x isa anotherEntity, has resource $r; };";
            String resourceString3 = "{ $x isa anotherEntity, has resource 'test'; };";

            Atom resource = reasonerQueryFactory.atomic(conjunction(resourceString)).getAtom();
            Atom resource2 = reasonerQueryFactory.atomic(conjunction(resourceString2)).getAtom();
            Atom resource3 = reasonerQueryFactory.atomic(conjunction(resourceString3)).getAtom();

            assertEquals(1, resource.getApplicableRules().count());
            assertThat(resource2.getApplicableRules().collect(toSet()), empty());
            assertThat(resource3.getApplicableRules().collect(toSet()), empty());
        }
    }

    @Test
    public void whenMatchingRelationsWithDoubleAttributeRolePlayers_rulesWithCompatibleValuesAreMatched(){
        try(Transaction tx = explicitOwnershipRelationSession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String queryString = "{ $x > 3.0 isa res-double; ($x, $y); };";
            String queryString2 = "{ $x > 4.0 isa res-double; ($x, $y); };";
            String queryString3 = "{ $x < 3.0 isa res-double; ($x, $y); };";
            String queryString4 = "{ $x < 4.0 isa res-double; ($x, $y); };";
            String queryString5 = "{ $x >= 5.0 isa res-double; ($x, $y); };";
            String queryString6 = "{ $x <= 5.0 isa res-double; ($x, $y); };";
            String queryString7 = "{ $x == 3.14 isa res-double; ($x, $y); };";
            String queryString8 = "{ $x !== 5.0 isa res-double; ($x, $y); };";

            Atom atom = reasonerQueryFactory.atomic(conjunction(queryString)).getAtom();
            Atom atom2 = reasonerQueryFactory.atomic(conjunction(queryString2)).getAtom();
            Atom atom3 = reasonerQueryFactory.atomic(conjunction(queryString3)).getAtom();
            Atom atom4 = reasonerQueryFactory.atomic(conjunction(queryString4)).getAtom();
            Atom atom5 = reasonerQueryFactory.atomic(conjunction(queryString5)).getAtom();
            Atom atom6 = reasonerQueryFactory.atomic(conjunction(queryString6)).getAtom();
            Atom atom7 = reasonerQueryFactory.atomic(conjunction(queryString7)).getAtom();
            Atom atom8 = reasonerQueryFactory.atomic(conjunction(queryString8)).getAtom();

            assertEquals(1, atom.getApplicableRules().count());
            assertThat(atom2.getApplicableRules().collect(toSet()), empty());
            assertThat(atom3.getApplicableRules().collect(toSet()), empty());
            assertEquals(1, atom4.getApplicableRules().count());
            assertThat(atom5.getApplicableRules().collect(toSet()), empty());
            assertEquals(1, atom6.getApplicableRules().count());
            assertEquals(1, atom7.getApplicableRules().count());
            assertEquals(1, atom8.getApplicableRules().count());
        }
    }

    @Test
    public void whenMatchingRelationsWithLongAttributeRolePlayers_rulesWithCompatibleValuesAreMatched(){
        try(Transaction tx = explicitOwnershipRelationSession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String queryString = "{ $x > 100 isa res-long; ($x, $y); };";
            String queryString2 = "{ $x > 150 isa res-long; ($x, $y); };";
            String queryString3 = "{ $x < 100 isa res-long; ($x, $y); };";
            String queryString4 = "{ $x < 200 isa res-long; ($x, $y); };";
            String queryString5 = "{ $x >= 130 isa res-long; ($x, $y); };";
            String queryString6 = "{ $x <= 130 isa res-long; ($x, $y); };";
            String queryString7 = "{ $x == 123 isa res-long; ($x, $y); };";
            String queryString8 = "{ $x !== 200 isa res-long; ($x, $y); };";

            Atom atom = reasonerQueryFactory.atomic(conjunction(queryString)).getAtom();
            Atom atom2 = reasonerQueryFactory.atomic(conjunction(queryString2)).getAtom();
            Atom atom3 = reasonerQueryFactory.atomic(conjunction(queryString3)).getAtom();
            Atom atom4 = reasonerQueryFactory.atomic(conjunction(queryString4)).getAtom();
            Atom atom5 = reasonerQueryFactory.atomic(conjunction(queryString5)).getAtom();
            Atom atom6 = reasonerQueryFactory.atomic(conjunction(queryString6)).getAtom();
            Atom atom7 = reasonerQueryFactory.atomic(conjunction(queryString7)).getAtom();
            Atom atom8 = reasonerQueryFactory.atomic(conjunction(queryString8)).getAtom();

            assertEquals(1, atom.getApplicableRules().count());
            assertThat(atom2.getApplicableRules().collect(toSet()), empty());
            assertThat(atom3.getApplicableRules().collect(toSet()), empty());
            assertEquals(1, atom4.getApplicableRules().count());
            assertThat(atom5.getApplicableRules().collect(toSet()), empty());
            assertEquals(1, atom6.getApplicableRules().count());
            assertEquals(1, atom7.getApplicableRules().count());
            assertEquals(1, atom8.getApplicableRules().count());
        }
    }

    @Test
    public void whenMatchingRelationsWithStringAttributeRolePlayers_rulesWithCompatibleValuesAreMatched(){
        try(Transaction tx = explicitOwnershipRelationSession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String queryString = "{ $x contains 'val' isa res-string; ($x, $y); };";
            String queryString2 = "{ $x 'test' isa res-string; ($x, $y); };";
            String queryString3 = "{ $x like \".*(fast|value).*\" isa res-string; ($x, $y); };";
            String queryString4 = "{ $x like \".*\" isa res-string; ($x, $y); };";

            Atom atom = reasonerQueryFactory.atomic(conjunction(queryString)).getAtom();
            Atom atom2 = reasonerQueryFactory.atomic(conjunction(queryString2)).getAtom();
            Atom atom3 = reasonerQueryFactory.atomic(conjunction(queryString3)).getAtom();
            Atom atom4 = reasonerQueryFactory.atomic(conjunction(queryString4)).getAtom();

            assertEquals(1, atom.getApplicableRules().count());
            assertThat(atom2.getApplicableRules().collect(toSet()), empty());
            assertEquals(1, atom3.getApplicableRules().count());
            assertEquals(1, atom4.getApplicableRules().count());
        }
    }

    @Test
    public void whenMatchingRelationsWithBooleanAttributeRolePlayers_rulesWithCompatibleValuesAreMatched() {
        try(Transaction tx = explicitOwnershipRelationSession.transaction(Transaction.Type.WRITE)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            String queryString = "{ $x 'true' isa res-boolean;($x, $y); };";
            String queryString2 = "{ $x 'false' isa res-boolean;($x, $y); };";

            Atom atom = reasonerQueryFactory.atomic(conjunction(queryString)).getAtom();
            Atom atom2 = reasonerQueryFactory.atomic(conjunction(queryString2)).getAtom();

            assertEquals(1, atom.getApplicableRules().count());
            assertThat(atom2.getApplicableRules().collect(toSet()), empty());
        }
    }

    @Test
    public void whenMatchingRulesForGroundAtomRedefinedViaRule_ruleIsMatched(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.READ)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();
            Relation instance = tx.stream(Graql.parse("match $r isa located; get;").asGet()).map(ans -> ans.get("r").asRelation()).findFirst().get();
            String queryString = "{ $r has description 'in a fictional hilly world'; $r id " + instance.id().getValue() + "; };";
            Atom atom = reasonerQueryFactory.atomic(conjunction(queryString)).getAtom();

            assertTrue(atom.getApplicableRules().findFirst().isPresent());
        }
    }

    @Test
    public void whenMatchingRulesForGroundTypeWhichIsNotRedefined_noRulesAreMatched(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.READ)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            Relation instance = tx.getRelationType("steep-path").instances().findFirst().orElse(null);
            String queryString = "{ $x isa steep-path; $x id " + instance.id().getValue() + "; };";
            Atom atom = reasonerQueryFactory.atomic(conjunction(queryString)).getAtom();

            assertThat(atom.getApplicableRules().collect(toSet()), empty());
        }
    }

    /*
    we cant infer a relation concept that already exists by ID - so we don't need to try to apply any rules to it
     */
    @Test
    public void whenMatchingRulesForASpecificRelation_noRulesAreMatched(){
        try(Transaction tx = ruleApplicabilitySession.transaction(Transaction.Type.READ)) {
            ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

            Relation instance = tx.getRelationType("steep-path").instances().findFirst().orElse(null);
            String queryString = "{ $r ($x, $y) isa steep-path; $r id " + instance.id().getValue() + "; };";
            Atom atom = reasonerQueryFactory.atomic(conjunction(queryString)).getAtom();

            assertThat(atom.getApplicableRules().collect(toSet()), empty());
        }
    }

    private Conjunction<Statement> conjunction(String patternString){
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }

    private Multimap<Role, Variable> roleSetMap(Multimap<Role, Variable> roleVarMap) {
        Multimap<Role, Variable> roleMap = HashMultimap.create();
        roleVarMap.entries().forEach(e -> roleMap.put(e.getKey(), e.getValue()));
        return roleMap;
    }

    private Concept getConcept(Transaction tx, String typeName, String val){
        return tx.stream(Graql.match((Pattern) var("x").has(typeName, val)).get("x"))
                .map(ans -> ans.get("x")).findAny().orElse(null);
    }
}