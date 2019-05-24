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

package grakn.core.graql.reasoner.atomic;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import grakn.core.concept.Concept;
import grakn.core.concept.Label;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.Role;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.query.ReasonerQueries;
import grakn.core.graql.reasoner.rule.InferenceRule;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.kb.Schema;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static graql.lang.Graql.var;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
public class RuleApplicabilityIT {

    private static String resourcePath = "test-integration/graql/reasoner/resources/";

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl ruleApplicabilitySession;
    private static SessionImpl resourceApplicabilitySession;
    private static SessionImpl reifiedResourceApplicabilitySession;

    private static Thing putEntityWithResource(TransactionOLTP tx, EntityType type, Label resource, Object value) {
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
        resourceApplicabilitySession = server.sessionWithNewKeyspace();
        loadFromFileAndCommit(resourcePath,"resourceApplicabilityTest.gql", resourceApplicabilitySession);
        reifiedResourceApplicabilitySession = server.sessionWithNewKeyspace();
        loadFromFileAndCommit(resourcePath,"reifiedResourceApplicabilityTest.gql", reifiedResourceApplicabilitySession);


        ruleApplicabilitySession = server.sessionWithNewKeyspace();
        loadFromFileAndCommit(resourcePath,"ruleApplicabilityTest.gql", ruleApplicabilitySession);

        //add extra data so that all rules can be possibly triggered
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {

            EntityType singleRoleEntity = tx.getEntityType("singleRoleEntity");
            EntityType twoRoleEntity = tx.getEntityType("twoRoleEntity");
            Thing x = putEntityWithResource(tx, singleRoleEntity, Label.of("resource-string"), "someValue");
            Thing y = putEntityWithResource(tx, twoRoleEntity, Label.of("resource-long"), 2000L);

            Role someRole = tx.getRole("someRole");
            Role subRole = tx.getRole("subRole");
            tx.getRelationType("binary")
                    .create()
                    .assign(someRole, x)
                    .assign(subRole, y)
                    .has(tx.getAttributeType("description").create("someDescription"));
            tx.commit();
        }
    }

    @AfterClass
    public static void closeSession(){
        ruleApplicabilitySession.close();
        resourceApplicabilitySession.close();
        reifiedResourceApplicabilitySession.close();
    }

    @Test
    public void testRuleApplicability_OntologicalAtomsDoNotMatchAnyRules(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            Atom subAtom = ReasonerQueries.atomic(conjunction("{ $x sub relation; };", tx), tx).getAtom();
            Atom hasAtom = ReasonerQueries.atomic(conjunction("{ $x has description; };", tx), tx).getAtom();
            Atom relatesAtom = ReasonerQueries.atomic(conjunction("{ reifiable-relation relates $x; };", tx), tx).getAtom();
            Atom relatesAtom2 = ReasonerQueries.atomic(conjunction("{ $x relates someRole; };", tx), tx).getAtom();
            Atom playsAtom = ReasonerQueries.atomic(conjunction("{ $x plays someRole; };", tx), tx).getAtom();
            assertThat(subAtom.getApplicableRules().collect(toSet()), empty());
            assertThat(hasAtom.getApplicableRules().collect(toSet()), empty());
            assertThat(relatesAtom.getApplicableRules().collect(toSet()), empty());
            assertThat(relatesAtom2.getApplicableRules().collect(toSet()), empty());
            assertThat(playsAtom.getApplicableRules().collect(toSet()), empty());
        }
    }

    @Test
    public void testRuleApplicability_AmbiguousRoleMapping(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            //although singleRoleEntity plays only one role it can also play an implicit role of the resource so mapping ambiguous
            String relationString = "{ ($x, $y, $z);$x isa singleRoleEntity; $y isa anotherTwoRoleEntity; $z isa twoRoleEntity; };";
            RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
            ImmutableSetMultimap<Role, Variable> roleMap = ImmutableSetMultimap.of(
                    tx.getRole("role"), new Variable("x"),
                    tx.getRole("role"), new Variable("y"),
                    tx.getRole("role"), new Variable("z"));
            assertEquals(roleMap, roleSetMap((relation.getRoleVarMap())));
            assertEquals(5, relation.getApplicableRules().count());
        }
    }

    @Test
    public void testRuleApplicability_AmbiguousRoleMapping_RolePlayerTypeMismatch(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()){
            //although singleRoleEntity plays only one role it can also play an implicit role of the resource so mapping ambiguous
            String relationString = "{ ($x, $y, $z);$x isa singleRoleEntity; $y isa twoRoleEntity; $z isa threeRoleEntity; };";
            RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
            ImmutableSetMultimap<Role, Variable> roleMap = ImmutableSetMultimap.of(
                    tx.getRole("role"), new Variable("x"),
                    tx.getRole("role"), new Variable("y"),
                    tx.getRole("role"), new Variable("z"));
            assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
            assertEquals(2, relation.getApplicableRules().count());
        }
    }

    @Test //threeRoleEntity subs twoRoleEntity -> (role, role, role)
    public void testRuleApplicability_AmbiguousRoleMapping_TypeHierarchyEnablesExtraRule(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()){
            String relationString = "{ ($x, $y, $z);$x isa twoRoleEntity; $y isa threeRoleEntity; $z isa anotherTwoRoleEntity; };";
            RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
            relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().label())));
            assertEquals(2, relation.getApplicableRules().count());
        }
    }

    @Test
    public void testRuleApplicability_attributeBoundRelationPlayers(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()){

            //attribute mismatch with the rule
            String relationString = "{ ($x, $y) isa attributed-relation;$x has resource-string 'valueOne'; $y has resource-string 'someValue'; };";

            //attributes overlap with the ones from rule
            String relationString2 = "{ ($x, $y) isa attributed-relation;$x has resource-string 'someValue'; $y has resource-string 'yetAnotherValue'; };";

            //attributes overlap with the ones from rule
            String relationString3 = "{ ($x, $y) isa attributed-relation;$x has resource-string 'someValue'; $y has resource-string contains 'Value'; };";

            //generic relation with attributes not matching the rule ones
            String relationString4 = "{ ($x, $y);$x has resource-string 'valueOne'; $y has resource-string 'valueTwo'; };";

            Atom relation = ReasonerQueries.create(conjunction(relationString, tx), tx).getAtoms(RelationAtom.class).findFirst().orElse(null);
            Atom relation2 = ReasonerQueries.create(conjunction(relationString2, tx), tx).getAtoms(RelationAtom.class).findFirst().orElse(null);
            Atom relation3 = ReasonerQueries.create(conjunction(relationString3, tx), tx).getAtoms(RelationAtom.class).findFirst().orElse(null);
            Atom relation4 = ReasonerQueries.create(conjunction(relationString4, tx), tx).getAtoms(RelationAtom.class).findFirst().orElse(null);
            Set<InferenceRule> rules = tx.ruleCache().getRules().map(r -> new InferenceRule(r, tx)).collect(Collectors.toSet());

            assertEquals(rules.stream().filter(r -> r.getRule().label().equals(Label.of("attributed-relation-long-rule"))).collect(Collectors.toSet()), relation.getApplicableRules().collect(toSet()));
            assertEquals(rules.stream().filter(r -> r.getRule().label().getValue().contains("attributed-relation")).collect(toSet()), relation2.getApplicableRules().collect(toSet()));
            assertEquals(rules.stream().filter(r -> r.getRule().label().getValue().contains("attributed-relation")).collect(toSet()), relation3.getApplicableRules().collect(toSet()));
            assertEquals(tx.ruleCache().getRules().count() - 1, relation4.getApplicableRules().count());
        }
    }

    @Test
    public void testRuleApplicability_nonSpecificattributeBoundRelationPlayers(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {

            //attributes satisfy the ones from rule
            String relationString = "{ ($x, $y) isa attributed-relation; " +
                    "$x has resource-long 1334; " +
                    "$y has resource-long 1607; };";

            //inequality attribute not satisfied
            String relationString2 = "{ " +
                    "($x, $y, $z) isa attributed-relation; " +
                    "$x has resource-long -1410; " +
                    "$y has resource-long 0; " +
                    "$z has resource-long 1667; " +
                    "};";

            //attributes with inequalities than have overlap with rule ones
            String relationString3 = "{ " +
                    "($x, $y) isa attributed-relation; " +
                    "$x has resource-long > -1667; " +
                    "$y has resource-long > 20; " +
                    "$z has resource-long < 2000; " +
                    "};";

            Atom relation = ReasonerQueries.create(conjunction(relationString, tx), tx).getAtoms(RelationAtom.class).findFirst().orElse(null);
            Atom relation2 = ReasonerQueries.create(conjunction(relationString2, tx), tx).getAtoms(RelationAtom.class).findFirst().orElse(null);
            Atom relation3 = ReasonerQueries.create(conjunction(relationString3, tx), tx).getAtoms(RelationAtom.class).findFirst().orElse(null);

            Set<InferenceRule> rules = tx.ruleCache().getRules().map(r -> new InferenceRule(r, tx)).collect(Collectors.toSet());

            assertEquals(
                    rules.stream().filter(r -> r.getRule().label().getValue().contains("attributed-relation")).collect(Collectors.toSet()),
                    relation.getApplicableRules().collect(Collectors.toSet()));
            assertEquals(
                    rules.stream().filter(r -> r.getRule().label().equals(Label.of("attributed-relation-string-rule"))).collect(Collectors.toSet()),
                    relation2.getApplicableRules().collect(Collectors.toSet()));
            assertEquals(
                    rules.stream().filter(r -> r.getRule().label().getValue().contains("attributed-relation")).collect(Collectors.toSet()),
                    relation3.getApplicableRules().collect(Collectors.toSet()));
        }
    }

    @Test
    public void testRuleApplicability_MissingRelationPlayers(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {

            //inferred relation (role {role2, role3} : $x, role {role1, role2} : $y)
            String relationString = "{ ($x, $y);$x isa twoRoleEntity; $y isa anotherTwoRoleEntity; };";

            //inferred relation: (role1: $x, role1: $y)
            String relationString2 = "{ ($x, $y);$x isa singleRoleEntity; $y isa singleRoleEntity; };";

            //inferred relation: (role1: $x, role {role1, role2}: $y)
            String relationString3 = "{ ($x, $y);$x isa singleRoleEntity; $y isa anotherTwoRoleEntity; };";

            //inferred relation: (role1: $x, role {role1, role2, role3}: $y)
            String relationString4 = "{ ($x, $y);$x isa singleRoleEntity; $y isa threeRoleEntity; };";

            //inferred relation: (role {role2, role3}: $x, role {role2, role3}: $y)
            String relationString5 = "{ ($x, $y);$x isa twoRoleEntity; $y isa twoRoleEntity; };";

            //inferred relation: (role {role1, role2}: $x, role {role1, role2}: $y)
            String relationString6 = "{ ($x, $y);$x isa anotherTwoRoleEntity; $y isa anotherTwoRoleEntity; };";

            Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
            Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, tx), tx).getAtom();
            Atom relation3 = ReasonerQueries.atomic(conjunction(relationString3, tx), tx).getAtom();
            Atom relation4 = ReasonerQueries.atomic(conjunction(relationString4, tx), tx).getAtom();
            Atom relation5 = ReasonerQueries.atomic(conjunction(relationString5, tx), tx).getAtom();
            Atom relation6 = ReasonerQueries.atomic(conjunction(relationString6, tx), tx).getAtom();

            assertEquals(6, relation.getApplicableRules().count());
            assertThat(relation2.getApplicableRules().collect(toSet()), empty());
            assertEquals(6, relation3.getApplicableRules().count());
            assertEquals(3, relation4.getApplicableRules().count());
            assertThat(relation5.getApplicableRules().collect(toSet()), empty());
            assertEquals(4, relation6.getApplicableRules().count());
        }
    }

    @Test //should assign (role : $x, role1: $y, role: $z) which is compatible with 3 ternary rules
    public void testRuleApplicability_WithWildcard(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            //although singleRoleEntity plays only one role it can also play an implicit role of the resource so mapping ambiguous
            String relationString = "{ ($x, $y, $z);$y isa singleRoleEntity; $z isa twoRoleEntity; };";
            RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
            ImmutableSetMultimap<Role, Variable> roleMap = ImmutableSetMultimap.of(
                    tx.getRole("role"), new Variable("x"),
                    tx.getRole("role"), new Variable("y"),
                    tx.getRole("role"), new Variable("z"));
            assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
            assertEquals(5, relation.getApplicableRules().count());
        }
    }

    @Test
    public void testRuleApplicability_TypedResources(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            String relationString = "{ $x isa reifiable-relation; $x has description $d; };";
            String relationString2 = "{ $x isa typed-relation; $x has description $d; };";
            String relationString3 = "{ $x isa relation; $x has description $d; };";
            Atom resource = ReasonerQueries.create(conjunction(relationString, tx), tx).getAtoms(AttributeAtom.class).findFirst().orElse(null);
            Atom resource2 = ReasonerQueries.create(conjunction(relationString2, tx), tx).getAtoms(AttributeAtom.class).findFirst().orElse(null);
            Atom resource3 = ReasonerQueries.create(conjunction(relationString3, tx), tx).getAtoms(AttributeAtom.class).findFirst().orElse(null);
            assertEquals(2, resource.getApplicableRules().count());
            assertEquals(2, resource2.getApplicableRules().count());
            assertEquals(3, resource3.getApplicableRules().count());
        }
    }

    @Test
    public void testRuleApplicability_DerivedTypes(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            String typeString = "{ $x isa reifying-relation; };";
            String typeString2 = "{ $x isa typed-relation; };";
            String typeString3 = "{ $x isa description; };";
            String typeString4 = "{ $x isa attribute; };";
            String typeString5 = "{ $x isa relation; };";
            Atom type = ReasonerQueries.atomic(conjunction(typeString, tx), tx).getAtom();
            Atom type2 = ReasonerQueries.atomic(conjunction(typeString2, tx), tx).getAtom();
            Atom type3 = ReasonerQueries.atomic(conjunction(typeString3, tx), tx).getAtom();
            Atom type4 = ReasonerQueries.atomic(conjunction(typeString4, tx), tx).getAtom();
            Atom type5 = ReasonerQueries.atomic(conjunction(typeString5, tx), tx).getAtom();

            List<InferenceRule> rules = tx.ruleCache().getRules().map(r -> new InferenceRule(r, tx)).collect(Collectors.toList());
            assertEquals(2, type.getApplicableRules().count());
            assertEquals(1, type2.getApplicableRules().count());
            assertEquals(3, type3.getApplicableRules().count());
            assertEquals(rules.stream().filter(r -> r.getHead().getAtom().isResource()).count(), type4.getApplicableRules().count());
            assertEquals(rules.stream().filter(r -> r.getHead().getAtom().isRelation()).count(), type5.getApplicableRules().count());
        }
    }

    @Test //should assign (role: $x, role: $y) which is compatible with all rules
    public void testRuleApplicability_genericRelation(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            String relationString = "{ ($x, $y); };";
            Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
            assertEquals(
                    tx.ruleCache().getRules().count(),
                    relation.getApplicableRules().count()
            );
        }
    }

    @Test
    public void testRuleApplicability_genericType(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            String typeString = "{ $x isa $type; };";
            String typeString2 = "{ $x isa $type;$x isa entity; };";
            String typeString3 = "{ $x isa $type;$x isa relation; };";
            Atom type = ReasonerQueries.atomic(conjunction(typeString, tx), tx).getAtom();
            Atom type2 = ReasonerQueries.atomic(conjunction(typeString2, tx), tx).getAtom();
            Atom type3 = ReasonerQueries.create(conjunction(typeString3, tx), tx).getAtoms(IsaAtom.class).filter(at -> at.getSchemaConcept() == null).findFirst().orElse(null);

            assertEquals(tx.ruleCache().getRules().count(), type.getApplicableRules().count());
            assertThat(type2.getApplicableRules().collect(toSet()), empty());
            assertEquals(tx.ruleCache().getRules().filter(r -> r.thenTypes().allMatch(Concept::isRelationType)).count(), type3.getApplicableRules().count());
        }
    }

    @Test
    public void testRuleApplicability_genericTypeAsARoleplayer(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            String typeString = "{ (symmetricRole: $x); $x isa $type; };";
            String typeString2 = "{ (someRole: $x); $x isa $type; };";
            Atom type = ReasonerQueries.create(conjunction(typeString, tx), tx).getAtoms(IsaAtom.class).findFirst().orElse(null);
            Atom type2 = ReasonerQueries.create(conjunction(typeString2, tx), tx).getAtoms(IsaAtom.class).findFirst().orElse(null);
            assertThat(type.getApplicableRules().collect(toSet()), empty());
            assertEquals(
                    tx.ruleCache().getRules().filter(r -> r.thenTypes().anyMatch(c -> c.label().equals(Label.of("binary")))).count(),
                    type2.getApplicableRules().count()
            );
        }
    }

    @Test
    public void testRuleApplicability_genericTypeWithBounds(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            String relationString = "{ $x isa $type; };";
            Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
            assertEquals(
                    tx.ruleCache().getRules().count(),
                    relation.getApplicableRules().count()
            );
        }
    }

    @Test
    public void testRuleApplicability_WithWildcard_MissingMappings(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            //although singleRoleEntity plays only one role it can also play an implicit role of the resource so mapping ambiguous
            String relationString = "{ ($x, $y, $z);$y isa singleRoleEntity; $z isa singleRoleEntity; };";
            RelationAtom relation = (RelationAtom) ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
            ImmutableSetMultimap<Role, Variable> roleMap = ImmutableSetMultimap.of(
                    tx.getRole("role"), new Variable("x"),
                    tx.getRole("role"), new Variable("y"),
                    tx.getRole("role"), new Variable("z"));
            assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
            assertThat(relation.getApplicableRules().collect(toSet()), empty());
        }
    }

    @Test //NB: role2 sub role1
    public void testRuleApplicability_RepeatingRoleTypesWithHierarchy(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            String relationString = "{ (someRole: $x1, someRole: $x2, subRole: $x3); };";
            String relationString2 = "{ (someRole: $x1, subRole: $x2, subRole: $x3); };";
            String relationString3 = "{ (subRole: $x1, subRole: $x2, subRole: $x3); };";
            Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
            Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, tx), tx).getAtom();
            Atom relation3 = ReasonerQueries.atomic(conjunction(relationString3, tx), tx).getAtom();
            assertEquals(1, relation.getApplicableRules().count());
            assertEquals(1, relation2.getApplicableRules().count());
            assertThat(relation3.getApplicableRules().collect(toSet()), empty());
        }
    }

    @Test
    public void testRuleApplicability_genericRelationWithGenericType(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            String relationString = "{ ($x, $y);$x isa noRoleEntity; };";
            String relationString2 = "{ ($x, $y);$x isa entity; };";
            String relationString3 = "{ ($x, $y);$x isa relation; };";
            Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
            Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, tx), tx).getAtom();
            Atom relation3 = ReasonerQueries.create(conjunction(relationString3, tx), tx).getAtoms(RelationAtom.class).findFirst().orElse(null);

            assertEquals(7, relation.getApplicableRules().count());
            assertEquals(tx.ruleCache().getRules().filter(r -> r.thenTypes().allMatch(Concept::isRelationType)).count(), relation2.getApplicableRules().count());

            //TODO not filtered correctly
            //assertEquals(tx.ruleCache().getRules().filter(r -> r.thenTypes().allMatch(Concept::isAttributeType)).count(), relation3.getApplicableRules().count());
        }
    }

    @Test
    public void testRuleApplicability_ReifiedRelationsWithType(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            String relationString = "{ (someRole: $x, subRole: $y) isa reifying-relation; };";
            String relationString2 = "{ $x isa entity;(someRole: $x, subRole: $y) isa reifying-relation; };";
            String relationString3 = "{ $x isa anotherTwoRoleEntity;(someRole: $x, subRole: $y) isa reifying-relation; };";
            String relationString4 = "{ $x isa twoRoleEntity;(someRole: $x, subRole: $y) isa reifying-relation; };";

            Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
            Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, tx), tx).getAtom();
            Atom relation3 = ReasonerQueries.atomic(conjunction(relationString3, tx), tx).getAtom();
            Atom relation4 = ReasonerQueries.atomic(conjunction(relationString4, tx), tx).getAtom();
            assertEquals(2, relation.getApplicableRules().count());
            assertEquals(2, relation2.getApplicableRules().count());
            assertEquals(1, relation3.getApplicableRules().count());
            assertThat(relation4.getApplicableRules().collect(toSet()), empty());
        }
    }

    @Test
    public void testRuleApplicability_TypePlayabilityDeterminesApplicability(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            String relationString = "{ $y isa singleRoleEntity;(someRole:$x, role:$y, anotherRole: $z) isa ternary; };";
            String relationString2 = "{ $y isa twoRoleEntity;(someRole:$x, subRole:$y, anotherRole: $z) isa ternary; };";
            String relationString3 = "{ $y isa anotherTwoRoleEntity;(someRole:$x, subRole:$y, anotherRole: $z) isa ternary; };";
            String relationString4 = "{ $y isa noRoleEntity;(someRole:$x, subRole:$y, anotherRole: $z) isa ternary; };";
            String relationString5 = "{ $y isa entity;(someRole:$x, subRole:$y, anotherRole: $z) isa ternary; };";
            Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
            Atom relation2 = ReasonerQueries.atomic(conjunction(relationString2, tx), tx).getAtom();
            Atom relation3 = ReasonerQueries.atomic(conjunction(relationString3, tx), tx).getAtom();
            Atom relation4 = ReasonerQueries.atomic(conjunction(relationString4, tx), tx).getAtom();
            Atom relation5 = ReasonerQueries.atomic(conjunction(relationString5, tx), tx).getAtom();

            assertEquals(1, relation.getApplicableRules().count());
            assertThat(relation2.getApplicableRules().collect(toSet()), empty());
            assertEquals(1, relation3.getApplicableRules().count());
            assertEquals(1, relation4.getApplicableRules().count());
            assertEquals(1, relation5.getApplicableRules().count());
        }
    }

    @Test
    public void testRuleApplicability_TypeRelation(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            String typeString = "{ $x isa reifying-relation; };";
            String typeString2 = "{ $x isa ternary; };";
            String typeString3 = "{ $x isa binary; };";
            Atom type = ReasonerQueries.atomic(conjunction(typeString, tx), tx).getAtom();
            Atom type2 = ReasonerQueries.atomic(conjunction(typeString2, tx), tx).getAtom();
            Atom type3 = ReasonerQueries.atomic(conjunction(typeString3, tx), tx).getAtom();
            assertEquals(2, type.getApplicableRules().count());
            assertEquals(2, type2.getApplicableRules().count());
            assertEquals(1, type3.getApplicableRules().count());
        }
    }

    @Test
    public void testRuleApplicability_OntologicalTypes(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            String typeString = "{ $x sub " + Schema.MetaSchema.RELATION.getLabel() + "; };";
            String typeString2 = "{ $x relates someRole; };";
            String typeString3 = "{ $x plays someRole; };";
            String typeString4 = "{ $x has name; };";
            Atom type = ReasonerQueries.atomic(conjunction(typeString, tx), tx).getAtom();
            Atom type2 = ReasonerQueries.atomic(conjunction(typeString2, tx), tx).getAtom();
            Atom type3 = ReasonerQueries.atomic(conjunction(typeString3, tx), tx).getAtom();
            Atom type4 = ReasonerQueries.atomic(conjunction(typeString4, tx), tx).getAtom();
            assertThat(type.getApplicableRules().collect(toSet()), empty());
            assertThat(type2.getApplicableRules().collect(toSet()), empty());
            assertThat(type3.getApplicableRules().collect(toSet()), empty());
            assertThat(type4.getApplicableRules().collect(toSet()), empty());
        }
    }

    @Test
    public void testRuleApplicability_InstancesMakeRuleInapplicable_NoRoleTypes(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            Concept concept = getConcept(tx, "name", "noRoleEntity");
            String relationString = "{" +
                    "($x, $y) isa ternary;" +
                    "$x id " + concept.id().getValue() + ";" +
                    "};";
            Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
            assertThat(relation.getApplicableRules().collect(toSet()), empty());
        }
    }

    @Test
    public void testRuleApplicability_InstancesMakeRuleInapplicable_NoRoleTypes_NoRelationType(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().write()) {
            Concept concept = getConcept(tx, "name", "noRoleEntity");
            String relationString = "{" +
                    "($x, $y);" +
                    "$x id " + concept.id().getValue() + ";" +
                    "};";

            Atom relation = ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
            assertThat(relation.getApplicableRules().collect(toSet()), empty());
        }
    }

    @Test
    public void testRuleApplicability_ResourceDouble(){
        try(TransactionOLTP tx = resourceApplicabilitySession.transaction().write()) {
            String resourceString = "{ $x has res-double > 3.0; };";
            String resourceString2 = "{ $x has res-double > 4.0; };";
            String resourceString3 = "{ $x has res-double < 3.0; };";
            String resourceString4 = "{ $x has res-double < 4.0; };";
            String resourceString5 = "{ $x has res-double >= 5.0; };";
            String resourceString6 = "{ $x has res-double <= 5.0; };";
            String resourceString7 = "{ $x has res-double == 3.14; };";
            String resourceString8 = "{ $x has res-double !== 5.0; };";

            Atom resource = ReasonerQueries.atomic(conjunction(resourceString, tx), tx).getAtom();
            Atom resource2 = ReasonerQueries.atomic(conjunction(resourceString2, tx), tx).getAtom();
            Atom resource3 = ReasonerQueries.atomic(conjunction(resourceString3, tx), tx).getAtom();
            Atom resource4 = ReasonerQueries.atomic(conjunction(resourceString4, tx), tx).getAtom();
            Atom resource5 = ReasonerQueries.atomic(conjunction(resourceString5, tx), tx).getAtom();
            Atom resource6 = ReasonerQueries.atomic(conjunction(resourceString6, tx), tx).getAtom();
            Atom resource7 = ReasonerQueries.atomic(conjunction(resourceString7, tx), tx).getAtom();
            Atom resource8 = ReasonerQueries.atomic(conjunction(resourceString8, tx), tx).getAtom();

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
    public void testRuleApplicability_ResourceLong(){
        try(TransactionOLTP tx = resourceApplicabilitySession.transaction().write()) {
            String resourceString = "{ $x has res-long > 100; };";
            String resourceString2 = "{ $x has res-long > 150; };";
            String resourceString3 = "{ $x has res-long < 100; };";
            String resourceString4 = "{ $x has res-long < 200; };";
            String resourceString5 = "{ $x has res-long >= 130; };";
            String resourceString6 = "{ $x has res-long <= 130; };";
            String resourceString7 = "{ $x has res-long == 123; };";
            String resourceString8 = "{ $x has res-long !== 200; };";

            Atom resource = ReasonerQueries.atomic(conjunction(resourceString, tx), tx).getAtom();
            Atom resource2 = ReasonerQueries.atomic(conjunction(resourceString2, tx), tx).getAtom();
            Atom resource3 = ReasonerQueries.atomic(conjunction(resourceString3, tx), tx).getAtom();
            Atom resource4 = ReasonerQueries.atomic(conjunction(resourceString4, tx), tx).getAtom();
            Atom resource5 = ReasonerQueries.atomic(conjunction(resourceString5, tx), tx).getAtom();
            Atom resource6 = ReasonerQueries.atomic(conjunction(resourceString6, tx), tx).getAtom();
            Atom resource7 = ReasonerQueries.atomic(conjunction(resourceString7, tx), tx).getAtom();
            Atom resource8 = ReasonerQueries.atomic(conjunction(resourceString8, tx), tx).getAtom();

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
    public void testRuleApplicability_ResourceString(){
        try(TransactionOLTP tx = resourceApplicabilitySession.transaction().write()) {
            String resourceString = "{ $x has res-string contains 'ing'; };";
            String resourceString2 = "{ $x has res-string 'test' ; };";
            String resourceString3 = "{ $x has res-string like \".*(fast|string).*\"; };";
            String resourceString4 = "{ $x has res-string like \".*\"; };";

            Atom resource = ReasonerQueries.atomic(conjunction(resourceString, tx), tx).getAtom();
            Atom resource2 = ReasonerQueries.atomic(conjunction(resourceString2, tx), tx).getAtom();
            Atom resource3 = ReasonerQueries.atomic(conjunction(resourceString3, tx), tx).getAtom();
            Atom resource4 = ReasonerQueries.atomic(conjunction(resourceString4, tx), tx).getAtom();

            assertEquals(1, resource.getApplicableRules().count());
            assertThat(resource2.getApplicableRules().collect(toSet()), empty());
            assertEquals(1, resource3.getApplicableRules().count());
            assertEquals(1, resource4.getApplicableRules().count());
        }
    }

    @Test
    public void testRuleApplicability_ResourceBoolean(){
        try(TransactionOLTP tx = resourceApplicabilitySession.transaction().write()) {
            String resourceString = "{ $x has res-boolean 'true'; };";
            String resourceString2 = "{ $x has res-boolean 'false'; };";

            Atom resource = ReasonerQueries.atomic(conjunction(resourceString, tx), tx).getAtom();
            Atom resource2 = ReasonerQueries.atomic(conjunction(resourceString2, tx), tx).getAtom();

            assertEquals(1, resource.getApplicableRules().count());
            assertThat(resource2.getApplicableRules().collect(toSet()), empty());
        }
    }

    @Test
    public void testRuleApplicability_TypeResource(){
        try(TransactionOLTP tx = resourceApplicabilitySession.transaction().write()) {
            String typeString = "{ $x isa resource; };";
            Atom type = ReasonerQueries.atomic(conjunction(typeString, tx), tx).getAtom();
            assertEquals(1, type.getApplicableRules().count());
        }
    }

    @Test
    public void testRuleApplicability_Resource_TypeMismatch(){
        try(TransactionOLTP tx = resourceApplicabilitySession.transaction().write()) {
            String resourceString = "{ $x isa someEntity, has resource $r; };";
            String resourceString2 = "{ $x isa anotherEntity, has resource $r; };";
            String resourceString3 = "{ $x isa anotherEntity, has resource 'test'; };";

            Atom resource = ReasonerQueries.atomic(conjunction(resourceString, tx), tx).getAtom();
            Atom resource2 = ReasonerQueries.atomic(conjunction(resourceString2, tx), tx).getAtom();
            Atom resource3 = ReasonerQueries.atomic(conjunction(resourceString3, tx), tx).getAtom();

            assertEquals(1, resource.getApplicableRules().count());
            assertThat(resource2.getApplicableRules().collect(toSet()), empty());
            assertThat(resource3.getApplicableRules().collect(toSet()), empty());
        }
    }

    @Test
    public void testRuleApplicability_ReifiedResourceDouble(){
        try(TransactionOLTP tx = reifiedResourceApplicabilitySession.transaction().write()) {
            String queryString = "{ $x > 3.0 isa res-double; ($x, $y); };";
            String queryString2 = "{ $x > 4.0 isa res-double; ($x, $y); };";
            String queryString3 = "{ $x < 3.0 isa res-double; ($x, $y); };";
            String queryString4 = "{ $x < 4.0 isa res-double; ($x, $y); };";
            String queryString5 = "{ $x >= 5.0 isa res-double; ($x, $y); };";
            String queryString6 = "{ $x <= 5.0 isa res-double; ($x, $y); };";
            String queryString7 = "{ $x == 3.14 isa res-double; ($x, $y); };";
            String queryString8 = "{ $x !== 5.0 isa res-double; ($x, $y); };";

            Atom atom = ReasonerQueries.atomic(conjunction(queryString, tx), tx).getAtom();
            Atom atom2 = ReasonerQueries.atomic(conjunction(queryString2, tx), tx).getAtom();
            Atom atom3 = ReasonerQueries.atomic(conjunction(queryString3, tx), tx).getAtom();
            Atom atom4 = ReasonerQueries.atomic(conjunction(queryString4, tx), tx).getAtom();
            Atom atom5 = ReasonerQueries.atomic(conjunction(queryString5, tx), tx).getAtom();
            Atom atom6 = ReasonerQueries.atomic(conjunction(queryString6, tx), tx).getAtom();
            Atom atom7 = ReasonerQueries.atomic(conjunction(queryString7, tx), tx).getAtom();
            Atom atom8 = ReasonerQueries.atomic(conjunction(queryString8, tx), tx).getAtom();

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
    public void testRuleApplicability_ReifiedResourceLong(){
        try(TransactionOLTP tx = reifiedResourceApplicabilitySession.transaction().write()) {
            String queryString = "{ $x > 100 isa res-long; ($x, $y); };";
            String queryString2 = "{ $x > 150 isa res-long; ($x, $y); };";
            String queryString3 = "{ $x < 100 isa res-long; ($x, $y); };";
            String queryString4 = "{ $x < 200 isa res-long; ($x, $y); };";
            String queryString5 = "{ $x >= 130 isa res-long; ($x, $y); };";
            String queryString6 = "{ $x <= 130 isa res-long; ($x, $y); };";
            String queryString7 = "{ $x == 123 isa res-long; ($x, $y); };";
            String queryString8 = "{ $x !== 200 isa res-long; ($x, $y); };";

            Atom atom = ReasonerQueries.atomic(conjunction(queryString, tx), tx).getAtom();
            Atom atom2 = ReasonerQueries.atomic(conjunction(queryString2, tx), tx).getAtom();
            Atom atom3 = ReasonerQueries.atomic(conjunction(queryString3, tx), tx).getAtom();
            Atom atom4 = ReasonerQueries.atomic(conjunction(queryString4, tx), tx).getAtom();
            Atom atom5 = ReasonerQueries.atomic(conjunction(queryString5, tx), tx).getAtom();
            Atom atom6 = ReasonerQueries.atomic(conjunction(queryString6, tx), tx).getAtom();
            Atom atom7 = ReasonerQueries.atomic(conjunction(queryString7, tx), tx).getAtom();
            Atom atom8 = ReasonerQueries.atomic(conjunction(queryString8, tx), tx).getAtom();

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
    public void testRuleApplicability_ReifiedResourceString(){
        try(TransactionOLTP tx = reifiedResourceApplicabilitySession.transaction().write()) {
            String queryString = "{ $x contains 'val' isa res-string; ($x, $y); };";
            String queryString2 = "{ $x 'test' isa res-string; ($x, $y); };";
            String queryString3 = "{ $x like \".*(fast|value).*\" isa res-string; ($x, $y); };";
            String queryString4 = "{ $x like \".*\" isa res-string; ($x, $y); };";

            Atom atom = ReasonerQueries.atomic(conjunction(queryString, tx), tx).getAtom();
            Atom atom2 = ReasonerQueries.atomic(conjunction(queryString2, tx), tx).getAtom();
            Atom atom3 = ReasonerQueries.atomic(conjunction(queryString3, tx), tx).getAtom();
            Atom atom4 = ReasonerQueries.atomic(conjunction(queryString4, tx), tx).getAtom();

            assertEquals(1, atom.getApplicableRules().count());
            assertThat(atom2.getApplicableRules().collect(toSet()), empty());
            assertEquals(1, atom3.getApplicableRules().count());
            assertEquals(1, atom4.getApplicableRules().count());
        }
    }

    @Test
    public void testRuleApplicability_ReifiedResourceBoolean() {
        try(TransactionOLTP tx = reifiedResourceApplicabilitySession.transaction().write()) {
            String queryString = "{ $x 'true' isa res-boolean;($x, $y); };";
            String queryString2 = "{ $x 'false' isa res-boolean;($x, $y); };";

            Atom atom = ReasonerQueries.atomic(conjunction(queryString, tx), tx).getAtom();
            Atom atom2 = ReasonerQueries.atomic(conjunction(queryString2, tx), tx).getAtom();

            assertEquals(1, atom.getApplicableRules().count());
            assertThat(atom2.getApplicableRules().collect(toSet()), empty());
        }
    }

    @Test
    public void testRuleApplicability_whenMatchingRulesForGroundAtomRedefinedViaRule_ruleIsMatched(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().read()) {
            Relation instance = tx.getRelationType("reifiable-relation").instances().findFirst().orElse(null);
            String queryString = "{ $r has description 'typed-reified'; $r id " + instance.id().getValue() + "; };";
            Atom atom = ReasonerQueries.atomic(conjunction(queryString, tx), tx).getAtom();

            assertTrue(atom.getApplicableRules().findFirst().isPresent());
        }
    }

    @Test
    public void testRuleApplicability_whenMatchingRulesForGroundTypeWhichIsNotRedefined_noRulesAreMatched(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().read()) {
            Relation instance = tx.getRelationType("binary").instances().findFirst().orElse(null);
            String queryString = "{ $x isa binary; $x id " + instance.id().getValue() + "; };";
            Atom atom = ReasonerQueries.atomic(conjunction(queryString, tx), tx).getAtom();

            assertThat(atom.getApplicableRules().collect(toSet()), empty());
        }
    }

    @Test
    public void testRuleApplicability_whenMatchingRulesForASpecificRelation_noRulesAreMatched(){
        try(TransactionOLTP tx = ruleApplicabilitySession.transaction().read()) {
            Relation instance = tx.getRelationType("binary").instances().findFirst().orElse(null);
            String queryString = "{ $r ($x, $y) isa binary; $r id " + instance.id().getValue() + "; };";
            Atom atom = ReasonerQueries.atomic(conjunction(queryString, tx), tx).getAtom();

            assertThat(atom.getApplicableRules().collect(toSet()), empty());
        }
    }

    private Conjunction<Statement> conjunction(String patternString, TransactionOLTP tx){
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

    private Concept getConcept(TransactionOLTP tx, String typeName, String val){
        return tx.stream(Graql.match((Pattern) var("x").has(typeName, val)).get("x"))
                .map(ans -> ans.get("x")).findAny().orElse(null);
    }
}