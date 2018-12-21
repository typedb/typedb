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
import grakn.core.graql.concept.Role;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.internal.reasoner.atom.binary.RelationshipAtom;
import grakn.core.graql.internal.reasoner.query.ReasonerQueries;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.graql.query.pattern.Pattern.var;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class AtomicRoleInferenceIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl roleInferenceSetSession;
    private static SessionImpl genericSchemaSession;
    private static SessionImpl ruleApplicabilitySetSession;

    private static void loadFromFile(String fileName, Session session){
        try {
            InputStream inputStream = AtomicRoleInferenceIT.class.getClassLoader().getResourceAsStream("test-integration/graql/reasoner/resources/"+fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            Transaction tx = session.transaction(Transaction.Type.WRITE);
            Graql.parseList(s).forEach(tx::execute);
            tx.commit();
        } catch (Exception e){
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void loadContext(){
        roleInferenceSetSession = server.sessionWithNewKeyspace();
        loadFromFile("roleInferenceTest.gql", roleInferenceSetSession);
        genericSchemaSession = server.sessionWithNewKeyspace();
        loadFromFile("genericSchema.gql", genericSchemaSession);
        ruleApplicabilitySetSession = server.sessionWithNewKeyspace();
        loadFromFile("ruleApplicabilityTest.gql", ruleApplicabilitySetSession);
    }

    @AfterClass
    public static void closeSession(){
        roleInferenceSetSession.close();
        genericSchemaSession.close();
        ruleApplicabilitySetSession.close();
    }

    @Test
    public void testRoleInference_TypedBinaryRelation(){
        TransactionOLTP tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        String patternString = "{($x, $y); $x isa entity1; $y isa entity2;}";
        String patternString2 = "{($x, $y) isa binary; $x isa entity1; $y isa entity2;}";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role1"), var("x"),
                tx.getRole("role2"), var("y"));
        roleInference(patternString, correctRoleMap, tx);
        roleInference(patternString2, correctRoleMap, tx);
        tx.close();
    }

    @Test
    public void testRoleInference_TypedBinaryRelation_SingleTypeMissing(){
        TransactionOLTP tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        String patternString = "{($x, $y); $x isa entity1;}";
        String patternString2 = "{($x, $y) isa binary; $x isa entity1;}";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role1"), var("x"),
                tx.getRole("role"), var("y"));
        roleInference(patternString, correctRoleMap, tx);
        roleInference(patternString2, correctRoleMap, tx);
        tx.close();
    }

    @Test //each type maps to a specific role
    public void testRoleInference_TypedTernaryRelationWithKnownRole(){
        TransactionOLTP tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        String patternString = "{($x, $y, role3: $z);$x isa entity1;$y isa entity2;}";
        String patternString2 = "{($x, $y, role3: $z) isa ternary;$x isa entity1;$y isa entity2;}";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role1"), var("x"),
                tx.getRole("role2"), var("y"),
                tx.getRole("role3"), var("z"));
        roleInference(patternString, correctRoleMap, tx);
        roleInference(patternString2, correctRoleMap, tx);
        tx.close();
    }

    @Test //without cardinality constraints the $y variable can be mapped to any of the three roles hence metarole is assigned
    public void testRoleInference_TypedTernaryRelation(){
        TransactionOLTP tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        String patternString = "{($x, $y, $z);$x isa entity1;$y isa entity2;}";
        String patternString2 = "{($x, $y, $z) isa ternary;$x isa entity1;$y isa entity2;}";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role1"), var("x"),
                tx.getRole("role2"), var("y"),
                tx.getRole("role"), var("z"));
        roleInference(patternString, correctRoleMap, tx);
        roleInference(patternString2, correctRoleMap, tx);
        tx.close();
    }

    @Test
    public void testRoleInference_TernaryRelationWithRepeatingRolePlayers(){
        TransactionOLTP tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        String patternString = "{(role1: $x, role2: $y, $y);}";
        String patternString2 = "{(role1: $x, role2: $y, $y) isa ternary;}";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role1"), var("x"),
                tx.getRole("role2"), var("y"),
                tx.getRole("role"), var("y"));
        roleInference(patternString, correctRoleMap, tx);
        roleInference(patternString2, correctRoleMap, tx);
        tx.close();
    }

    @Test
    public void testRoleInference_TypedTernaryRelation_TypesPlaySubRoles_SubRolesAreCorrectlyIdentified(){
        TransactionOLTP tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        String patternString = "{(role: $x, role: $y, role: $z); $x isa anotherEntity1; $y isa anotherEntity2; $z isa anotherEntity3;}";
        String patternString2 = "{(role: $x, role: $y, role: $z) isa ternary; $x isa anotherEntity1; $y isa anotherEntity2; $z isa anotherEntity3;}";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("subRole1"), var("x"),
                tx.getRole("subRole2"), var("y"),
                tx.getRole("subRole3"), var("z"));
        roleInference(patternString, correctRoleMap, tx);
        roleInference(patternString2, correctRoleMap, tx);
        tx.close();
    }

    @Test
    public void testRoleInference_TypedTernaryRelationWithMetaRoles_MetaRolesShouldBeOverwritten(){
        TransactionOLTP tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        String patternString = "{(role: $x, role: $y, role: $z); $x isa entity1; $y isa entity2; $z isa entity3;}";
        String patternString2 = "{(role: $x, role: $y, role: $z) isa ternary; $x isa entity1; $y isa entity2; $z isa entity3;}";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role1"), var("x"),
                tx.getRole("role2"), var("y"),
                tx.getRole("role3"), var("z"));
        roleInference(patternString, correctRoleMap, tx);
        roleInference(patternString2, correctRoleMap, tx);
        tx.close();
    }

    @Test
    public void testRoleInference_TypedTernaryRelation_TypesAreSubTypes_TopRolesShouldBeChosen(){
        TransactionOLTP tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        String patternString = "{(role: $x, role: $y, role: $z); $x isa subEntity1; $y isa subEntity2; $z isa subEntity3;}";
        String patternString2 = "{(role: $x, role: $y, role: $z) isa ternary; $x isa subEntity1; $y isa subEntity2; $z isa subEntity3;}";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role1"), var("x"),
                tx.getRole("role2"), var("y"),
                tx.getRole("role3"), var("z"));
        roleInference(patternString, correctRoleMap, tx);
        roleInference(patternString2, correctRoleMap, tx);
        tx.close();
    }

    @Test
    public void testRoleInference_TypedTernaryRelation_TypesCanPlayMultipleRoles_MetaRoleIsChosen(){
        TransactionOLTP tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        String patternString = "{($x, $y, $z); $x isa genericEntity; $y isa genericEntity; $z isa genericEntity;}";
        String patternString2 = "{($x, $y, $z) isa ternary; $x isa genericEntity; $y isa genericEntity; $z isa genericEntity;}";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role"), var("x"),
                tx.getRole("role"), var("y"),
                tx.getRole("role"), var("z"));
        roleInference(patternString, correctRoleMap, tx);
        roleInference(patternString2, correctRoleMap, tx);
        tx.close();
    }

    @Test //for each role player role mapping is ambiguous so metarole has to be assigned
    public void testRoleInference_NoInformationPresent(){
        TransactionOLTP tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        String relationString = "{($x, $y);}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().label())));
        tx.close();
    }

    @Test //for each role player role mapping is ambiguous so metarole has to be assigned
    public void testRoleInference_MetaRelationType(){
        TransactionOLTP tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        String relationString = "{($x, $y) isa relationship;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().label())));
        tx.close();
    }

    @Test //missing role is ambiguous without cardinality constraints
    public void testRoleInference_RoleHierarchyInvolved() {
        TransactionOLTP tx = genericSchemaSession.transaction(Transaction.Type.WRITE);
        String relationString = "{($p, subRole2: $gc) isa binary;}";
        String relationString2 = "{(subRole1: $gp, $p) isa binary;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        RelationshipAtom relation2 = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString2, tx), tx).getAtom();
        Multimap<Role, Variable> roleMap = roleSetMap(relation.getRoleVarMap());
        Multimap<Role, Variable> roleMap2 = roleSetMap(relation2.getRoleVarMap());

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role"), var("p"),
                tx.getRole("subRole2"), var("gc"));
        ImmutableSetMultimap<Role, Variable> correctRoleMap2 = ImmutableSetMultimap.of(
                tx.getRole("role"), var("p"),
                tx.getRole("subRole1"), var("gp"));
        assertEquals(correctRoleMap, roleMap);
        assertEquals(correctRoleMap2, roleMap2);
        tx.close();
    }

    @Test //entity1 plays role1 but entity2 plays roles role1, role2 hence ambiguous and metarole has to be assigned, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRoleInference_WithMetaType(){
        TransactionOLTP tx = ruleApplicabilitySetSession.transaction(Transaction.Type.WRITE);
        String relationString = "{($x, $y, $z) isa ternary;$x isa singleRoleEntity; $y isa twoRoleEntity; $z isa entity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        ImmutableSetMultimap<Role, Variable> roleMap = ImmutableSetMultimap.of(
                tx.getRole("someRole"), var("x"),
                tx.getRole("role"), var("y"),
                tx.getRole("role"), var("z"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
        tx.close();
    }

    @Test //entity1 plays role1, entity2 plays 2 roles, entity3 plays 3 roles hence ambiguous and metarole has to be assigned, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRoleInference_RoleMappingUnambiguous(){
        TransactionOLTP tx = ruleApplicabilitySetSession.transaction(Transaction.Type.WRITE);
        String relationString = "{($x, $y, $z) isa ternary;$x isa singleRoleEntity; $y isa twoRoleEntity; $z isa threeRoleEntity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        ImmutableSetMultimap<Role, Variable> roleMap = ImmutableSetMultimap.of(
                tx.getRole("someRole"), var("x"),
                tx.getRole("role"), var("y"),
                tx.getRole("role"), var("z"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
        tx.close();
    }

    @Test //for each role player role mapping is ambiguous so metarole has to be assigned
    public void testRoleInference_AllRolePlayersHaveAmbiguousRoles(){
        TransactionOLTP tx = ruleApplicabilitySetSession.transaction(Transaction.Type.WRITE);
        String relationString = "{($x, $y, $z) isa ternary;$x isa twoRoleEntity; $y isa threeRoleEntity; $z isa anotherTwoRoleEntity;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().label())));
        tx.close();
    }

    @Test //relation relates a single role so instead of assigning metarole this role should be assigned
    public void testRoleInference_RelationHasVerticalRoleHierarchy(){
        TransactionOLTP tx = ruleApplicabilitySetSession.transaction(Transaction.Type.WRITE);
        String relationString = "{($x, $y) isa reifying-relation;}";
        RelationshipAtom relation = (RelationshipAtom) ReasonerQueries.atomic(conjunction(relationString, tx), tx).getAtom();
        ImmutableSetMultimap<Role, Variable> roleMap = ImmutableSetMultimap.of(
                tx.getRole("someRole"), var("x"),
                tx.getRole("someRole"), var("y"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
        tx.close();
    }

    private void roleInference(String patternString, ImmutableSetMultimap<Role, Variable> expectedRoleMAp, TransactionOLTP tx){
        RelationshipAtom atom = (RelationshipAtom) ReasonerQueries.atomic(conjunction(patternString, tx), tx).getAtom();
        Multimap<Role, Variable> roleMap = roleSetMap(atom.getRoleVarMap());
        assertEquals(expectedRoleMAp, roleMap);
    }

    private Multimap<Role, Variable> roleSetMap(Multimap<Role, Variable> roleVarMap) {
        Multimap<Role, Variable> roleMap = HashMultimap.create();
        roleVarMap.entries().forEach(e -> roleMap.put(e.getKey(), e.getValue()));
        return roleMap;
    }

    private Conjunction<Statement> conjunction(String patternString, TransactionOLTP tx){
        Set<Statement> vars = Pattern.parse(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Pattern.and(vars);
    }
}
