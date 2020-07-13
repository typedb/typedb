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
import com.google.common.collect.Multimap;
import grakn.core.common.config.Config;
import grakn.core.core.Schema;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestStorage;
import grakn.core.test.rule.SessionUtil;
import grakn.core.test.rule.TestTransactionProvider;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

import static grakn.core.test.common.GraqlTestUtil.loadFromFileAndCommit;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class RoleInferenceIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session session;

    private static Session roleInferenceSetSession;
    private static Session genericSchemaSession;
    private static Session ruleApplicabilitySetSession;

    @BeforeClass
    public static void loadContext(){
        Config compatibleServerConfig = storage.createCompatibleServerConfig();
        final String resourcePath = "test/integration/graql/reasoner/resources/";
        roleInferenceSetSession = SessionUtil.serverlessSessionWithNewKeyspace(compatibleServerConfig);
        loadFromFileAndCommit(resourcePath, "roleInferenceTest.gql", roleInferenceSetSession);
        genericSchemaSession = SessionUtil.serverlessSessionWithNewKeyspace(compatibleServerConfig);
        loadFromFileAndCommit(resourcePath, "genericSchema.gql", genericSchemaSession);
        ruleApplicabilitySetSession = SessionUtil.serverlessSessionWithNewKeyspace(compatibleServerConfig);
        loadFromFileAndCommit(resourcePath,"ruleApplicabilityTest.gql", ruleApplicabilitySetSession);


        session = SessionUtil.serverlessSessionWithNewKeyspace(compatibleServerConfig);


        // define role inference schema

        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            // binary inference schema
            tx.execute(Graql.parse("define " +
                    "ownership sub relation, " +
                    "  relates owner," +
                    "  relates owned;" +
                    "borrowing sub ownership," +
                    "  relates owner, " + // TODO remove once inherited
                    "  relates owned, " + // TODO remove once inherited
                    "  relates borrower as owner," +
                    "  relates borrowed as owned;" +
                    "friendship sub relation," +
                    "  relates friend;" +
                    "best-friendship sub relation," +
                    "  relates friend, " + // "inherited" and overriden in the same hierarchy
                    "  relates best-friend as friend;" +
                    "person sub entity," +
                    "  plays owner," +
                    "  plays friend, " +
                    "  plays best-friend;" +
                    "reader sub person, " +
                    "  plays borrower;" +
                    "item sub entity," +
                    "  plays owned;" +
                    "book sub item," +
                    "  plays borrowed;").asDefine());
            tx.commit();
        }


    }

    @AfterClass
    public static void closeSession(){
        roleInferenceSetSession.close();
        genericSchemaSession.close();
        ruleApplicabilitySetSession.close();
        session.close();
    }

    /*

      Using Binary Relations

     */

    @Test
    public void testRoleInference_TypedBinaryRelation(){
        Transaction tx = session.transaction(Transaction.Type.WRITE);
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{ ($x, $y); $x isa person; $y isa item; };";
        String patternString2 = "{ ($x, $y) isa ownership; $x isa person; $y isa item; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("owner"), new Variable("x"),
                tx.getRole("owned"), new Variable("y"));
        roleInference(patternString, correctRoleMap, reasonerQueryFactory);
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
        tx.close();
    }

    @Test
    public void testRoleInference_TypedBinaryRelation_SingleTypeMissing(){
        Transaction tx = session.transaction(Transaction.Type.WRITE);
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{ ($x, $y); $x isa person; };";
        String patternString2 = "{ ($x, $y) isa ownership; $x isa person; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("owner"), new Variable("x"),
                tx.getRole("role"), new Variable("y"));
        roleInference(patternString, correctRoleMap, reasonerQueryFactory);
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
        tx.close();
    }


    @Test //for each role player role mapping is ambiguous so metarole has to be assigned
    public void testRoleInference_NoInformationPresent(){
        Transaction tx = session.transaction(Transaction.Type.WRITE);
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();
        String relationString = "{ ($x, $y); };";
        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role"), new Variable("x"),
                tx.getRole("role"), new Variable("y"));
        roleInference(relationString, correctRoleMap, reasonerQueryFactory);
        tx.close();
    }

    @Test //for each role player role mapping is ambiguous so metarole has to be assigned
    public void testRoleInference_MetaRelationType(){
        Transaction tx = session.transaction(Transaction.Type.WRITE);
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();
        String relationString = "{ ($x, $y) isa relation; };";
        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role"), new Variable("x"),
                tx.getRole("role"), new Variable("y"));
        roleInference(relationString, correctRoleMap, reasonerQueryFactory);
        tx.close();
    }

    @Test //missing role is ambiguous without cardinality constraints
    public void testRoleInference_RoleHierarchyInvolved() {
        Transaction tx = session.transaction(Transaction.Type.WRITE);
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String ownerKnown = "{ (owner: $x, $generic) isa ownership; };";
        String ownedKnown = "{ ($generic, owned: $y) isa ownership; };";

        ImmutableSetMultimap<Role, Variable> correctOwnerKnownMap = ImmutableSetMultimap.of(
                tx.getRole("role"), new Variable("generic"),
                tx.getRole("owner"), new Variable("x"));
        ImmutableSetMultimap<Role, Variable> correctOwnedKnownMap = ImmutableSetMultimap.of(
                tx.getRole("role"), new Variable("generic"),
                tx.getRole("owned"), new Variable("y"));
        roleInference(ownerKnown, correctOwnerKnownMap, reasonerQueryFactory);
        roleInference(ownedKnown, correctOwnedKnownMap, reasonerQueryFactory);
        tx.close();
    }


    @Test //relation relates a single role so instead of assigning metarole this role should be assigned
    public void testRoleInference_RelationHasVerticalRoleHierarchy(){
        Transaction tx = session.transaction(Transaction.Type.WRITE);
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        // should be able to infer the most-specialised role possible, when all
        // roles played are of the same ancestor (friend -> best-friend)
        String relationString = "{ ($x) isa best-friendship; };";
        ImmutableSetMultimap<Role, Variable> roleMap = ImmutableSetMultimap.of(
                tx.getRole("friend"), new Variable("x"));
        roleInference(relationString, roleMap, reasonerQueryFactory);
        tx.close();
    }


    /*

      Using ternary relations

     */

    @Test //each type maps to a specific role
    public void testRoleInference_TypedTernaryRelationWithKnownRole(){
        Transaction tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{  ($x, $y, role3: $z);$x isa entity1;$y isa entity2;  };";
        String patternString2 = "{ ($x, $y, role3: $z) isa ternary;$x isa entity1;$y isa entity2; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role1"), new Variable("x"),
                tx.getRole("role2"), new Variable("y"),
                tx.getRole("role3"), new Variable("z"));
        roleInference(patternString, correctRoleMap, reasonerQueryFactory);
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
        tx.close();
    }

    @Test //without cardinality constraints the $y variable can be mapped to any of the three roles hence metarole is assigned
    public void testRoleInference_TypedTernaryRelation(){
        Transaction tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{ ($x, $y, $z);$x isa entity1;$y isa entity2; };";
        String patternString2 = "{ ($x, $y, $z) isa ternary;$x isa entity1;$y isa entity2; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role1"), new Variable("x"),
                tx.getRole("role2"), new Variable("y"),
                tx.getRole("role"), new Variable("z"));
        roleInference(patternString, correctRoleMap, reasonerQueryFactory);
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
        tx.close();
    }

    @Test
    public void testRoleInference_TernaryRelationWithRepeatingRolePlayers(){
        Transaction tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{ (role1: $x, role2: $y, $y); };";
        String patternString2 = "{ (role1: $x, role2: $y, $y) isa ternary; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role1"), new Variable("x"),
                tx.getRole("role2"), new Variable("y"),
                tx.getRole("role"), new Variable("y"));
        roleInference(patternString, correctRoleMap, reasonerQueryFactory);
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
        tx.close();
    }

    @Test
    public void testRoleInference_TypedTernaryRelation_TypesPlaySubRoles_SubRolesAreCorrectlyIdentified(){
        Transaction tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{ (role: $x, role: $y, role: $z); $x isa anotherEntity1; $y isa anotherEntity2; $z isa anotherEntity3; };";
        String patternString2 = "{ (role: $x, role: $y, role: $z) isa ternary; $x isa anotherEntity1; $y isa anotherEntity2; $z isa anotherEntity3; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("subRole1"), new Variable("x"),
                tx.getRole("subRole2"), new Variable("y"),
                tx.getRole("subRole3"), new Variable("z"));
        roleInference(patternString, correctRoleMap, reasonerQueryFactory);
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
        tx.close();
    }

    @Test
    public void testRoleInference_TypedTernaryRelationWithMetaRoles_MetaRolesShouldBeOverwritten(){
        Transaction tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{ (role: $x, role: $y, role: $z); $x isa entity1; $y isa entity2; $z isa entity3; };";
        String patternString2 = "{ (role: $x, role: $y, role: $z) isa ternary; $x isa entity1; $y isa entity2; $z isa entity3; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role1"), new Variable("x"),
                tx.getRole("role2"), new Variable("y"),
                tx.getRole("role3"), new Variable("z"));
        roleInference(patternString, correctRoleMap, reasonerQueryFactory);
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
        tx.close();
    }

    @Test
    public void testRoleInference_TypedTernaryRelation_TypesAreSubTypes_TopRolesShouldBeChosen(){
        Transaction tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{ (role: $x, role: $y, role: $z); $x isa subEntity1; $y isa subEntity2; $z isa subEntity3; };";
        String patternString2 = "{ (role: $x, role: $y, role: $z) isa ternary; $x isa subEntity1; $y isa subEntity2; $z isa subEntity3; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role1"), new Variable("x"),
                tx.getRole("role2"), new Variable("y"),
                tx.getRole("role3"), new Variable("z"));
        roleInference(patternString, correctRoleMap, reasonerQueryFactory);
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
        tx.close();
    }

    @Test
    public void testRoleInference_TypedTernaryRelation_TypesCanPlayMultipleRoles_MetaRoleIsChosen(){
        Transaction tx = roleInferenceSetSession.transaction(Transaction.Type.WRITE);
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{ ($x, $y, $z); $x isa genericEntity; $y isa genericEntity; $z isa genericEntity; };";
        String patternString2 = "{ ($x, $y, $z) isa ternary; $x isa genericEntity; $y isa genericEntity; $z isa genericEntity; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role"), new Variable("x"),
                tx.getRole("role"), new Variable("y"),
                tx.getRole("role"), new Variable("z"));
        roleInference(patternString, correctRoleMap, reasonerQueryFactory);
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
        tx.close();
    }
    @Test //entity1 plays role1 but entity2 plays roles role1, role2 hence ambiguous and metarole has to be assigned, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRoleInference_WithMetaType(){
        Transaction tx = ruleApplicabilitySetSession.transaction(Transaction.Type.WRITE);
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String relationString = "{ ($x, $y, $z) isa ternary; $x isa singleRoleEntity; $y isa twoRoleEntity; $z isa entity; };";
        RelationAtom relation = (RelationAtom) reasonerQueryFactory.atomic(conjunction(relationString)).getAtom();
        ImmutableSetMultimap<Role, Variable> roleMap = ImmutableSetMultimap.of(
                tx.getRole("someRole"), new Variable("x"),
                tx.getRole("role"), new Variable("y"),
                tx.getRole("role"), new Variable("z"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
        tx.close();
    }

    @Test //entity1 plays role1, entity2 plays 2 roles, entity3 plays 3 roles hence ambiguous and metarole has to be assigned, EXPECTED TO CHANGE WITH CARDINALITY CONSTRAINTS
    public void testRoleInference_RoleMappingUnambiguous(){
        Transaction tx = ruleApplicabilitySetSession.transaction(Transaction.Type.WRITE);
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String relationString = "{ ($x, $y, $z) isa ternary;$x isa singleRoleEntity; $y isa twoRoleEntity; $z isa threeRoleEntity; };";
        RelationAtom relation = (RelationAtom) reasonerQueryFactory.atomic(conjunction(relationString)).getAtom();
        ImmutableSetMultimap<Role, Variable> roleMap = ImmutableSetMultimap.of(
                tx.getRole("someRole"), new Variable("x"),
                tx.getRole("role"), new Variable("y"),
                tx.getRole("role"), new Variable("z"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
        tx.close();
    }

    @Test //for each role player role mapping is ambiguous so metarole has to be assigned
    public void testRoleInference_AllRolePlayersHaveAmbiguousRoles(){
        Transaction tx = ruleApplicabilitySetSession.transaction(Transaction.Type.WRITE);
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String relationString = "{ ($x, $y, $z) isa ternary;$x isa twoRoleEntity; $y isa threeRoleEntity; $z isa anotherTwoRoleEntity; };";
        RelationAtom relation = (RelationAtom) reasonerQueryFactory.atomic(conjunction(relationString)).getAtom();
        relation.getRoleVarMap().entries().forEach(e -> assertTrue(Schema.MetaSchema.isMetaLabel(e.getKey().label())));
        tx.close();
    }

    private void roleInference(String patternString, ImmutableSetMultimap<Role, Variable> expectedRoleMap, ReasonerQueryFactory reasonerQueryFactory) {
        RelationAtom atom = (RelationAtom) reasonerQueryFactory.atomic(conjunction(patternString)).getAtom();
        Multimap<Role, Variable> roleMap = roleSetMap(atom.getRoleVarMap());
        assertEquals(expectedRoleMap, roleMap);
    }

    private Multimap<Role, Variable> roleSetMap(Multimap<Role, Variable> roleVarMap) {
        Multimap<Role, Variable> roleMap = HashMultimap.create();
        roleVarMap.entries().forEach(e -> roleMap.put(e.getKey(), e.getValue()));
        return roleMap;
    }

    private Conjunction<Statement> conjunction(String patternString){
        Set<Statement> vars = Graql.parsePattern(patternString)
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }
}
