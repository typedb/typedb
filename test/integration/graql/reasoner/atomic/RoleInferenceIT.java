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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

/**
 * Test the implementation of role inference. Given some roles, role player types, or relation types
 * we can sometimes deduce more information that the original query had in it.
 *
 * Note that with restrictions such as cardinality constraints, the
 */
@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class RoleInferenceIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session session;
    private static Transaction tx;

    @BeforeClass
    public static void loadContext(){
        Config compatibleServerConfig = storage.createCompatibleServerConfig();
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

            // ternary schema
            // three roles in 'recipe'
            // three roles in 'indian-recipe sub recipe' that subtype parent roles
            tx.execute(Graql.parse("define " +
                    "recipe sub relation, " +
                    "  relates recipe-ingredient, " +
                    "  relates recipe-utensil, " +
                    "  relates recipe-cooker;" +
                    "indian-recipe sub recipe, " +
//                    "  relates ingredient, " +  // TODO remove once inherited
//                    "  relates utensil, " +     // TODO remove once inherited
//                    "  relates cooker," +       // TODO remove once inherited
                    "  relates indian-recipe-spice as recipe-ingredient, " +
                    "  relates indian-recipe-ladle as recipe-utensil, " +
                    "  relates indian-recipe-grill as recipe-cooker; " +
                    "vegetable sub entity," +
                    "  plays recipe-ingredient;" +
                    "cutlery sub entity," +
                    "  plays recipe-utensil;" +
                    "heated-element sub entity," +
                    "  plays recipe-cooker;" +
                    "ginger sub vegetable," +
                    "  plays indian-recipe-spice;" +
                    "spoon sub cutlery," +
                    "  plays indian-recipe-ladle;" +
                    "oven sub heated-element," +
                    "  plays indian-recipe-grill; " +
                    //plays with ambiguity, unrelated to above hierarchies
                    "toaster sub entity," +
                    "  plays recipe-utensil," +
                    "  plays recipe-cooker;").asDefine());
            tx.commit();
        }
    }

    @AfterClass
    public static void closeSession(){
        session.close();
    }

    @Before
    public void setup() {
        tx = session.transaction(Transaction.Type.WRITE);
    }

    @After
    public void tearDown() {
        tx.close();
    }


    /*

      Using Binary Relations

     */

    @Test
    public void testRoleInference_TypedBinaryRelation(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{ ($x, $y); $x isa person; $y isa item; };";
        String patternString2 = "{ ($x, $y) isa ownership; $x isa person; $y isa item; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("owner"), new Variable("x"),
                tx.getRole("owned"), new Variable("y"));
        roleInference(patternString, correctRoleMap, reasonerQueryFactory);
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
    }

    @Test
    public void testRoleInference_TypedBinaryRelation_SingleTypeMissing(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{ ($x, $y); $x isa item; };";
        String patternString2 = "{ ($x, $y) isa ownership; $x isa item; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("owned"), new Variable("x"),
                tx.getRole("role"), new Variable("y"));
        roleInference(patternString, correctRoleMap, reasonerQueryFactory);
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
    }


    @Test //for each role player role mapping is ambiguous so metarole has to be assigned
    public void testRoleInference_NoInformationPresent(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();
        String relationString = "{ ($x, $y); };";
        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role"), new Variable("x"),
                tx.getRole("role"), new Variable("y"));
        roleInference(relationString, correctRoleMap, reasonerQueryFactory);
    }

    @Test //for each role player role mapping is ambiguous so metarole has to be assigned
    public void testRoleInference_MetaRelationType(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();
        String relationString = "{ ($x, $y) isa relation; };";
        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("role"), new Variable("x"),
                tx.getRole("role"), new Variable("y"));
        roleInference(relationString, correctRoleMap, reasonerQueryFactory);
    }

    @Test //missing role is ambiguous without cardinality constraints
    public void testRoleInference_RoleHierarchyInvolved() {
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String ownershipRelation = "{ ($x, $generic) isa ownership; $x isa reader; };";
        String borrowingRelation = "{ (owner: $x, owned: $y) isa borrowing; };";

        ImmutableSetMultimap<Role, Variable> correctOwnerKnownMap = ImmutableSetMultimap.of(
                tx.getRole("owner"), new Variable("x"), // is an owner or a subtype!
                tx.getRole("role"), new Variable("generic"));
        // cannot deduce any further that what is given
        ImmutableSetMultimap<Role, Variable> correctLenderKnownMap = ImmutableSetMultimap.of(
                tx.getRole("owner"), new Variable("x"),
                tx.getRole("owned"), new Variable("y"));
        roleInference(ownershipRelation, correctOwnerKnownMap, reasonerQueryFactory);
        roleInference(borrowingRelation, correctLenderKnownMap, reasonerQueryFactory);
    }


    @Test //relation relates a single role so instead of assigning metarole this role should be assigned
    public void testRoleInference_RelationHasVerticalRoleHierarchy(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        // should be able to infer the most-specialised role possible, when all
        // roles played are of the same ancestor (friend -> best-friend)
        String relationString = "{ ($x) isa best-friendship; };";
        ImmutableSetMultimap<Role, Variable> roleMap = ImmutableSetMultimap.of(
                tx.getRole("friend"), new Variable("x"));
        roleInference(relationString, roleMap, reasonerQueryFactory);
    }


    /*

      Using ternary relations

     */

    @Test //each type maps to a specific role
    public void testRoleInference_TypedTernaryRelationWithKnownRole(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{  ($x, $y, recipe-cooker: $z);$x isa vegetable;$y isa cutlery;  };";
        String patternString2 = "{ ($x, $y, recipe-cooker: $z) isa recipe; $x isa vegetable;$y isa cutlery; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("recipe-ingredient"), new Variable("x"),
                tx.getRole("recipe-utensil"), new Variable("y"),
                tx.getRole("recipe-cooker"), new Variable("z"));
        roleInference(patternString, correctRoleMap, reasonerQueryFactory);
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
    }

    @Test //without cardinality constraints the $y variable can be mapped to any of the three roles hence metarole is assigned
    public void testRoleInference_TypedTernaryRelation(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{ ($x, $y, $z); $x isa vegetable; $y isa cutlery; };";
        String patternString2 = "{ ($x, $y, $z) isa recipe; $x isa vegetable; $y isa cutlery; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("recipe-ingredient"), new Variable("x"),
                tx.getRole("recipe-utensil"), new Variable("y"),
                tx.getRole("role"), new Variable("z"));
        roleInference(patternString, correctRoleMap, reasonerQueryFactory);
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
    }

    @Test
    public void testRoleInference_TernaryRelationWithRepeatingRolePlayers(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{ (recipe-ingredient: $x, recipe-utensil: $y, $y); };";
        String patternString2 = "{ (recipe-ingredient: $x, recipe-utensil: $y, $y) isa recipe; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("recipe-ingredient"), new Variable("x"),
                tx.getRole("recipe-utensil"), new Variable("y"),
                tx.getRole("role"), new Variable("y"));
        roleInference(patternString, correctRoleMap, reasonerQueryFactory);
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
    }

    @Test
    public void testRoleInference_TypedTernaryRelation_TypesPlaySubRoles_SubRolesAreCorrectlyIdentified(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString2 = "{ (role: $x, role: $y, role: $z) isa indian-recipe; $x isa ginger; $y isa spoon; $z isa oven; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("indian-recipe-spice"), new Variable("x"),
                tx.getRole("indian-recipe-ladle"), new Variable("y"),
                tx.getRole("indian-recipe-grill"), new Variable("z"));
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
    }

    @Test
    public void testRoleInference_TypedTernaryRelationWithMetaRoles_MetaRolesShouldBeOverwritten(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{ (role: $x, role: $y, role: $z); $x isa vegetable; $y isa cutlery; $z isa heated-element; };";
        String patternString2 = "{ (role: $x, role: $y, role: $z) isa recipe; $x isa vegetable; $y isa cutlery; $z isa heated-element; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("recipe-ingredient"), new Variable("x"),
                tx.getRole("recipe-utensil"), new Variable("y"),
                tx.getRole("recipe-cooker"), new Variable("z"));
        roleInference(patternString, correctRoleMap, reasonerQueryFactory);
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
    }

    @Test
    public void testRoleInference_TypedTernaryRelation_TypesAreSubTypes_TopRolesShouldBeChosen(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{ (role: $x, role: $y, role: $z); $x isa ginger; $y isa spoon; $z isa oven; };";
        String patternString2 = "{ (role: $x, role: $y, role: $z) isa recipe; $x isa ginger; $y isa spoon; $z isa oven; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("recipe-ingredient"), new Variable("x"),
                tx.getRole("recipe-utensil"), new Variable("y"),
                tx.getRole("recipe-cooker"), new Variable("z"));
        roleInference(patternString, correctRoleMap, reasonerQueryFactory);
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
    }

    @Test
    public void testRoleInference_TypedTernaryRelation_TypesCanPlayMultipleRoles_MostSuperCommonRoleIsChosen(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String patternString = "{ ($x, $y, $z); $x isa vegetable; $y isa vegetable; $z isa vegetable; };";
        String patternString2 = "{ ($x, $y, $z) isa recipe; $x isa vegetable; $y isa vegetable; $z isa vegetable; };";

        ImmutableSetMultimap<Role, Variable> correctRoleMap = ImmutableSetMultimap.of(
                tx.getRole("recipe-ingredient"), new Variable("x"),
                tx.getRole("recipe-ingredient"), new Variable("y"),
                tx.getRole("recipe-ingredient"), new Variable("z"));
        roleInference(patternString, correctRoleMap, reasonerQueryFactory);
        roleInference(patternString2, correctRoleMap, reasonerQueryFactory);
    }

    @Test
    public void testRoleInference_WithMetaType(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String relationString = "{ ($x, $y, $z) isa recipe; $x isa vegetable; $y isa toaster; $z isa toaster; };";
        RelationAtom relation = (RelationAtom) reasonerQueryFactory.atomic(conjunction(relationString)).getAtom();
        ImmutableSetMultimap<Role, Variable> roleMap = ImmutableSetMultimap.of(
                tx.getRole("recipe-ingredient"), new Variable("x"),
                tx.getRole("role"), new Variable("y"),
                tx.getRole("role"), new Variable("z"));
        assertEquals(roleMap, roleSetMap(relation.getRoleVarMap()));
    }

    @Test
    public void testRoleInference_RoleMappingUnambiguous(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String relationString = "{ ($x, $y, $z) isa recipe; $x isa vegetable; $y isa toaster; $z isa toaster; };";
        ImmutableSetMultimap<Role, Variable> correctMap = ImmutableSetMultimap.of(
                tx.getRole("recipe-ingredient"), new Variable("x"),
                tx.getRole("role"), new Variable("y"),
                tx.getRole("role"), new Variable("z"));
        roleInference(relationString, correctMap, reasonerQueryFactory);
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
