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

package grakn.core.graql.reasoner.query;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import grakn.core.common.config.Config;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestStorage;
import grakn.core.rule.SessionUtil;
import grakn.core.rule.TestTransactionProvider;
import graql.lang.Graql;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static grakn.core.graql.reasoner.query.QueryTestUtil.atomicEquivalence;
import static grakn.core.graql.reasoner.query.QueryTestUtil.conjunction;
import static grakn.core.graql.reasoner.query.QueryTestUtil.queryEquivalence;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;

@SuppressWarnings("CheckReturnValue")
public class AtomicQueryEquivalenceIT {

    private static String resourcePath = "test-integration/graql/reasoner/resources/";

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session genericSchemaSession;

    private Transaction tx;
    // Transaction-bound reasonerQueryFactory
    private ReasonerQueryFactory reasonerQueryFactory;

    @BeforeClass
    public static void loadContext(){
        Config mockServerConfig = storage.createCompatibleServerConfig();
        genericSchemaSession = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);
        loadFromFileAndCommit(resourcePath, "genericSchema.gql", genericSchemaSession);
    }

    @AfterClass
    public static void closeSession(){
        genericSchemaSession.close();
    }

    @Before
    public void setUp(){
        tx = genericSchemaSession.writeTransaction();
        reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();
    }

    @After
    public void tearDown(){
        tx.close();
    }

    @Test
    public void testEquivalence_DifferentIsaVariants(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String query = "{ $x isa baseRoleEntity; };";
        String query2 = "{ $y isa $type;$type type baseRoleEntity; };";
        String query3 = "{ $z isa $t;$t type baseRoleEntity; };";
        String query4 = "{ $x isa $y; };";
        String query5 = "{ $x isa subRoleEntity; };";

        ArrayList<String> queries = Lists.newArrayList(query, query2, query3, query4, query5);

        equivalence(query, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query2, queries, Lists.newArrayList(query3), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query2, queries, Lists.newArrayList(query3), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query3, queries, Lists.newArrayList(query2), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query3, queries, Lists.newArrayList(query2), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);
    }

    @Test
    public void testEquivalence_DifferentSubVariants(){
        testEquivalence_DifferentOntologicalVariants(tx, "sub", "baseRoleEntity", "baseRole1");
    }

    @Test
    public void testEquivalence_DifferentPlaysVariants(){
        testEquivalence_DifferentOntologicalVariants(tx, "plays", "baseRole1", "baseRole2");
    }

    @Test
    public void testEquivalence_DifferentRelatesVariants(){
        testEquivalence_DifferentOntologicalVariants(tx, "relates", "baseRole1", "baseRole2");
    }

    private void testEquivalence_DifferentOntologicalVariants(Transaction tx, String keyword, String label, String label2){

        String query = "{ $x " + keyword + " " + label + "; };";
        String query2 = "{ $y " + keyword + " $type;$type type " + label +"; };";
        String query3 = "{ $z " + keyword + " $t;$t type " + label +"; };";
        String query4 = "{ $x " + keyword + " $y; };";
        String query5 = "{ $x " + keyword + " " + label2 + "; };";

        ArrayList<String> queries = Lists.newArrayList(query, query2, query3, query4, query5);

        equivalence(query, queries, Lists.newArrayList(query2, query3), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query, queries, Lists.newArrayList(query2, query3), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query2, queries, Lists.newArrayList(query, query3), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query2, queries, Lists.newArrayList(query, query3), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query3, queries, Lists.newArrayList(query, query2), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query3, queries, Lists.newArrayList(query, query2), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);
    }

    @Test
    public void testEquivalence_DifferentHasVariants(){
        String query = "{ $x has resource; };";
        String query2 = "{ $y has resource; };";
        String query3 = "{ $x has " + Graql.Token.Type.ATTRIBUTE + "; };";

        ArrayList<String> queries = Lists.newArrayList(query, query2, query3);

        equivalence(query, queries, Lists.newArrayList(query2), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query, queries, Lists.newArrayList(query2), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query2, queries, Lists.newArrayList(query), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query2, queries, Lists.newArrayList(query), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);
    }

    @Test
    public void testEquivalence_TypesWithSameLabel(){
        String isaQuery = "{ $x isa baseRoleEntity; };";
        String subQuery = "{ $x sub baseRoleEntity; };";

        String playsQuery = "{ $x plays baseRole1; };";
        String relatesQuery = "{ $x relates baseRole1; };";
        String hasQuery = "{ $x has resource; };";
        String subQuery2 = "{ $x sub baseRole1; };";

        ArrayList<String> queries = Lists.newArrayList(isaQuery, subQuery, playsQuery, relatesQuery, hasQuery, subQuery2);

        equivalence(isaQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(isaQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(subQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(subQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(playsQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(playsQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(relatesQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(relatesQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(hasQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(hasQuery, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);
    }

    @Test
    public void testEquivalence_DifferentRelationInequivalentVariants(){

        HashSet<String> queries = Sets.newHashSet(
                "{ $x isa binary; };",
                "{ ($y) isa binary; };",

                "{ ($x, $y); };",
                "{ ($x, $y) isa binary; };",
                "{ (baseRole1: $x, baseRole2: $y) isa binary; };",
                "{ (role: $y, baseRole2: $z) isa binary; };",
                "{ (role: $y, baseRole2: $z) isa $type; };",
                "{ (role: $y, baseRole2: $z) isa $type; $type type binary; };",
                "{ (role: $x, role: $x, baseRole2: $z) isa binary; };",

                "{ $x ($y, $z) isa binary; };",
                "{ $x (baseRole1: $y, baseRole2: $z) isa binary; };"
        );

        queries.forEach(qA -> {
            queries.stream()
                    .filter(qB -> !qA.equals(qB))
                    .forEach(qB -> {
                        equivalence(qA, qB, false, ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
                        equivalence(qA, qB, false, ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);
                    });
        });
    }

    @Test
    public void testEquivalence_RelationWithRepeatingVariables(){
        String query = "{ (baseRole1: $x, baseRole2: $y); };";
        String query2 = "{ (baseRole1: $x, baseRole2: $x); };";

        equivalence(query, query2, false, ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query, query2, false, ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);
    }

    @Test
    public void testEquivalence_RelationsWithTypedRolePlayers(){
        String query = "{ (role: $x, role: $y);$x isa baseRoleEntity; };";
        String query2 = "{ (role: $x, role: $y);$y isa baseRoleEntity; };";
        String query3 = "{ (role: $x, role: $y);$x isa subRoleEntity; };";
        String query4 = "{ (role: $x, role: $y);$y isa baseRoleEntity;$x isa baseRoleEntity; };";
        String query5 = "{ (baseRole1: $x, baseRole2: $y);$x isa baseRoleEntity; };";
        String query6 = "{ (baseRole1: $x, baseRole2: $y);$y isa baseRoleEntity; };";
        String query7 = "{ (baseRole1: $x, baseRole2: $y);$x isa baseRoleEntity;$y isa subRoleEntity; };";
        String query8 = "{ (baseRole1: $x, baseRole2: $y);$x isa baseRoleEntity;$y isa baseRoleEntity; };";

        ArrayList<String> queries = Lists.newArrayList(query, query2, query3, query4, query5, query6, query7, query8);

        equivalence(query, queries, Collections.singletonList(query2), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query, queries, Collections.singletonList(query2), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query2, queries, Collections.singletonList(query), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query2, queries, Collections.singletonList(query), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query6, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query6, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query7, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query7, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);
    }

    @Test
    public void testEquivalence_RelationsWithSubstitution(){
        String query = "{ (role: $x, role: $y);$x id V666; };";
        String queryb = "{ (role: $x, role: $y);$y id V666; };";

        String query2 = "{ (role: $x, role: $y);$x id V666;$y id V667; };";
        String query2b = "{ (role: $x, role: $y);$y id V666;$x id V667; };";

        String query3 = "{ (role: $x, role: $y);$x id V666;$y id V666; };";

        String query4 = "{ (baseRole1: $x, baseRole2: $y);$x id V666;$y id V667; };";
        String query5 = "{ (baseRole1: $x, baseRole2: $y);$y id V666;$x id V667; };";

        ArrayList<String> queries = Lists.newArrayList(query, queryb, query2, query2b, query3, query4, query5);

        equivalence(query, queries, Collections.singletonList(queryb), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query, queries, Collections.singletonList(queryb), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(queryb, queries, Collections.singletonList(query), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(queryb, queries, Collections.singletonList(query), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query2, queries, Collections.singletonList(query2b), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query2, queries, Lists.newArrayList(query2b, query3), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query2b, queries, Collections.singletonList(query2), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query2b, queries, Lists.newArrayList(query2, query3), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query4, queries, Collections.singletonList(query5), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query5, queries, Collections.singletonList(query4), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query3, queries, Lists.newArrayList(query2, query2b), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);
    }

    @Test
    public void testEquivalence_RelationsWithSubstitution_differentRolesMapped(){
        String query = "{ (baseRole1: $x, baseRole2: $y);$x id V666; };";
        String query2 = "{ (baseRole1: $x, baseRole2: $y);$x id V667; };";
        String query3 = "{ (baseRole1: $x, baseRole2: $y);$y id V666; };";
        String query4 = "{ (baseRole1: $x, baseRole2: $y);$y id V667; };";

        String query5 = "{ (baseRole1: $x, baseRole2: $y);$x id V666;$y id V667; };";
        String query6 = "{ (baseRole1: $x, baseRole2: $y);$y id V666;$x id V667; };";
        ArrayList<String> queries = Lists.newArrayList(query, query2, query3, query4, query6);

        equivalence(query, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query, queries, Collections.singletonList(query2), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query2, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query2, queries, Collections.singletonList(query), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query3, queries, Collections.singletonList(query4), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query4, queries, Collections.singletonList(query3), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query5, queries, Collections.singletonList(query6), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);
    }

    @Test
    public void testEquivalence_ResourcesAsRoleplayers(){
        String query = "{ (baseRole1: $x, baseRole2: $y);$x == 'V666'; };";
        String query2 = "{ (baseRole1: $x, baseRole2: $y);$x == 'V667'; };";

        String query3 = "{ (baseRole1: $x, baseRole2: $y);$y == 'V666'; };";
        String query4 = "{ (baseRole1: $x, baseRole2: $y);$y == 'V667'; };";

        String query5 = "{ (baseRole1: $x, baseRole2: $y);$x == 'V666';$y == 'V667'; };";
        String query6 = "{ (baseRole1: $x, baseRole2: $y);$y == 'V666';$x == 'V667'; };";
        ArrayList<String> queries = Lists.newArrayList(query, query2, query3, query4, query5, query6);

        equivalence(query, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query2, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query2, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query4, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query5, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);
    }

    @Test
    public void testEquivalence_RelationsWithVariableAndSubstitution(){
        String query = "{ $r (baseRole1: $x);$x id V666; };";
        String query2 = "{ $a (baseRole1: $x);$x id V667; };";
        String query3 = "{ $b (baseRole2: $y);$y id V666; };";
        ArrayList<String> queries = Lists.newArrayList(query, query2, query3);

        equivalence(query, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query, queries, Collections.singletonList(query2), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query2, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query2, queries, Collections.singletonList(query), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);

        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query3, queries, new ArrayList<>(), ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);
    }

    private void equivalence(String target, List<String> queries, List<String> equivalentQueries, ReasonerQueryEquivalence equiv, ReasonerQueryFactory reasonerQueryFactory){
        queries.forEach(q -> equivalence(target, q, equivalentQueries.contains(q) || q.equals(target), equiv,reasonerQueryFactory));
    }

    private void equivalence(String patternA, String patternB, boolean expectation, ReasonerQueryEquivalence equiv, ReasonerQueryFactory reasonerQueryFactory){
        ReasonerAtomicQuery a = reasonerQueryFactory.atomic(conjunction(patternA));
        ReasonerAtomicQuery b = reasonerQueryFactory.atomic(conjunction(patternB));
        queryEquivalence(a, b, expectation, equiv);
        atomicEquivalence(a.getAtom(), b.getAtom(), expectation, equiv.atomicEquivalence());
    }
}