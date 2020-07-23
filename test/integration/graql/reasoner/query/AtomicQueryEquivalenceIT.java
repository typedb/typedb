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
import grakn.core.test.rule.GraknTestStorage;
import grakn.core.test.rule.SessionUtil;
import grakn.core.test.rule.TestTransactionProvider;
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

@SuppressWarnings("CheckReturnValue")
public class AtomicQueryEquivalenceIT {

    @ClassRule
    public static final GraknTestStorage storage = new GraknTestStorage();

    private static Session session;

    private Transaction tx;
    // Transaction-bound reasonerQueryFactory
    private ReasonerQueryFactory reasonerQueryFactory;

    @BeforeClass
    public static void loadContext() {
        Config mockServerConfig = storage.createCompatibleServerConfig();
        session = SessionUtil.serverlessSessionWithNewKeyspace(mockServerConfig);

        // define schema
        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
            tx.execute(Graql.parse("define " +
                    "organisation sub entity," +
                    "  has name, " +
                    "  plays employer," +
                    "  plays owner," +
                    "  abstract; " +
                    "company sub organisation;" +
                    "charity sub company;" +
                    "employment sub relation, " +
                    "  relates employer," +
                    "  relates employee;" +
                    "ownership sub relation," +
                    "  relates owner;" +
                    "name sub attribute, value string;").asDefine());
            tx.commit();
        }
    }


    @AfterClass
    public static void closeSession(){
        session.close();
    }

    @Before
    public void setUp(){
        tx = session.transaction(Transaction.Type.WRITE);
        reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();
        // disable type inference so we test fewer things
        reasonerQueryFactory.disableInferTypes();
    }

    @After
    public void tearDown(){
        tx.close();
    }

    @Test
    public void testEquivalence_DifferentIsaVariants(){
        ReasonerQueryFactory reasonerQueryFactory = ((TestTransactionProvider.TestTransaction)tx).reasonerQueryFactory();

        String query = "{ $x isa organisation; };";
        String query2 = "{ $y isa $type;$type type organisation; };";
        String query3 = "{ $z isa $t;$t type organisation; };";
        String query4 = "{ $x isa $y; };";
        String query5 = "{ $x isa company; };";

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
        testEquivalence_DifferentOntologicalVariants(tx, "sub", "organisation", "employee");
    }

    @Test
    public void testEquivalence_DifferentPlaysVariants(){
        testEquivalence_DifferentOntologicalVariants(tx, "plays", "employee", "employer");
    }

    @Test
    public void testEquivalence_DifferentRelatesVariants(){
        testEquivalence_DifferentOntologicalVariants(tx, "relates", "employee", "employer");
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
        String query = "{ $x has name; };";
        String query2 = "{ $y has name; };";
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
        String isaQuery = "{ $x isa organisation; };";
        String subQuery = "{ $x sub organisation; };";

        String playsQuery = "{ $x plays employer; };";
        String relatesQuery = "{ $x relates employer; };";
        String hasQuery = "{ $x has name; };";
        String subQuery2 = "{ $x sub employer; };";

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
                "{ $x isa employment; };",
                "{ ($y) isa employment; };",

                "{ ($x, $y); };",
                "{ ($x, $y) isa employment; };",
                "{ (employer: $x, employee: $y) isa employment; };",
                "{ (role: $y, employee: $z) isa employment; };",
                "{ (role: $y, employee: $z) isa $type; };",
                "{ (role: $y, employee: $z) isa $type; $type type employment; };",
                "{ (role: $x, role: $x, employee: $z) isa employment; };",

                "{ $x ($y, $z) isa employment; };",
                "{ $x (employer: $y, employee: $z) isa employment; };"
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
        String query = "{ (employer: $x, employee: $y); };";
        String query2 = "{ (employer: $x, employee: $x); };";

        equivalence(query, query2, false, ReasonerQueryEquivalence.AlphaEquivalence, reasonerQueryFactory);
        equivalence(query, query2, false, ReasonerQueryEquivalence.StructuralEquivalence, reasonerQueryFactory);
    }

    @Test
    public void testEquivalence_RelationsWithTypedRolePlayers(){
        String query = "{ (role: $x, role: $y);$x isa organisation; };";
        String query2 = "{ (role: $x, role: $y);$y isa organisation; };";
        String query3 = "{ (role: $x, role: $y);$x isa company; };";
        String query4 = "{ (role: $x, role: $y);$y isa organisation;$x isa organisation; };";
        String query5 = "{ (employer: $x, employee: $y);$x isa organisation; };";
        String query6 = "{ (employer: $x, employee: $y);$y isa organisation; };";
        String query7 = "{ (employer: $x, employee: $y);$x isa organisation;$y isa company; };";
        String query8 = "{ (employer: $x, employee: $y);$x isa organisation;$y isa organisation; };";

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

        String query4 = "{ (employer: $x, employee: $y);$x id V666;$y id V667; };";
        String query5 = "{ (employer: $x, employee: $y);$y id V666;$x id V667; };";

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
        String query = "{ (employer: $x, employee: $y);$x id V666; };";
        String query2 = "{ (employer: $x, employee: $y);$x id V667; };";
        String query3 = "{ (employer: $x, employee: $y);$y id V666; };";
        String query4 = "{ (employer: $x, employee: $y);$y id V667; };";

        String query5 = "{ (employer: $x, employee: $y);$x id V666;$y id V667; };";
        String query6 = "{ (employer: $x, employee: $y);$y id V666;$x id V667; };";
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
        String query = "{ (employer: $x, employee: $y);$x == 'V666'; };";
        String query2 = "{ (employer: $x, employee: $y);$x == 'V667'; };";

        String query3 = "{ (employer: $x, employee: $y);$y == 'V666'; };";
        String query4 = "{ (employer: $x, employee: $y);$y == 'V667'; };";

        String query5 = "{ (employer: $x, employee: $y);$x == 'V666';$y == 'V667'; };";
        String query6 = "{ (employer: $x, employee: $y);$y == 'V666';$x == 'V667'; };";
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
        String query = "{ $r (employer: $x);$x id V666; };";
        String query2 = "{ $a (employer: $x);$x id V667; };";
        String query3 = "{ $b (employee: $y);$y id V666; };";
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