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

package grakn.core.graql.reasoner.reasoning;

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.QueryBuilder;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionImpl;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static grakn.core.graql.query.pattern.Pattern.var;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static org.junit.Assert.assertEquals;

public class VariableRolesIT {

    private static String resourcePath = "test-integration/graql/reasoner/stubs/";

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl variableRoleSession;


    @BeforeClass
    public static void loadContext(){
        variableRoleSession = server.sessionWithNewKeyspace();
        loadFromFileAndCommit(resourcePath,"testSet29.gql", variableRoleSession);
    }

    @AfterClass
    public static void closeSession(){
        variableRoleSession.close();
    }

    @Test
    public void binaryRelationWithDifferentVariantsOfVariableRoles(){
        try(TransactionImpl tx = variableRoleSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql();

            //9 binary-base instances with {role, role2} = 2 roles for r2 -> 18 answers
            /*
            String queryString = "match " +
                    "(role1: $a, $r2: $b) isa binary-base;" +
                    "get;";

            String equivalentQueryString = "match " +
                    "($r1: $a, $r2: $b) isa binary-base;" +
                    "$r1 label 'role1';" +
                    "get $a, $b, $r2;";

            List<ConceptMap> answers = tx.execute(Graql.<GetQuery>parse(queryString));
            List<ConceptMap> equivalentAnswers = tx.execute(Graql.<GetQuery>parse(equivalentQueryString));
            assertEquals(18, answers.size());
            assertTrue(CollectionUtils.isEqualCollection(answers, equivalentAnswers));

            */
            //9 binary-base instances with {role, role1, role2} = 3 roles for r2 -> 27 answers
            String queryString2 = "match " +
                    "(role: $a, $r2: $b) isa binary-base;" +
                    "get;";

            String equivalentQueryString2 = "match " +
                    "($r1: $a, $r2: $b) isa binary-base;" +
                    "$r1 label 'role';" +
                    "get $a, $b, $r2;";

            GetQuery query2 = qb.<GetQuery>parse(queryString2);
            //System.out.println(query2);
            //List<ConceptMap> answers2 = tx.execute(query2);
            //System.out.println();

            GetQuery equivQuery2 = qb.<GetQuery>parse(equivalentQueryString2);
            System.out.println(equivQuery2);
            List<ConceptMap> equivalentAnswers2 = tx.execute(equivQuery2);
            System.out.println();

            //assertEquals(27, answers2.size());
            //assertCollectionsEqual(answers2, equivalentAnswers2);

            /*
            //role variables bound hence should return original 9 instances
            String queryString3 = "match " +
                    "($r1: $a, $r2: $b) isa binary-base;" +
                    "$r1 label 'role';" +
                    "$r2 label 'role2';" +
                    "get $a, $b;";

            String equivalentQueryString3 = "match " +
                    "(role1: $a, role2: $b) isa binary-base;" +
                    "get;";

            List<ConceptMap> answers3 = tx.execute(Graql.<GetQuery>parse(queryString3));
            List<ConceptMap> equivalentAnswers3 = tx.execute(Graql.<GetQuery>parse(equivalentQueryString3));
            assertEquals(9, answers3.size());
            assertCollectionsEqual(answers3, equivalentAnswers3);

            //9 relation instances with 7 possible permutations for each - 63 answers
            String queryString4 = "match " +
                    "($r1: $a, $r2: $b) isa binary-base;" +
                    "get;";

            List<ConceptMap> answers4 = tx.execute(Graql.<GetQuery>parse(queryString4));
            assertEquals(63, answers4.size());
            */
        }
    }

    @Test
    public void binaryRelationWithVariableRoles_basicSet(){
        final int conceptDOF = 2;
        ternaryNaryRelationWithVariableRoles("binary", conceptDOF);
    }

    @Test
    public void binaryRelationWithVariableRoles_extendedSet(){
        final int conceptDOF = 3;
        ternaryNaryRelationWithVariableRoles("binary-base", conceptDOF);
    }

    @Test
    public void ternaryRelationWithVariableRoles_basicSet(){
        /*
        As each vertex is a starting point for {a, b, c} x {a, b c} = 9 relations, starting with a we have:

        (r1: a, r2: a, r3: a), (r1: a, r2: a, r3: b), (r1: a, r2: a, r3: c)
        (r1: a, r2: b, r3: a), (r1: a, r2: b, r3: b), (r1: a, r2: b, r3: c)
        (r1: a, r2: c, r3: a), (r1: a, r2: c, r3: b), (r1: a. r2: c, r3: c)

        If we generify two roles each of these produces 7 answers, taking (r1: a, r2: b, r3:c) we have:

        (a, r2: b, r3: c)
        (a, r: b, r3: c)
        (a, r2: b, r: c)
        (a, r3: c, r2: b)
        (a, r3: c, r: b)
        (a, r: c, r2: b)
        (a, r: b, r: c)

        plus
        (a, r: c, r: b) but this one is counted in (r1: a, r2: c, r3:b)
        hence 7 answers per single relation.
        */
        final int conceptDOF = 2;
        ternaryNaryRelationWithVariableRoles("ternary", conceptDOF);
    }

    @Test
    public void ternaryRelationWithVariableRoles_extendedSet(){
        final int conceptDOF = 3;
        ternaryNaryRelationWithVariableRoles("ternary-base", conceptDOF);
    }

    @Test
    public void quaternaryRelationWithVariableRoles_basicSet(){
        final int conceptDOF = 2;
        ternaryNaryRelationWithVariableRoles("quaternary", conceptDOF);
    }

    @Test
    public void quaternaryRelationWithVariableRoles2_extendedSet(){
        final int conceptDOF = 3;
        ternaryNaryRelationWithVariableRoles("quaternary-base", conceptDOF);
    }

    private void ternaryNaryRelationWithVariableRoles(String label, int conceptDOF){
        try(TransactionImpl tx = variableRoleSession.transaction(Transaction.Type.WRITE)) {
            QueryBuilder qb = tx.graql();
            final int arity = (int) tx.getRelationshipType(label).roles().count();

            Statement resourcePattern = var("a1").has("name", "a");

            //This query generalises all roles but the first one.
            Statement pattern = var().rel("role1", "a1");
            for (int i = 2; i <= arity; i++) pattern = pattern.rel(var("r" + i), "a" + i);
            pattern = pattern.isa(label);

            List<ConceptMap> answers = tx.execute(Graql.match(pattern, resourcePattern).get());
            assertEquals(answerCombinations(arity - 1, conceptDOF), answers.size());

            //We get extra conceptDOF degrees of freedom by removing the resource constraint on $a1 and the set is symmetric.
            List<ConceptMap> answers2 = tx.execute(Graql.match(pattern).get());
            assertEquals(answerCombinations(arity - 1, conceptDOF) * conceptDOF, answers2.size());


            //The general case of mapping all available Rps
            Statement generalPattern = var();
            for (int i = 1; i <= arity; i++) generalPattern = generalPattern.rel(var("r" + i), "a" + i);
            generalPattern = generalPattern.isa(label);

            List<ConceptMap> answers3 = tx.execute(Graql.match(generalPattern).get());
            assertEquals(answerCombinations(arity, conceptDOF), answers3.size());
        }
    }

    /**
     *Each role player variable can be mapped to either of the conceptDOF concepts and these can repeat.
     *Each role variable can be mapped to either of RPs roles and only meta roles can repeat.

     *For the case of conceptDOF = 3, roleDOF = 3.
     *We start by considering the number of meta roles we allow.
     *If we consider only non-meta roles, considering each relation player we get:
     *C^3_0 x 3.3 x 3.2 x 3 = 162 combinations
     *
     *If we consider single metarole - C^3_1 = 3 possibilities of assigning them:
     *C^3_1 x 3.3 x 3.2 x 3 = 486 combinations
     *
     *Two metaroles - again C^3_2 = 3 possibilities of assigning them:
     *C^3_2 x 3.3 x 3   x 3 = 243 combinations
     *
     *Three metaroles, C^3_3 = 1 possiblity of assignment:
     *C^3_3 x 3   x 3   x 3 = 81 combinations
     *
     *-> Total = 918 different answers
     *In general, for i allowed meta roles we have:
     *C^{RP}_i PRODUCT_{j = RP-i}{ (conceptDOF)x(roleDOF-j) } x PRODUCT_i{ conceptDOF} } answers.
     *
     *So total number of answers is:
     *SUM_i{ C^{RP}_i PRODUCT_{j = RP-i}{ (conceptDOF)x(roleDOF-j) } x PRODUCT_i{ conceptDOF} }
     *
     * @param RPS number of relation players available
     * @param conceptDOF number of concept degrees of freedom
     * @return number of answer combinations
     */
    private int answerCombinations(int RPS, int conceptDOF) {
        int answers = 0;
        //i is the number of meta roles
        for (int i = 0; i <= RPS; i++) {
            int RPProduct = 1;
            //rps with non-meta roles
            for (int j = 0; j < RPS - i; j++) RPProduct *= conceptDOF * (RPS - j);
            //rps with meta roles
            for (int k = 0; k < i; k++) RPProduct *= conceptDOF;
            answers += CombinatoricsUtils.binomialCoefficient(RPS, i) * RPProduct;
        }
        return answers;
    }
}
