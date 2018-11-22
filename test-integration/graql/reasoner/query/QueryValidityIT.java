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

package grakn.core.graql.reasoner.query;

import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.QueryBuilder;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.server.session.SessionImpl;
import grakn.core.graql.query.Query;
import grakn.core.server.session.TransactionImpl;
import grakn.core.rule.GraknTestServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

@SuppressWarnings("CheckReturnValue")
public class QueryValidityIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl genericSchemaSession;

    private static void loadFromFile(String fileName, Session session){
        try {
            InputStream inputStream = QueryValidityIT.class.getClassLoader().getResourceAsStream("test-integration/graql/reasoner/resources/"+fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            Transaction tx = session.transaction(Transaction.Type.WRITE);
            tx.graql().parser().parseList(s).forEach(Query::execute);
            tx.commit();
        } catch (Exception e){
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    private static TransactionImpl tx;

    @BeforeClass
    public static void loadContext(){
        genericSchemaSession = server.sessionWithNewKeyspace();
        loadFromFile("ruleApplicabilityTest.gql", genericSchemaSession);
        tx = genericSchemaSession.transaction(Transaction.Type.WRITE);
    }

    @AfterClass
    public static void closeSession(){
        tx.close(); genericSchemaSession.close();
    }


    @Test
    public void whenQueryingForInexistentConceptId_emptyResultReturned(){
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x id 'V1337'; $y id 'V456'; ($x, $y); get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }

    @Test
    public void whenQueryingForInexistentEntityTypeId_emptyResultReturned(){
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x isa $type; $type id 'V1337'; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }

    @Test
    public void whenQueryingForInexistentRelationTypeId_emptyResultReturned(){
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match ($x, $y) isa $type; $type id 'V1337'; get;";
        String queryString2 = "match $r ($x, $y) isa $type; $r id 'V1337'; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
        assertThat(qb.<GetQuery>parse(queryString2).execute(), empty());
    }

    @Test
    public void whenQueryingForInexistentResourceId_emptyResultReturned(){
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x has name $y; $x id 'V1337'; get;";
        String queryString2 = "match $x has name $y; $y id 'V1337'; get;";
        String queryString3 = "match $x has name $y via $r; $r id 'V1337'; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
        assertThat(qb.<GetQuery>parse(queryString2).execute(), empty());
        assertThat(qb.<GetQuery>parse(queryString3).execute(), empty());
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForInexistentEntityTypeLabel_Throws() throws GraqlQueryException{
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x isa polok; get;";
        qb.<GetQuery>parse(queryString).execute();
    }

    @Test
    public void whenQueryingForInexistentEntityTypeLabelViaVariable_emptyResultReturned(){
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x isa $type; $type label polok; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForMismatchedResourceTypeLabel_Throws() throws GraqlQueryException{
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x has binary $r; get;";
        qb.<GetQuery>parse(queryString).execute();
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForInexistentRelationTypeLabel_Throws() throws GraqlQueryException{
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match ($x, $y) isa jakas-relacja; get;";
        qb.<GetQuery>parse(queryString).execute();
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForMismatchedRelationTypeLabel_Throws() throws GraqlQueryException{
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match ($x, $y) isa name; get;";
        qb.<GetQuery>parse(queryString).execute();
    }

    @Test
    public void whenQueryingForInexistentRelationTypeLabelViaVariable_emptyResultReturned(){
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match ($x, $y) isa $type; $type label jakas-relacja; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForRelationWithNonRoleRoles_Throws() throws GraqlQueryException{
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match (entity: $x, entity: $y) isa relationship; get;";
        qb.<GetQuery>parse(queryString).execute();
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForRelationWithNonExistentRoles_Throws() throws GraqlQueryException{
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match (rola: $x, rola: $y) isa relationship; get;";
        qb.<GetQuery>parse(queryString).execute();
    }

    @Test
    public void whenQueryingForRelationWithIllegalRoles_emptyResultReturned(){
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match (anotherRole: $x) isa binary; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }

    @Test
    public void whenQueryingForIllegalRolePlayer_emptyResultReturned(){
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match ($x, $y) isa binary; $x isa anotherNoRoleEntity; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }

    @Test
    public void whenQueryingForIllegalResource_emptyResultReturned(){
        QueryBuilder qb = tx.graql().infer(true);
        String queryString = "match $x has name $n; $x isa binary; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }
}
