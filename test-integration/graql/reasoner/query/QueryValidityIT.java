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

import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.Graql;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionImpl;
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

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class QueryValidityIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl genericSchemaSession;

    private static void loadFromFile(String fileName, Session session){
        try {
            InputStream inputStream = QueryValidityIT.class.getClassLoader().getResourceAsStream("test-integration/graql/reasoner/resources/"+fileName);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            Transaction tx = session.transaction(Transaction.Type.WRITE);
            Graql.parseList(s).forEach(tx::execute);
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
                String queryString = "match $x id 'V1337'; $y id 'V456'; ($x, $y); get;";
        assertThat(tx.execute(Graql.<GetQuery>parse(queryString)), empty());
    }

    @Test
    public void whenQueryingForInexistentEntityTypeId_emptyResultReturned(){
                String queryString = "match $x isa $type; $type id 'V1337'; get;";
        assertThat(tx.execute(Graql.<GetQuery>parse(queryString)), empty());
    }

    @Test
    public void whenQueryingForInexistentRelationTypeId_emptyResultReturned(){
                String queryString = "match ($x, $y) isa $type; $type id 'V1337'; get;";
        String queryString2 = "match $r ($x, $y) isa $type; $r id 'V1337'; get;";
        assertThat(tx.execute(Graql.<GetQuery>parse(queryString)), empty());
        assertThat(tx.execute(Graql.<GetQuery>parse(queryString2)), empty());
    }

    @Test
    public void whenQueryingForInexistentResourceId_emptyResultReturned(){
                String queryString = "match $x has name $y; $x id 'V1337'; get;";
        String queryString2 = "match $x has name $y; $y id 'V1337'; get;";
        String queryString3 = "match $x has name $y via $r; $r id 'V1337'; get;";
        assertThat(tx.execute(Graql.<GetQuery>parse(queryString)), empty());
        assertThat(tx.execute(Graql.<GetQuery>parse(queryString2)), empty());
        assertThat(tx.execute(Graql.<GetQuery>parse(queryString3)), empty());
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForInexistentEntityTypeLabel_Throws() throws GraqlQueryException{
                String queryString = "match $x isa polok; get;";
        tx.execute(Graql.<GetQuery>parse(queryString));
    }

    @Test
    public void whenQueryingForInexistentEntityTypeLabelViaVariable_emptyResultReturned(){
                String queryString = "match $x isa $type; $type label polok; get;";
        assertThat(tx.execute(Graql.<GetQuery>parse(queryString)), empty());
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForMismatchedResourceTypeLabel_Throws() throws GraqlQueryException{
                String queryString = "match $x has binary $r; get;";
        tx.execute(Graql.<GetQuery>parse(queryString));
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForInexistentRelationTypeLabel_Throws() throws GraqlQueryException{
                String queryString = "match ($x, $y) isa jakas-relacja; get;";
        tx.execute(Graql.<GetQuery>parse(queryString));
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForMismatchedRelationTypeLabel_Throws() throws GraqlQueryException{
                String queryString = "match ($x, $y) isa name; get;";
        tx.execute(Graql.<GetQuery>parse(queryString));
    }

    @Test
    public void whenQueryingForInexistentRelationTypeLabelViaVariable_emptyResultReturned(){
                String queryString = "match ($x, $y) isa $type; $type label jakas-relacja; get;";
        assertThat(tx.execute(Graql.<GetQuery>parse(queryString)), empty());
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForRelationWithNonRoleRoles_Throws() throws GraqlQueryException{
                String queryString = "match (entity: $x, entity: $y) isa relationship; get;";
        tx.execute(Graql.<GetQuery>parse(queryString));
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForRelationWithNonExistentRoles_Throws() throws GraqlQueryException{
                String queryString = "match (rola: $x, rola: $y) isa relationship; get;";
        tx.execute(Graql.<GetQuery>parse(queryString));
    }

    @Test
    public void whenQueryingForRelationWithIllegalRoles_emptyResultReturned(){
                String queryString = "match (anotherRole: $x) isa binary; get;";
        assertThat(tx.execute(Graql.<GetQuery>parse(queryString)), empty());
    }

    @Test
    public void whenQueryingForIllegalRolePlayer_emptyResultReturned(){
                String queryString = "match ($x, $y) isa binary; $x isa anotherNoRoleEntity; get;";
        assertThat(tx.execute(Graql.<GetQuery>parse(queryString)), empty());
    }

    @Test
    public void whenQueryingForIllegalResource_emptyResultReturned(){
                String queryString = "match $x has name $n; $x isa binary; get;";
        assertThat(tx.execute(Graql.<GetQuery>parse(queryString)), empty());
    }
}
