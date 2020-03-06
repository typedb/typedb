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

package grakn.core.graql.query;

import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.exception.GraqlException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

@SuppressWarnings({"CheckReturnValue", "Duplicates"})
public class QueryValidityIT {

    private static String resourcePath = "test-integration/graql/reasoner/resources/";

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static Session genericSchemaSession;

    private static Transaction tx;

    @BeforeClass
    public static void loadContext(){
        genericSchemaSession = server.sessionWithNewKeyspace();
        tx = genericSchemaSession.writeTransaction();
        tx.execute(Graql.parse("define " +
                "anotherRole sub role;" +
                "someRel sub relation, relates anotherRole; " +
                "anotherNoRoleEntity sub entity, plays role1;" +
                "binary sub relation, relates role1;" +
                "name sub attribute, datatype string;").asDefine());
        tx.commit();
        tx = genericSchemaSession.writeTransaction();
    }

    @AfterClass
    public static void closeSession(){
        tx.close();
        genericSchemaSession.close();
    }

    @Test
    public void whenQueryingForRelationWithIllegalRoles_emptyResultReturned(){
        String queryString = "match (anotherRole: $x) isa binary; get;";
        assertThat(tx.execute(Graql.parse(queryString).asGet()), empty());
    }

    @Test
    public void whenQueryingForIllegalRolePlayer_emptyResultReturned(){
        String queryString = "match ($x, $y) isa binary; $x isa anotherNoRoleEntity; get;";
        assertThat(tx.execute(Graql.parse(queryString).asGet()), empty());
    }

    @Test
    public void whenQueryingForIllegalResource_emptyResultReturned(){
        String queryString = "match $x has name $n; $x isa binary; get;";
        assertThat(tx.execute(Graql.parse(queryString).asGet()), empty());
    }

    @Test
    public void whenQueryingForInexistentConceptId_emptyResultReturned(){
        String queryString = "match $x id V1337; $y id V456; ($x, $y); get;";
        assertThat(tx.execute(Graql.parse(queryString).asGet()), empty());
    }

    @Test
    public void whenQueryingForInexistentEntityTypeId_emptyResultReturned(){
        String queryString = "match $x isa $type; $type id V1337; get;";
        assertThat(tx.execute(Graql.parse(queryString).asGet()), empty());
    }

    @Test
    public void whenQueryingForInexistentRelationTypeId_emptyResultReturned(){
        String queryString = "match ($x, $y) isa $type; $type id V1337; get;";
        String queryString2 = "match $r ($x, $y) isa $type; $r id V1337; get;";
        assertThat(tx.execute(Graql.parse(queryString).asGet()), empty());
        assertThat(tx.execute(Graql.parse(queryString2).asGet()), empty());
    }

    @Test
    public void whenQueryingForInexistentResourceId_emptyResultReturned(){
        String queryString = "match $x has name $y; $x id V1337; get;";
        String queryString2 = "match $x has name $y; $y id V1337; get;";
        String queryString3 = "match $x has name $y via $r; $r id V1337; get;";
        assertThat(tx.execute(Graql.parse(queryString).asGet()), empty());
        assertThat(tx.execute(Graql.parse(queryString2).asGet()), empty());
        assertThat(tx.execute(Graql.parse(queryString3).asGet()), empty());
    }

    @Test (expected = GraqlSemanticException.class)
    public void whenQueryingForInexistentRelationTypeLabelViaVariable_emptyResultReturned() throws GraqlSemanticException {
        String queryString = "match ($x, $y) isa $type; $type type jakas-relacja; get;";
        tx.execute(Graql.parse(queryString).asGet());
    }

    @Test (expected = GraqlSemanticException.class)
    public void whenQueryingForInexistentEntityTypeLabelViaVariable_Throws() throws GraqlSemanticException {
        String queryString = "match $x isa $type; $type type polok; get;";
        tx.execute(Graql.parse(queryString).asGet());
    }

    @Test (expected = GraqlSemanticException.class)
    public void whenQueryingForInexistentEntityTypeLabel_Throws() throws GraqlSemanticException {
        String queryString = "match $x isa polok; get;";
        tx.execute(Graql.parse(queryString).asGet());
    }

    @Test (expected = GraqlSemanticException.class)
    public void whenQueryingForMismatchedResourceTypeLabel_Throws() throws GraqlSemanticException {
        String queryString = "match $x has binary $r; get;";
        tx.execute(Graql.parse(queryString).asGet());
    }

    @Test (expected = GraqlSemanticException.class)
    public void whenQueryingForInexistentRelationTypeLabel_Throws() throws GraqlSemanticException {
        String queryString = "match ($x, $y) isa jakas-relacja; get;";
        tx.execute(Graql.parse(queryString).asGet());
    }

    @Test (expected = GraqlSemanticException.class)
    public void whenQueryingForMismatchedRelationTypeLabel_Throws() throws GraqlSemanticException {
        String queryString = "match ($x, $y) isa name; get;";
        tx.execute(Graql.parse(queryString).asGet());
    }

    @Test (expected = GraqlSemanticException.class)
    public void whenQueryingForRelationWithNonExistentRoles_Throws() throws GraqlSemanticException {
        String queryString = "match (rola: $x, rola: $y) isa relation; get;";
        tx.execute(Graql.parse(queryString).asGet());
    }

    // this should be caught at the parser level
    @Test (expected = GraqlSemanticException.class)
    public void whenQueryingForRelationWithNonRoleRoles_Throws() throws GraqlException {
        String queryString = "match (entity: $x, entity: $y) isa relation; get;";
        tx.execute(Graql.parse(queryString).asGet());
    }
}
