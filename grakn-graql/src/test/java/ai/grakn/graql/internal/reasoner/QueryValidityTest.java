/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner;

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.GraknTestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

public class QueryValidityTest {

    @ClassRule
    public static final SampleKBContext testContext = SampleKBContext.load("ruleApplicabilityTest.gql");

    @BeforeClass
    public static void onStartup() throws Exception {
        assumeTrue(GraknTestUtil.usingTinker());
    }

    @Test
    public void whenQueryingForInexistentConceptId_emptyResultReturned(){
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match $x id 'V123'; $y id 'V456'; ($x, $y); get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }

    @Test
    public void whenQueryingForInexistentEntityTypeId_emptyResultReturned(){
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match $x isa $type; $type id 'V123'; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }

    @Test
    public void whenQueryingForInexistentRelationTypeId_emptyResultReturned(){
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match ($x, $y) isa $type; $type id 'V123'; get;";
        String queryString2 = "match $r ($x, $y) isa $type; $r id 'V123'; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
        assertThat(qb.<GetQuery>parse(queryString2).execute(), empty());
    }

    @Test
    public void whenQueryingForInexistentResourceId_emptyResultReturned(){
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match $x has name $y; $x id 'V123'; get;";
        String queryString2 = "match $x has name $y; $y id 'V123'; get;";
        String queryString3 = "match $x has name $y via $r; $r id 'V123'; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
        assertThat(qb.<GetQuery>parse(queryString2).execute(), empty());
        assertThat(qb.<GetQuery>parse(queryString3).execute(), empty());
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForInexistentEntityTypeLabel_Throws() throws GraqlQueryException{
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match $x isa polok; get;";
        qb.<GetQuery>parse(queryString).execute();
    }

    @Test
    public void whenQueryingForInexistentEntityTypeLabelViaVariable_emptyResultReturned(){
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match $x isa $type; $type label polok; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForMismatchedResourceTypeLabel_Throws() throws GraqlQueryException{
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match $x has binary $r; get;";
        qb.<GetQuery>parse(queryString).execute();
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForInexistentRelationTypeLabel_Throws() throws GraqlQueryException{
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match ($x, $y) isa jakas-relacja; get;";
        qb.<GetQuery>parse(queryString).execute();
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForMismatchedRelationTypeLabel_Throws() throws GraqlQueryException{
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match ($x, $y) isa name; get;";
        qb.<GetQuery>parse(queryString).execute();
    }

    @Test
    public void whenQueryingForInexistentRelationTypeLabelViaVariable_emptyResultReturned(){
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match ($x, $y) isa $type; $type label jakas-relacja; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForRelationWithNonRoleRoles_Throws() throws GraqlQueryException{
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match (entity: $x, entity: $y) isa relationship; get;";
        qb.<GetQuery>parse(queryString).execute();
    }

    @Test (expected = GraqlQueryException.class)
    public void whenQueryingForRelationWithNonExistentRoles_Throws() throws GraqlQueryException{
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match (rola: $x, rola: $y) isa relationship; get;";
        qb.<GetQuery>parse(queryString).execute();
    }

    @Test
    public void whenQueryingForRelationWithIllegalRoles_emptyResultReturned(){
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match (role3: $x) isa binary; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }

    @Test
    public void whenQueryingForIllegalRolePlayer_emptyResultReturned(){
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match ($x, $y) isa binary; $x isa anotherNoRoleEntity; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }

    @Test
    public void whenQueryingForIllegalResource_emptyResultReturned(){
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match $x has name $n; $x isa binary; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }
}
