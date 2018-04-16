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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.reasoner;

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.test.rule.SampleKBContext;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

public class QueryValidityTest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @ClassRule
    public static final SampleKBContext testContext = SampleKBContext.load("ruleApplicabilityTest.gql");

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

    @Test
    public void whenQueryingForInexistentEntityTypeLabel_Throws() throws GraqlQueryException{
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match $x isa polok; get;";
        expectedException.expect(GraqlQueryException.class);
        qb.<GetQuery>parse(queryString).execute();
    }

    @Test
    public void whenQueryingForInexistentEntityTypeLabelViaVariable_emptyResultReturned(){
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match $x isa $type; $type label polok; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }

    @Test
    public void whenQueryingForMismatchedResourceTypeLabel_Throws() throws GraqlQueryException{
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match $x has binary $r; get;";
        expectedException.expect(GraqlQueryException.class);
        qb.<GetQuery>parse(queryString).execute();
    }

    @Test
    public void whenQueryingForInexistentRelationTypeLabel_Throws() throws GraqlQueryException{
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match ($x, $y) isa jakas-relacja; get;";
        expectedException.expect(GraqlQueryException.class);
        qb.<GetQuery>parse(queryString).execute();
    }

    @Test
    public void whenQueryingForMismatchedRelationTypeLabel_Throws() throws GraqlQueryException{
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match ($x, $y) isa name; get;";
        expectedException.expect(GraqlQueryException.class);
        qb.<GetQuery>parse(queryString).execute();
    }

    @Test
    public void whenQueryingForInexistentRelationTypeLabelViaVariable_emptyResultReturned(){
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match ($x, $y) isa $type; $type label jakas-relacja; get;";
        assertThat(qb.<GetQuery>parse(queryString).execute(), empty());
    }

    @Test
    public void whenQueryingForRelationWithNonRoleRoles_Throws() throws GraqlQueryException{
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match (entity: $x, entity: $y) isa relationship; get;";
        expectedException.expect(GraqlQueryException.class);
        qb.<GetQuery>parse(queryString).execute();
    }

    @Test
    public void whenQueryingForRelationWithNonExistentRoles_Throws() throws GraqlQueryException{
        QueryBuilder qb = testContext.tx().graql().infer(true);
        String queryString = "match (rola: $x, rola: $y) isa relationship; get;";
        expectedException.expect(GraqlQueryException.class);
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
