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

package ai.grakn.test.property.kb;

import ai.grakn.GraknTx;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.generator.GraknTxs.Open;
import ai.grakn.graql.Graql;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assume.assumeFalse;

@RunWith(JUnitQuickcheck.class)
public class InsertQueryPropertyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Property
    public void ifAPropertyCanBeInserted_AResultShouldBeReturned(@Open GraknTx tx, VarProperty property){
        VarPatternAdmin pattern = Patterns.varPattern(Graql.var("x"), Collections.singleton(property));
        try {
            assertThat(tx.graql().insert(pattern).execute(), not(empty()));
        } catch(GraqlQueryException | GraknTxOperationException e){
            // IGNORED
        }
    }

    @Ignore("Currently no error message is returned when trying to insert an empty set of propoerties. I am not entirely sure this is correct")
    @Property
    public void anInsertQueryWithoutAnIsaProperty_CannotBeInserted(@Open GraknTx tx, @Size(max=5) Set<VarProperty> properties){
        boolean containsIsa = properties.stream().anyMatch(IsaProperty.class::isInstance);
        assumeFalse(containsIsa);

        VarPatternAdmin pattern = Patterns.varPattern(Graql.var("x"), properties);

        exception.expect(GraqlQueryException.class);

        tx.graql().insert(pattern).execute();
    }
}