/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.test.property.graql;

import ai.grakn.GraknTx;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.generator.GraknTxs.Open;
import ai.grakn.graql.Graql;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.pattern.property.PlaysProperty;
import ai.grakn.graql.internal.pattern.property.RelatesProperty;
import ai.grakn.graql.internal.pattern.property.SubProperty;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Set;

import static org.junit.Assume.assumeFalse;

@RunWith(JUnitQuickcheck.class)
public class DefineQueryPropertyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Ignore("Currently no error message is returned when trying to insert an empty set of propoerties. I am not entirely sure this is correct")
    @Property
    public void aDefineQueryWithoutASubOrPlaysOrRelatesProperty_CannotBeInserted(@Open GraknTx tx, @Size(max=5) Set<VarProperty> properties){
        boolean containsSub = properties.stream().anyMatch(SubProperty.class::isInstance);
        boolean containsPlays = properties.stream().anyMatch(PlaysProperty.class::isInstance);
        boolean containsRelates = properties.stream().anyMatch(RelatesProperty.class::isInstance);
        assumeFalse(containsSub || containsPlays || containsRelates);

        VarPatternAdmin pattern = Patterns.varPattern(Graql.var("x"), properties);

        exception.expect(GraqlQueryException.class);

        tx.graql().define(pattern).execute();
    }
}