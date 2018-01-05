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

package ai.grakn;

import ai.grakn.exception.GraknTxOperationException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class KeyspaceTest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void whenCreatingKeyspaceWithUpperCaseLetters_Throw(){
        invalidKeyspace("bOb");
    }

    @Test
    public void whenCreatingKeyspaceStartingWithNumbers_Throw(){
        invalidKeyspace("123hello");
    }

    @Test
    public void whenCreatingKeyspaceWithNonAlphaNumberis_Throw(){
        invalidKeyspace("mynameis@£$%^&");
    }

    @Test
    public void whenCreatingKeyspaceLongerThan48Characters_Throw(){
        invalidKeyspace("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa49");
    }

    private void invalidKeyspace(String keyspace) {
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.invalidKeyspace(keyspace).getMessage());
        Keyspace.of(keyspace);
    }

}
