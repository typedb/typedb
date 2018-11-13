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

package grakn.core;

import grakn.core.exception.TransactionException;
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
        expectedException.expect(TransactionException.class);
        expectedException.expectMessage(TransactionException.invalidKeyspace(keyspace).getMessage());
        Keyspace.of(keyspace);
    }

}
