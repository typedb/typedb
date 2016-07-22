/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.core.implementation;

import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.junit.Assert.assertEquals;

public class DataTypeTest {
    @Test(expected = InvocationTargetException.class)
    public void testConstructor() throws Exception { //Checks that you cannot initialise it.
        Constructor<DataType> c = DataType.class.getDeclaredConstructor();
        c.setAccessible(true);
        DataType u = c.newInstance();
    }

    @Test
    public void enumParingAndValueTest(){
        DataType.EdgeLabel isa = DataType.EdgeLabel.valueOf("ISA");
        DataType.EdgeLabel ako = DataType.EdgeLabel.valueOf("AKO");
        DataType.EdgeLabel has_role = DataType.EdgeLabel.valueOf("HAS_ROLE");
        DataType.EdgeLabel has_scope = DataType.EdgeLabel.valueOf("HAS_SCOPE");
        DataType.EdgeLabel casting = DataType.EdgeLabel.valueOf("CASTING");

        DataType.ConceptMeta role_type = DataType.ConceptMeta.valueOf("ROLE_TYPE");

        // Basic Edges
        assertEquals(DataType.EdgeLabel.ISA, isa);
        assertEquals(DataType.EdgeLabel.AKO, ako);
        assertEquals(DataType.EdgeLabel.HAS_ROLE, has_role);
        assertEquals(DataType.EdgeLabel.HAS_SCOPE, has_scope);
        // Other
        assertEquals(DataType.EdgeLabel.CASTING, casting);

        //Internal Vertex
        assertEquals(DataType.ConceptMeta.ROLE_TYPE, role_type);

    }
}
