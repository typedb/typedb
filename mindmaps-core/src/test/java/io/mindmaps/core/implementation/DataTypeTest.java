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
