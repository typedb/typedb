package io.mindmaps.graql.api.query;

import org.junit.Test;

import static org.junit.Assert.*;

public class PatternTest {

    @Test
    public void testVarPattern() {
        Pattern x = QueryBuilder.var("x");

        assertTrue(x.admin().isVar());
        assertFalse(x.admin().isDisjunction());
        assertFalse(x.admin().isConjunction());

        assertEquals(x.admin(), x.admin().asVar());
    }

    @Test
    public void testDisjunction() {
        Pattern disjunction = QueryBuilder.or();

        assertFalse(disjunction.admin().isVar());
        assertTrue(disjunction.admin().isDisjunction());
        assertFalse(disjunction.admin().isConjunction());

        //noinspection AssertEqualsBetweenInconvertibleTypes
        assertEquals(disjunction.admin(), disjunction.admin().asDisjunction());
    }

    @Test
    public void testConjunction() {
        Pattern conjunction = QueryBuilder.and();

        assertFalse(conjunction.admin().isVar());
        assertFalse(conjunction.admin().isDisjunction());
        assertTrue(conjunction.admin().isConjunction());

        //noinspection AssertEqualsBetweenInconvertibleTypes
        assertEquals(conjunction.admin(), conjunction.admin().asConjunction());
    }
}
