package ai.grakn.graph;

import ai.grakn.graph.abc.ConceptPropertyTest;
import ai.grakn.graph.abc.EntityTypePropertyTest;
import ai.grakn.graph.abc.GraknGraphPropertyTest;
import ai.grakn.graph.internal.CastingTest;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import static junit.framework.TestCase.assertTrue;

public class SampleTest {
    @Test
    public void test(){
        JUnitCore junit = new JUnitCore();
        Result result = junit.run(ConceptPropertyTest.class, EntityTypePropertyTest.class, GraknGraphPropertyTest.class, CastingTest.class);
        assertTrue(result.wasSuccessful());
    }
}
