package ai.grakn.test.graql.analytics;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.test.AbstractScalingTest;
import org.junit.Test;

/**
 *
 */
public class DebugAnalyticsTestIT extends AbstractScalingTest {

    @Test
    public void testSlowMethod() {
        GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, ConfigProperties.SYSTEM_GRAPH_NAME).getGraph();
        System.out.println(graph.graql().compute().count().execute());
    }
}
