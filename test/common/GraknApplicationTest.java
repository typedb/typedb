import grakn.client.GraknClient;
import grakn.core.concept.answer.ConceptMap;
import graql.lang.Graql;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class GraknApplicationTest {
    private GraknClient graknClient;

    @Before
    public void before() {
        String host = "localhost:48555";
        graknClient = new GraknClient(host);
    }

    @After
    public void after() {
        graknClient.close();
    }

    @Test
    public void testDeployment() {
        try (GraknClient.Session session = graknClient.session("grakn")) {
            try (GraknClient.Transaction tx = session.transaction().write()) {
                List<ConceptMap> result = tx.execute(Graql.match(Graql.var("t").sub("thing")).get());
                assertTrue(result.size() > 0);
            }
        }
    }
}