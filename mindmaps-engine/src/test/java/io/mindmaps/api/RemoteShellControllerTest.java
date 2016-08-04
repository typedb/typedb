package io.mindmaps.api;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.util.ConfigProperties;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static com.jayway.restassured.RestAssured.get;
import static org.junit.Assert.assertTrue;

public class RemoteShellControllerTest {

    Properties prop = new Properties();
    String graphName;


    @Before
    public void setUp() throws Exception {
        new RemoteShellController();
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);
        try {
            prop.load(VisualiserControllerTest.class.getClassLoader().getResourceAsStream(ConfigProperties.CONFIG_TEST_FILE));
        } catch (Exception e) {
            e.printStackTrace();
        }
        graphName = prop.getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        MindmapsGraph graph = GraphFactory.getInstance().getGraph(graphName);
        MindmapsTransaction transaction = graph.newTransaction();
        EntityType man = transaction.putEntityType("Man");
        transaction.putEntity("actor-123", man).setValue("Al Pacino");
        transaction.commit();
        RestAssured.baseURI = prop.getProperty("server.url");
    }

    @Test
    public void notExistingID() {
        Response response = get("/match?graphName=" + graphName + "&query=match $x isa Man").then().statusCode(200).extract().response().andReturn();
        String message = response.getBody().asString();
        assertTrue(message.contains("Al Pacino"));
    }

}
