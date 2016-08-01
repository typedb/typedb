package io.mindmaps.api;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.util.ConfigProperties;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static com.jayway.restassured.RestAssured.get;
import static org.junit.Assert.assertTrue;

public class VisualiserControllerTest {

    Properties prop = new Properties();


    @Before
    public void setUp() throws Exception {


        new VisualiserController();
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.INFO);
        try {
            prop.load(VisualiserControllerTest.class.getClassLoader().getResourceAsStream(ConfigProperties.CONFIG_TEST_FILE));
        } catch (Exception e) {
            e.printStackTrace();
        }
        MindmapsGraph graph = GraphFactory.getInstance().getGraph(prop.getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY));
        MindmapsTransaction mindmapsGraph = graph.newTransaction();
//        RoleType prodCast = mindmapsGraph.putRoleType("Production with Cast");
//        RoleType prodRole = mindmapsGraph.putRoleType("Role within the production");
//        RoleType actor = mindmapsGraph.putRoleType("Actor");
//
//        RelationType casting = mindmapsGraph.putRelationType("Casting").
//                hasRole(prodCast).hasRole(prodRole).hasRole(actor);
//
//        EntityType production = mindmapsGraph.putEntityType("Production").
//                playsRole(prodCast);
//        EntityType tvShow = mindmapsGraph.putEntityType("Tv Show").superConcept(production);
//        EntityType movie = mindmapsGraph.putEntityType("Movie").superConcept(production);
//
//        Type character = mindmapsGraph.putType("Character").
//                playsRole(prodRole);
//
//        EntityType person = mindmapsGraph.putEntityType("Person").
//                playsRole(actor);
//        EntityType man = mindmapsGraph.putEntityType("Man").
//                supertype(person);
//        EntityType woman = mindmapsGraph.putEntityType("Man").
//                superType(person);
        RestAssured.baseURI = prop.getProperty("server.url");
    }

    @Test
    public void test() {
        Response response = get("/concept/6573gehjio").then().statusCode(404).extract().response().andReturn();
        String  message = response.getBody().asString();
        assertTrue(message.equals("ID [6573gehjio] not found in the graph."));
    }
}
