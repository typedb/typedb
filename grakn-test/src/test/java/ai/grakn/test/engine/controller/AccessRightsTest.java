package ai.grakn.test.engine.controller;

import static com.jayway.restassured.RestAssured.given;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.engine.user.UsersHandler;
import ai.grakn.test.EngineContext;
import mjson.Json;

public class AccessRightsTest {
    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Before
    public void addSomeUsersAndGraphs() {
        for (String user : new String[] { "user1", "user2", "user3" } ) { 
            given().contentType("application/json").body(Json.object(UsersHandler.USER_NAME, user).toString()).when().
                post("/user/one").then().statusCode(200).
                extract().response().andReturn();
        }
        for (String keyspace : new String [] { "keyspace1", "keyspace2", "keyspace3" } ) {
            GraknGraph graph = EngineGraknGraphFactory.getInstance().getGraph(keyspace, GraknTxType.WRITE);
            graph.close();
        }
    }    
    
    @Test
    public void testAddReadAccess() {
        given().contentType("application/json").when().
            put("/user/one/user1/grant/keyspace1/right").then().statusCode(200).
                    extract().response().andReturn();
        
    }
}
