package ai.grakn.engine.controller;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.EngineTestHelper;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.user.UsersHandler;
import ai.grakn.engine.util.JWTHandler;
import ai.grakn.util.SimpleURI;
import ai.grakn.util.MockRedisRule;
import com.google.common.collect.Iterables;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 
 * @author borislav
 *
 */
public class AuthControllerTest  {

    @ClassRule
    public static MockRedisRule mockRedisRule = MockRedisRule.create(new SimpleURI(Iterables.getOnlyElement(EngineTestHelper.config().getProperty(GraknConfigKey.REDIS_HOST))).getPort());

    @ClassRule
    public static ControllerFixture fixture = ControllerFixture.INSTANCE;
    
    private static final JWTHandler jwtHandler = JWTHandler.create("secret token");

    private UsersHandler usersHandler = UsersHandler.create(
            EngineTestHelper.config().getProperty(GraknConfigKey.ADMIN_PASSWORD),
                                                  EngineGraknTxFactory.createAndLoadSystemSchema(EngineTestHelper.config().getProperties()));

    @Test
    public void newSessionWithNonExistingUser() {
        Json body = Json.object("username", "navarro", "password", "ciaone");

        Response dataResponse = given().body(body.toString()).post("/auth/session/");
        dataResponse.then().statusCode(401);
    }

    @Ignore
    @Test
    public void newSessionWithWrongUser() {
        usersHandler.addUser(Json.object(UsersHandler.USER_NAME, "marco",
        											   UsersHandler.USER_PASSWORD, "ciao",
        											   UsersHandler.USER_IS_ADMIN, true));

        Json body = Json.object("username", "mark", "password", "ciao");

        Response dataResponseWrongUser = given().body(body.toString()).post("/auth/session/");
        dataResponseWrongUser.then().statusCode(401);
    }

    @Ignore
    @Test
    public void newSessionWithWrongPassword() {
        usersHandler.addUser(Json.object(UsersHandler.USER_NAME, "marco",
        											   UsersHandler.USER_PASSWORD, "ciao",
        											   UsersHandler.USER_IS_ADMIN, true));

        Json body = Json.object("username", "marco", "password", "hello");

        Response dataResponseWrongPass = given().body(body.toString()).post("/auth/session/");
        dataResponseWrongPass.then().statusCode(401);
    }

    @Ignore
    @Test
    public void newSessionWithExistingUser() {
        //Add a user
        usersHandler.addUser(Json.object(UsersHandler.USER_NAME, "giulio",
				   UsersHandler.USER_PASSWORD, "ciao",
				   UsersHandler.USER_IS_ADMIN, true));

        Json body = Json.object("username", "giulio", "password", "ciao");

        //Ask for a new Token
        Response dataResponse = given().body(body.toString()).post("/auth/session/");

        dataResponse.then().assertThat().statusCode(200);
        String token = dataResponse.asString();
        assertTrue(jwtHandler.verifyJWT(token));
        assertEquals("giulio", jwtHandler.extractUserFromJWT(dataResponse.asString()));

        //Try to execute query WRONG token in request
        Response dataResponseNonAuthenticated = given().
                header("Authorization", "Bearer aaaaaaaaaa.bbbbbbbbbbb.cccccccccccc").
                body(body.toString()).when().
                get("/graph/schema");
        dataResponseNonAuthenticated.then().statusCode(401);

        //Try to execute query with token in request
        Response dataResponseAuthenticated = given().
                header("Authorization", "Bearer " + token).
                body(body.toString()).get("/graph/schema");
        dataResponseAuthenticated.then().statusCode(200);

    }

    @Ignore
    @Test
    public void requestWithoutToken(){
        Json body = Json.object("username", "giulio", "password", "ciao");


        //Try to execute query without token in request, malformed request
        Response dataResponseMalformed = given().
                body(body.toString()).get("/graph/schema");
        dataResponseMalformed.then().statusCode(400);
    }

}
