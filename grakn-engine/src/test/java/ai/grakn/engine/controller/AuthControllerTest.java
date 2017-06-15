package ai.grakn.engine.controller;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;

import com.jayway.restassured.response.Response;

import ai.grakn.engine.EngineTestHelper;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.engine.user.UsersHandler;
import ai.grakn.engine.util.JWTHandler;
import mjson.Json;

/**
 * 
 * @author borislav
 *
 */
public class AuthControllerTest extends ControllerTestBase {

    private static final JWTHandler jwtHandler = JWTHandler.create("secret token");

    private UsersHandler usersHandler = UsersHandler.create(
            EngineTestHelper.config().getProperty(GraknEngineConfig.ADMIN_PASSWORD_PROPERTY), 
                                                  EngineGraknGraphFactory.create(EngineTestHelper.config().getProperties()));


    @Test
    public void newSessionWithNonExistingUser() {
        Json body = Json.object("username", "navarro", "password", "ciaone");

        Response dataResponse = given().
                contentType("application/json").
                body(body.toString()).when().
                post("/auth/session/");

        dataResponse.then().assertThat().statusCode(401);
    }

    @Test
    public void newSessionWithWrongUser() {
        usersHandler.addUser(Json.object(UsersHandler.USER_NAME, "marco",
        											   UsersHandler.USER_PASSWORD, "ciao",
        											   UsersHandler.USER_IS_ADMIN, true));

        Json body = Json.object("username", "mark", "password", "ciao");

        Response dataResponseWrongUser = given().
                contentType("application/json").
                body(body.toString()).when().
                post("/auth/session/");
        dataResponseWrongUser.then().assertThat().statusCode(401);
    }

    @Test
    public void newSessionWithWrongPassword() {
        usersHandler.addUser(Json.object(UsersHandler.USER_NAME, "marco",
        											   UsersHandler.USER_PASSWORD, "ciao",
        											   UsersHandler.USER_IS_ADMIN, true));

        Json body = Json.object("username", "marco", "password", "hello");

        Response dataResponseWrongPass = given().
                contentType("application/json").
                body(body.toString()).when().
                post("/auth/session/");
        dataResponseWrongPass.then().assertThat().statusCode(401);
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
        Response dataResponse = given().
                        contentType("application/json").
                        body(body.toString()).when().
                        post("/auth/session/");

        dataResponse.then().assertThat().statusCode(200);
        String token = dataResponse.asString();
        assertTrue(jwtHandler.verifyJWT(token));
        assertEquals("giulio", jwtHandler.extractUserFromJWT(dataResponse.asString()));

        //Try to execute query WRONG token in request
        Response dataResponseNonAuthenticated = given().
                header("Authorization", "Bearer aaaaaaaaaa.bbbbbbbbbbb.cccccccccccc").
                contentType("application/json").
                body(body.toString()).when().
                get("/graph/ontology");
        dataResponseNonAuthenticated.then().assertThat().statusCode(401);

        //Try to execute query with token in request
        Response dataResponseAuthenticated = given().
                header("Authorization", "Bearer " + token).
                contentType("application/json").
                body(body.toString()).when().
                get("/graph/ontology");
        dataResponseAuthenticated.then().assertThat().statusCode(200);

    }

    @Test
    public void requestWithoutToken(){
        Json body = Json.object("username", "giulio", "password", "ciao");


        //Try to execute query without token in request, malformed request
        Response dataResponseMalformed = given().
                contentType("application/json").
                body(body.toString()).when().
                get("/graph/ontology");
        dataResponseMalformed.then().assertThat().statusCode(400);
    }

}
