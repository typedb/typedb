package ai.grakn.test.engine.controller;

import ai.grakn.engine.controller.AuthController;
import ai.grakn.engine.user.UsersHandler;
import ai.grakn.engine.util.JWTHandler;
import ai.grakn.test.GraphContext;
import ai.grakn.test.SparkContext;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AuthControllerTest{

    private static final JWTHandler jwtHandler = JWTHandler.create("secret token");

    private UsersHandler usersHandler;

    @ClassRule
    public static final GraphContext graph = GraphContext.empty();

    @Rule
    public final SparkContext ctx = SparkContext.withControllers(spark -> {
        usersHandler = UsersHandler.create("top secret", graph.factory());
        new AuthController(spark, true, jwtHandler, usersHandler);
    });

    // TODO: Un-ignore these tests now that the config is not always a singleton
    //Ignoring a couple of randomly failing tests. I will probably need to create a new config file with password protection enabled.
    //Or maybe find alternative to singleton.
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

    @Ignore
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
