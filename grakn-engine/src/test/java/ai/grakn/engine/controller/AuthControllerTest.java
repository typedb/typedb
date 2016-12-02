package ai.grakn.engine.controller;

import ai.grakn.engine.GraknEngineServer;
import ai.grakn.engine.GraknEngineTestBase;
import ai.grakn.engine.user.UsersHandler;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.engine.util.JWTHandler;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Properties;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AuthControllerTest {

    @BeforeClass
    public static void setupControllers() throws InterruptedException {
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY, ConfigProperties.TEST_CONFIG_FILE);
        ConfigProperties.getInstance().setConfigProperty(ConfigProperties.PASSWORD_PROTECTED_PROPERTY,"true");

        Properties prop = ConfigProperties.getInstance().getProperties();
        RestAssured.baseURI = "http://" + prop.getProperty("server.host") + ":" + prop.getProperty("server.port");
        GraknEngineServer.start();
        Thread.sleep(5000);
    }

    @AfterClass
    public static void takeDownControllers() throws InterruptedException {
        GraknEngineServer.stop();
        Thread.sleep(10000);
    }


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
    public void newSessionWithWrongPasswordAndUser() {
        UsersHandler.getInstance().addUser(Json.object(UsersHandler.USER_NAME, "marco", 
        											   UsersHandler.USER_PASSWORD, "ciao", 
        											   UsersHandler.USER_IS_ADMIN, true));

        Json body = Json.object("username", "marco", "password", "hello");

        Response dataResponseWrongPass = given().
                contentType("application/json").
                body(body.toString()).when().
                post("/auth/session/");
        dataResponseWrongPass.then().assertThat().statusCode(401);

        Response dataResponseWrongUser = given().
                contentType("application/json").
                body(body.toString()).when().
                post("/auth/session/");
        dataResponseWrongUser.then().assertThat().statusCode(401);
    }


    @Test
    public void newSessionWithExistingUser() {
        //Add a user
        UsersHandler.getInstance().addUser(Json.object(UsersHandler.USER_NAME, "giulio", 
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
        assertTrue(JWTHandler.verifyJWT(token));
        assertEquals("giulio", JWTHandler.extractUserFromJWT(dataResponse.asString()));

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
        //find a way to change password.protected config in config file

        Json body = Json.object("username", "giulio", "password", "ciao");


        //Try to execute query without token in request, malformed request
        Response dataResponseMalformed = given().
                contentType("application/json").
                body(body.toString()).when().
                get("/graph/ontology");
        dataResponseMalformed.then().assertThat().statusCode(400);
    }

}