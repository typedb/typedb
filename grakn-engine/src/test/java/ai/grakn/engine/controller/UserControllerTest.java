package ai.grakn.engine.controller;

import ai.grakn.engine.GraknEngineTestBase;
import ai.grakn.engine.user.UsersHandler;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Before;
import org.junit.Test;
import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;

public class UserControllerTest extends GraknEngineTestBase {
    private UsersHandler users;

    @Before
    public void setup(){
        users = UsersHandler.getInstance();
    }

    @Test
    public void testAddNewUser(){
        String userName = "person";
        String password = "password";

        //Add New User
        Json body = Json.object(UsersHandler.USER_NAME, userName, UsersHandler.USER_PASSWORD, password);

        given().contentType("application/json").body(body.toString()).when().
                post("/user/one").then().statusCode(200).
                extract().response().andReturn();

        //Check he is there
        assertTrue(users.getUser(userName).is(UsersHandler.USER_NAME, userName));
        assertTrue(users.getUser(userName).is(UsersHandler.USER_PASSWORD, password));
    }

    @Test
    public void testGetUser(){
        String userName = "bob";
        String password = "smith";

        //Add User
        Json user = Json.object(UsersHandler.USER_NAME, userName, UsersHandler.USER_PASSWORD, password);
        users.addUser(user);

        //Get Him Back
        Response dataResponse =
                get("/user/one?" + UsersHandler.USER_NAME + "=" + userName).then().
                        statusCode(200).extract().response().andReturn();

        assertTrue(dataResponse.getBody().asString().contains(userName));
        assertTrue(dataResponse.getBody().asString().contains(password));
    }
}
