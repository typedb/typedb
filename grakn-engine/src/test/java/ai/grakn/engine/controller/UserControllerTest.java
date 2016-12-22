package ai.grakn.engine.controller;

import ai.grakn.engine.GraknEngineTestBase;
import ai.grakn.engine.user.UsersHandler;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Test;
import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;

public class UserControllerTest extends GraknEngineTestBase {

    @Test
    public void testAddAndGetNewUser(){
        String userName = "geralt";
        String password = "witcher";

        //Add New User
        Json body = Json.object(UsersHandler.USER_NAME, userName,
                UsersHandler.USER_PASSWORD, password);

        given().contentType("application/json").body(body.toString()).when().
                post("/user/one").then().statusCode(200).
                extract().response().andReturn();

        //Get Him Back
        Response dataResponse =
                get("/user/one?" + UsersHandler.USER_NAME + "=" + userName).then().
                statusCode(200).extract().response().andReturn();

        assertTrue(dataResponse.getBody().asString().contains(userName));
        assertTrue(dataResponse.getBody().asString().contains(password));
    }
}
