/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2017  Grakn Labs Ltd
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.engine.controller;

import ai.grakn.engine.user.UsersHandler;
import ai.grakn.test.EngineContext;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.ClassRule;
import org.junit.Test;
import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertTrue;

public class UserControllerTest {
    private UsersHandler users = UsersHandler.getInstance();

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

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
    }
}
