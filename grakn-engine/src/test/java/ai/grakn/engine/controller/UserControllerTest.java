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

package ai.grakn.engine.controller;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.EngineTestHelper;
import ai.grakn.engine.user.UsersHandler;
import ai.grakn.util.SimpleURI;
import ai.grakn.util.MockRedisRule;
import com.google.common.collect.Iterables;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.get;
import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.RestAssured.when;

public class UserControllerTest  {

    @ClassRule
    public static MockRedisRule mockRedisRule = MockRedisRule.create(new SimpleURI(Iterables.getOnlyElement(EngineTestHelper.config().getProperty(GraknConfigKey.REDIS_HOST))).getPort());

    @ClassRule
    public static ControllerFixture fixture = ControllerFixture.INSTANCE;

    @Test
    public void testAddAndGetNewUser() {        
        String userName = "person";
        String password = "password";

        fixture.cleanup(() -> when().delete("/user/one/" + userName).then().statusCode(200));

        //Add New User
        Json body = Json.object(UsersHandler.USER_NAME, userName, UsersHandler.USER_PASSWORD, password);
        Response resp = given().body(body.toString()).post("/user/one");
        resp.then().statusCode(200); // theoretically, the correct response should be 201 as per HTTP spec, but few do it this way...

        //Check he is there
        Assert.assertEquals(userName, get("/user/one/" + userName).jsonPath().getString("user-name"));
    }
    
    @Test
    public void testMissingUserLookup() {
        get("/user/one/nobody").then().statusCode(404);
    }
}
