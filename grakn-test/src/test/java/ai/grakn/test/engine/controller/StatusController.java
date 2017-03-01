/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
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

import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.test.EngineContext;
import ai.grakn.util.REST;
import com.jayway.restassured.response.Response;
import mjson.Json;
import org.junit.ClassRule;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.with;
import static junit.framework.TestCase.assertFalse;

public class StatusController {
    @ClassRule
    public static final EngineContext engine = EngineContext.startMultiQueueServer();

    @Test
    public void testNoJWTSecretInConfig() {
        Response response = with()
                .get(REST.WebPath.GET_STATUS_CONFIG_URI)
                .then().statusCode(200).extract().response().andReturn();

        Json resultObject = Json.read(response.getBody().asString());

        assertFalse(resultObject.has(ConfigProperties.JWT_SECRET_PROPERTY));
    }
}
