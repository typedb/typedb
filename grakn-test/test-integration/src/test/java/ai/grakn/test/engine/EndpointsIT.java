/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.test.engine;

import ai.grakn.test.rule.DistributionContext;
import com.jayway.restassured.http.ContentType;
import org.junit.ClassRule;
import org.junit.Test;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.RestAssured.when;
import static org.hamcrest.Matchers.containsString;

/**
 * @author Felix Chapman
 */
public class EndpointsIT {

    @ClassRule
    public static DistributionContext dist = DistributionContext.create();

    @Test
    public void whenGettingRootWithoutAcceptType_ReturnHTML() {
        when().get("/")
                .then().contentType(containsString(ContentType.HTML.toString())).body(containsString("<html>"));
    }

    @Test
    public void whenGettingRootWithJsonAcceptType_ReturnJson() {
        given().accept(ContentType.JSON).when().get("/")
                .then().contentType(containsString(ContentType.JSON.toString()));
    }
}
