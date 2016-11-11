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

package ai.grakn.engine.controller;

import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.util.REST;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.json.JSONObject;
import spark.Request;
import spark.Response;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.util.Enumeration;
import java.util.Properties;

import static spark.Spark.get;


@Path("/status")
@Api(value = "/status", description = "Endpoints used to retrieve all the info about the current instance of Grakn")
@Produces({"application/json"})
public class StatusController {

    public StatusController() {
        get(REST.WebPath.GET_STATUS_CONFIG_URI, this::getStatus);
    }

    @GET
    @Path("/config")
    @ApiOperation(
            value = "Return config file as a JSONObject.")
    private String getStatus(Request req, Response res) {

        JSONObject configObj = new JSONObject();
        Properties props = ConfigProperties.getInstance().getProperties();
        Enumeration e = props.propertyNames();

        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            configObj.put(key,props.getProperty(key));
        }

        return configObj.toString();
    }

}
