/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.engine.controller;

import io.mindmaps.constants.RESTUtil;
import io.mindmaps.engine.util.ConfigProperties;
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
@Api(value = "/status", description = "Endpoints used to retrieve all the info about the current instance of MindmapsDB")
@Produces({"application/json"})
public class StatusController {

    public StatusController() {

        get(RESTUtil.WebPath.GET_STATUS_URI, this::getStatus);
    }

    @GET
    @Path("/")
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
