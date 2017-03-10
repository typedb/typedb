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

import ai.grakn.engine.user.UsersHandler;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.util.JWTHandler;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.util.REST;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import mjson.Json;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

/**
 * <p>
 *     Endpoints used to handle operations related to authentication.
 * </p>
 *
 * @author Marco Scoppetta
 */
@Path("/auth")
@Api(value = "/graph", description = "Endpoints used to handle operations related to authentication.")
@Produces({"application/json", "text/plain"})
public class AuthController {

    private final static UsersHandler usersHandler = UsersHandler.getInstance();
    private final static String USERNAME_KEY = "username";
    private final static String PASSWORD_KEY = "password";


    public AuthController(Service spark) {
        spark.post(REST.WebPath.NEW_SESSION_URI, this::newSession);
        spark.get(REST.WebPath.IS_PASSWORD_PROTECTED_URI,this::isPasswordProtected);
    }

    @POST
    @Path("/session")
    @ApiOperation(
            value = "If a given user/password pair is valid, returns a new JWT")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "username", value = "Username", required = true, dataType = "string", paramType = "body"),
            @ApiImplicitParam(name = "password", value = "Password", required = true, dataType = "string", paramType = "body")
    })
    private String newSession(Request req, Response res) {
        String user;
        String password;
        try {
            Json jsonBody = Json.read(req.body());
            user = jsonBody.at(USERNAME_KEY).asString();
            password = jsonBody.at(PASSWORD_KEY).asString();
        } catch (Exception e) {
            throw new GraknEngineServerException(400, e);
        }

        if (usersHandler.validateUser(user, password)) {
            return JWTHandler.signJWT(user);
        } else {
            throw new GraknEngineServerException(401,"Wrong authentication parameters have been provided.");
        }


    }

    @GET
    @Path("/enabled")
    @ApiOperation(
            value = "Returns true if Engine endpoints are password protected. False otherwise.")
    private String isPasswordProtected(Request req, Response res) {
        return GraknEngineConfig.getInstance().getProperty(GraknEngineConfig.PASSWORD_PROTECTED_PROPERTY);
    }
}
