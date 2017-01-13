package ai.grakn.engine.controller;

import ai.grakn.engine.user.UsersHandler;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.util.REST;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;

@Path("/user")
@Produces({"application/json", "text/plain"})
public class UserController {
    private final Logger LOG = LoggerFactory.getLogger(UserController.class);
	private final UsersHandler users = UsersHandler.getInstance();
	
	public UserController() {
        get(REST.WebPath.ALL_USERS, this::findUsers);
        get(REST.WebPath.ONE_USER, this::getUser);
        post(REST.WebPath.ONE_USER, this::createUser);
        delete(REST.WebPath.ONE_USER, this::removeUser);
        put(REST.WebPath.ONE_USER, this::updateUser);
	}
	
    @GET
    @Path("/all")
    @ApiOperation(value = "Get users.")
    @ApiImplicitParams({
        @ApiImplicitParam(name = "limit", value = "Limit the number of users returned.", dataType = "int", paramType = "query"),
        @ApiImplicitParam(name = "offset", value = "Start results from the given offset.", dataType = "int", paramType = "query")
    })
    private Json findUsers(Request request, Response response) {
    	int limit, offset;
    	try {
    		limit = !request.params().containsKey("limit") ? Integer.MAX_VALUE : Integer.parseInt(request.params().get("limit"));
    		offset = !request.params().containsKey("offset") ? Integer.MAX_VALUE : Integer.parseInt(request.params().get("offset"));
        	return users.allUsers(offset, limit);
    	}
    	catch (NumberFormatException ex) {
    		response.status(400);
    		return Json.nil();
    	}
    }
    
    @GET
    @Path("/one/:user-name")
    @ApiOperation(value = "Get one user.")
    @ApiImplicitParam(name = "user-name", value = "Username of user.", required = true, dataType = "string", paramType = "path")
    private Json getUser(Request request, Response response) {
    	return users.getUser(request.queryParams(UsersHandler.USER_NAME));
    }
    
    @POST
    @Path("/one")
    @ApiOperation(value = "Create a new user.")
    @ApiImplicitParam(name = "user", value = "A JSON object representing the new user, with a unique username and a valid password.", dataType = "String", paramType = "body")
    private boolean createUser(Request request, Response response) {
        try {
            Json user = Json.read(request.body());
            if (users.userExists(user.at(UsersHandler.USER_NAME).asString()))
                return false;
            users.addUser(user);
            return true;
        } catch(Exception e){
            LOG.error("Error during creating new user", e);
            throw new GraknEngineServerException(500,e);
        }
    }
    
    @GET
    @Path("/one/:user-name")
    @ApiOperation(value = "Delete a user.")
    @ApiImplicitParam(name = "user-name", value = "Username of user.", required = true, dataType = "string", paramType = "path")
    private boolean removeUser(Request request, Response response) {
        return users.removeUser(request.queryParams(UsersHandler.USER_NAME));
    }
    
    @PUT
    @Path("/one")
    @ApiOperation(value = "Update an existing user.")
    @ApiImplicitParam(name = "user", value = "A JSON object representing the user.", dataType = "String", paramType = "body")
    private boolean updateUser(Request request, Response response) {
    	Json user = Json.read(request.body());    	
    	if (!users.userExists(user.at(UsersHandler.USER_NAME).asString()))
    		return false;
    	return users.updateUser(user);
    }
}