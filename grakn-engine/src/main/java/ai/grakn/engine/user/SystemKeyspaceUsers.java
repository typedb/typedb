package ai.grakn.engine.user;

import static ai.grakn.graql.Graql.var;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.factory.GraphFactory;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.graql.AskQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Var;
import mjson.Json;

/**
 * A DAO for managing users in the Grakn system keyspace. System the 'system.gql' ontology
 * for how users are modeled.
 * 
 * @author borislav
 *
 */
public class SystemKeyspaceUsers extends UsersHandler {
	private final Logger LOG = LoggerFactory.getLogger(SystemKeyspaceUsers.class);
		
	/**
	 * Add a new user. To make sure a user doesn't already exist, please 
	 * 
	 * @return <code>true</code> if the new user was added successfully and <code>false</code>
	 * otherwise.
	 */
	@Override
	public boolean addUser(Json userJson) {
		final Var user = var().isa(USER_ENTITY);
		userJson.asJsonMap().forEach( (n,v) -> {
			user.has(n, v.getValue());
		});
		try (GraknGraph graph = GraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME)) {
			InsertQuery query = graph.graql().insert(user);
			query.execute();
			graph.commit();
			LOG.debug("Created user " + userJson);
			return true;
		} catch (Throwable t) {
			LOG.error("Could not add user "  + userJson + " to system graph: ", t);
			rethrow(t);
			return false;
		}
	}

	/**
	 * Return <code>true</code> if the user with the specified name exists and <code>false</code> otherwise.
	 */
	public boolean userExists(String username) {
		Var lookup = var().isa(USER_ENTITY).has(USER_NAME, username);
		try (GraknGraph graph = GraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME)) {
			AskQuery query = graph.graql().match(lookup).ask();
			return query.execute();
		}
		catch (Throwable t) {
			LOG.error("While getting all users.", t);
			rethrow(t);
			return false;
		}		
	}
	
	/**
	 * Return the user with the specified name as a JSON object where the properties have the same
	 * names as the Grakn resource types. If the user does not exist, <code>Json.nil()</code> is returned.
	 */	
	public Json getUser(String username) {
		Var lookup = var("entity").isa(USER_ENTITY).has(USER_NAME, username);
		Var resource = var("property");
		try (GraknGraph graph = GraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME)) {
			MatchQuery query = graph.graql().match(lookup.has(resource));
			List<Map<String, Concept>> L = query.execute();
			if (L.isEmpty())
				return Json.nil();
			Json user = Json.object();
			L.forEach(property -> {
				String name = property.get("property").asInstance().type().getName();
				Object value = property.get("property").asResource().getValue();
				user.set(name, value);
			});
			return user;
		}
		catch (Throwable t) {
			LOG.error("While getting all users.", t);
			rethrow(t);
			return Json.nil();
		}		
	}

	/**
	 * Retrieve the list of all users with all their properties. 
	 * 
	 * @param offset
	 * @param limit
	 * @return
	 */
	public Json allUsers(int offset, int limit) {
		Var lookup = var("entity").isa(USER_ENTITY);		
		try (GraknGraph graph = GraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME)) {
			MatchQuery query = graph.graql().match(lookup.has(USER_NAME, var("username"))).limit(limit).offset(offset);
			List<Map<String, Concept>> L = query.execute();
			Json all = Json.array();
			L.forEach(concepts -> {
				String username = concepts.get("username").asResource().getValue().toString();
				all.add(getUser(username));
			});
			return all;
		}		
		catch (Throwable t) {
			LOG.error("While getting all users.", t);
			rethrow(t);
			return Json.nil();
		}
	}
	
	/**
	 * Removes a user with the given username.
	 * 
	 * @return <code>true</code> if the user was removed successfully and <code>false</code> otherwise.
	 */
	public boolean removeUser(String username) {
		Var lookup = var("entity").isa(USER_ENTITY).has(USER_NAME, username);
		Var resource = var("property");
		try (GraknGraph graph = GraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME)) {
			MatchQuery query = graph.graql().match(lookup.has(resource));
			List<Map<String, Concept>> L = query.execute();
			boolean existing = !L.isEmpty();
			L.forEach(map -> {
				map.forEach( (k,v) -> {
					if ("entity".equals(k)) {
						v.asInstance().resources().forEach(r -> r.delete() );
						v.delete();
					}
				});
			});
			return existing;
		}		
		catch (Throwable t) {
			LOG.error("While getting all users.", t);
			rethrow(t);
			return false;
		}		
	}	

	/**
	 * Update a given user.
	 * 
	 * @return <Code>true</code> if the user was updated successfully and <code>false</code> otherwise.
	 */
	public boolean updateUser(Json user) {
		throw new UnsupportedOperationException();
	}
	
	private void rethrow(Throwable t) {
		if (t instanceof Error)
			throw (Error)t;
		else if (t instanceof RuntimeException)
			throw (RuntimeException)t;
		else
			throw new RuntimeException(t);
	}
}