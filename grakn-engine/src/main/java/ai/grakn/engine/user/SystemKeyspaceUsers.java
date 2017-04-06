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

package ai.grakn.engine.user;

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.TypeLabel;
import ai.grakn.engine.cache.EngineCacheProvider;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.graql.AskQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Var;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.Schema.MetaSchema.RESOURCE;

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
        Var user = var().isa(USER_ENTITY);
        try (GraknGraph graph = EngineGraknGraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME, GraknTxType.WRITE)) {

            for (Map.Entry<String, Json> entry : userJson.asJsonMap().entrySet()) {
                String property = entry.getKey();
                Json value = entry.getValue();

                if(property.equals(UsersHandler.USER_PASSWORD)){//Hash the password with a salt
                    byte[] salt = Password.getNextSalt(graph);
                    byte[] hashedPassword = Password.hash(value.getValue().toString().toCharArray(), salt);

                    user = user.has(UsersHandler.USER_PASSWORD, Password.getString(hashedPassword));
                    user = user.has(UsersHandler.USER_SALT, Password.getString(salt));
                } else {
                    user = user.has(property, value.getValue());
                }
            }

            InsertQuery query = graph.graql().insert(user);
            query.execute();
            graph.admin().commit(EngineCacheProvider.getCache());
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
    @Override
    public boolean userExists(String username) {
        Var lookup = var().isa(USER_ENTITY).has(USER_NAME, username);
        try (GraknGraph graph = EngineGraknGraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME, GraknTxType.WRITE)) {
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
    @Override
    public Json getUser(String username) {
        Var lookup = var("entity").isa(USER_ENTITY).has(USER_NAME, username);
        Var resource = var("property");
        try (GraknGraph graph = EngineGraknGraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME, GraknTxType.WRITE)) {
            MatchQuery query = graph.graql().match(lookup.has(RESOURCE.getLabel(), resource));
            List<Map<String, Concept>> L = query.execute();
            if (L.isEmpty()) {
                return Json.nil();
            }
            Json user = Json.object();
            L.forEach(property -> {
                TypeLabel label = property.get("property").asInstance().type().getLabel();
                Object value = property.get("property").asResource().getValue();
                user.set(label.getValue(), value);
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
     *
     * @param username The username of the user to validate.
     * @param passwordClient The password sent from the client.
     * @return true if the user exists and if the password is correct
     */
    @Override
    public boolean validateUser(String username, String passwordClient) {
        try (GraknGraph graph = EngineGraknGraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME, GraknTxType.WRITE)) {
            List<Map<String, Concept>> results = graph.graql().match(
                    var("salt").isa(USER_SALT),
                    var("stored-password").isa(USER_PASSWORD),
                    var("entity").isa(USER_ENTITY).
                            has(USER_NAME, username).
                            has(USER_PASSWORD, var("stored-password")).
                            has(USER_SALT, var("salt"))).execute();

            if(!results.isEmpty()){
                Concept saltConcept = results.get(0).get("salt");
                Concept passwordConcept = results.get(0).get("stored-password");

                if(saltConcept != null && passwordConcept != null && saltConcept.isResource() && passwordConcept.isResource()){
                    byte[] salt = Password.getBytes(saltConcept.asResource().getValue().toString());
                    byte[] expectedPassword = Password.getBytes(passwordConcept.asResource().getValue().toString());
                    return Password.isExpectedPassword(passwordClient.toCharArray(), salt, expectedPassword);
                }
            }
        }
        return false;
    }

    /**
     * Retrieve the list of all users with all their properties.
     *
     * @param offset
     * @param limit
     * @return
     */
    @Override
    public Json allUsers(int offset, int limit) {
        Var lookup = var("entity").isa(USER_ENTITY);
        try (GraknGraph graph = EngineGraknGraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME, GraknTxType.WRITE)) {
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
    @Override
    public boolean removeUser(String username) {
        Var lookup = var("entity").isa(USER_ENTITY).has(USER_NAME, username);
        try (GraknGraph graph = EngineGraknGraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME, GraknTxType.WRITE)) {
            MatchQuery query = graph.graql().match(lookup);
            List<Map<String, Concept>> results = query.execute();
            boolean exists = !results.isEmpty();
            results.forEach(map -> {
                map.forEach( (k,v) -> {
                    v.asInstance().resources().forEach(Concept::delete);
                    v.delete();
                });
            });

            if(exists){
                graph.admin().commit(EngineCacheProvider.getCache());
            }

            return exists;
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
        if (t instanceof Error) {
            throw (Error) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else {
            throw new RuntimeException(t);
        }
    }
}