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

import ai.grakn.engine.factory.EngineGraknTxFactory;
import mjson.Json;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 *     Class to interact with users stored in the graph.
 * </p>
 *
 * @author Marco Scoppetta
 */
public class UsersHandler {
    public static final String SUPERUSER = "admin";
    public static final String USER_ENTITY = "user";
    public static final String USER_NAME = "user-name";
    public static final String USER_PASSWORD = "user-password";
    public static final String USER_SALT = "user-password-salt";
    public static final String USER_FIRST_NAME = "user-first-name";
    public static final String USER_LAST_NAME = "user-last-name";
    public static final String USER_EMAIL = "user-email";
    public static final String USER_IS_ADMIN = "user-is-admin";
    private final Map<String, Json> usersMap = new HashMap<>();
    final String adminPassword;

    public static UsersHandler create(String adminPassword, EngineGraknTxFactory factory) {
        return new SystemKeyspaceUsers(adminPassword, factory); // new UsersHandler();
    }

    protected UsersHandler(String adminPassword) {
        this.adminPassword = adminPassword;
    }

    public String superUsername() {
        return SUPERUSER;
    }
    
    public boolean addUser(Json user) {
        String username = user.at(USER_NAME).asString();
        if (superUsername().equals(username) || usersMap.containsKey(username)) {
            return false;
        }
        usersMap.put(username, user);
        return true;
    }

    public boolean updateUser(Json user) {
        String username = user.at(USER_NAME).asString();
        if (superUsername().equals(username) || !usersMap.containsKey(username)) {
            return false;
        }
        usersMap.put(user.at(USER_NAME).asString(), user);
        return true;
    }

    public boolean userExists(String username) {
        return superUsername().equals(username) || usersMap.containsKey(username);
    }

    public boolean validateUser(String username, String hashedPassword) {
        if (superUsername().equals(username)) {
            return hashedPassword.equals(adminPassword);
        }
        else if (userExists(username)) {
            return getUser(username).is(USER_PASSWORD, hashedPassword);
        }
        else {
            return false;
        }
    }

    public Json getUser(String username) {
        return superUsername().equals(username) ? Json.object(USER_NAME, superUsername()) : usersMap.get(username);
    }

    public boolean removeUser(String username) {
        if (superUsername().equals(username)) {
            return false;
        }
        else {
            return usersMap.remove(username) != null;
        }
    }

    public Json allUsers(int offset, int limit) {
        return Json.make(usersMap.values());
    }
}