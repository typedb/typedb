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

package ai.grakn.test.engine.user;

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.engine.user.Password;
import ai.grakn.engine.user.UsersHandler;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.test.EngineContext;
import mjson.Json;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class UserHandlerTest {

    private static UsersHandler users = UsersHandler.getInstance();
    private static String userName = "geralt";
    private static String password = "witcher";

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    @Before
    public void addUser(){
        Json body = Json.object(UsersHandler.USER_NAME, userName, UsersHandler.USER_PASSWORD, password);
        users.addUser(body);
    }

    @After
    public void removeUser(){
        assertTrue(users.userExists(userName));
        users.removeUser(userName);
        assertFalse(users.userExists(userName));
    }

    @Test
    public void testGetUser(){
        Map<String, Json> retrevedData = users.getUser(userName).asJsonMap();
        String retrievedUsername = retrevedData.get(UsersHandler.USER_NAME).asString();
        String retreivedPassword = retrevedData.get(UsersHandler.USER_PASSWORD).asString();
        String retreivedSalt = retrevedData.get(UsersHandler.USER_SALT).asString();

        byte[] salt = Password.getBytes(retreivedSalt);

        byte[] expectedHash = Password.getBytes(retreivedPassword);

        assertEquals(userName, retrievedUsername);
        assertTrue("Stored password does not match hashed one", Password.isExpectedPassword(password.toCharArray(), salt, expectedHash));
    }

    @Test
    public void testUserInGraph(){
        GraknGraph graph = EngineGraknGraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME, GraknTxType.WRITE);
        assertNotNull(graph.getResourceType(UsersHandler.USER_NAME).getResource(userName));
        graph.close();
    }

    @Test
    public void testValidateUser(){
        assertFalse(users.validateUser("bob", password));
        assertFalse(users.validateUser(userName, "bob"));
        assertTrue(users.validateUser(userName, password));
    }

    @Ignore // Not Supported Yet
    @Test
    public void testUpdateUser(){
        assertFalse(users.getUser(userName).has(UsersHandler.USER_IS_ADMIN));

        Json body = Json.object(UsersHandler.USER_NAME, userName,
                UsersHandler.USER_PASSWORD, password,
                UsersHandler.USER_IS_ADMIN, true);
        users.updateUser(body);

        assertTrue(users.getUser(userName).is(UsersHandler.USER_IS_ADMIN, true));
    }
}
