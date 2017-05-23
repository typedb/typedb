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

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.engine.user.UsersHandler;
import ai.grakn.test.EngineContext;
import mjson.Json;

/**
 * Testing behavior of admin/super user.
 * 
 * @author borislav
 *
 */
public class SuperUserTest {

    @ClassRule
    public static final EngineContext engine = EngineContext.startInMemoryServer();

    private static UsersHandler users = engine.server().usersHandler();

    @Test
    public void testSuperuserPresent() {
        Assert.assertNotNull(users.getUser(users.superUsername()));
    }
    
    @Test
    public void testSuperuserLogin() {        
        Assert.assertTrue(users.validateUser(users.superUsername(), 
                          GraknEngineConfig.getInstance().getProperty(GraknEngineConfig.ADMIN_PASSWORD_PROPERTY)));
        Assert.assertFalse(users.validateUser(users.superUsername(), "asgasgdsfgdsf"));
    }

    @Test
    public void testSuperuserAdd() {
        // We shouldn't be able to add a new user with the admin username
        Assert.assertFalse(users.addUser(Json.object(UsersHandler.USER_NAME, users.superUsername(), UsersHandler.USER_PASSWORD, "pass")));
    }
    
    @Test
    public void testSuperuserDelete() {
        Assert.assertFalse(users.removeUser(users.superUsername()));
    }
    
    @Test
    public void testAllUsersWithoutSuperuser() {
        Assert.assertFalse(users.allUsers(0,  Integer.MAX_VALUE).asJsonList().stream().anyMatch(
                user -> user.is(UsersHandler.USER_NAME, users.superUsername()) ));
    }
}
