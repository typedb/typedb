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

import ai.grakn.engine.EngineTestHelper;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.engine.user.UsersHandler;
import ai.grakn.test.GraphContext;
import mjson.Json;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Testing behavior of admin/super user.
 * 
 * @author borislav
 *
 */
public class SuperUserTest {
    private static final String adminPassword = "top secret";

    private static EngineGraknGraphFactory graknFactory;

    @ClassRule
    public static final GraphContext graph = GraphContext.empty();

    private UsersHandler users;

    @Before
    public void setUp() {
        users = UsersHandler.create(adminPassword, graknFactory);
    }

    @BeforeClass
    public static void beforeClass() {
        // TODO: Doing it here because it has to happen after Cassandra starts. Consider refactoring
        graknFactory = EngineGraknGraphFactory.createAndLoadSystemOntology(EngineTestHelper.config().getProperties());
    }

    @Test
    public void testSuperuserPresent() {
        Assert.assertNotNull(users.getUser(users.superUsername()));
    }
    
    @Test
    public void testSuperuserLogin() {        
        Assert.assertTrue(users.validateUser(users.superUsername(), adminPassword));
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
