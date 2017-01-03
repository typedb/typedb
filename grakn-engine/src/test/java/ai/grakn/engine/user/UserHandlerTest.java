package ai.grakn.engine.user;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.engine.GraknEngineTestBase;
import ai.grakn.factory.SystemKeyspace;
import mjson.Json;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class UserHandlerTest extends GraknEngineTestBase {
    private static UsersHandler users;
    private static String userName = "geralt";
    private static String password = "witcher";

    @BeforeClass
    public static void setup(){
        Json body = Json.object(UsersHandler.USER_NAME, userName, UsersHandler.USER_PASSWORD, password);
        users = UsersHandler.getInstance();
        users.addUser(body);
    }

    @Test
    public void testGetUser(){
        assertTrue(users.getUser(userName).is(UsersHandler.USER_NAME, userName));
        assertTrue(users.getUser(userName).is(UsersHandler.USER_PASSWORD, password));
    }

    @Test
    public void testUserInGraph(){
        GraknGraph graph = Grakn.factory(Grakn.IN_MEMORY, SystemKeyspace.SYSTEM_GRAPH_NAME).getGraph();
        assertNotNull(graph.getResourceType("user-name").getResource(userName));
    }

    @Test
    public void removeUser(){
        assertTrue(users.userExists(userName));
        users.removeUser(userName);
        assertFalse(users.userExists(userName));
    }

    @Ignore
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
