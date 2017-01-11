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

package ai.grakn.test;

import static ai.grakn.test.GraknTestEnv.ensureCassandraRunning;
import static ai.grakn.test.GraknTestEnv.ensureHTTPRunning;
import static ai.grakn.test.GraknTestEnv.usingOrientDB;

import ai.grakn.GraknGraph;

import ai.grakn.engine.backgroundtasks.standalone.StandaloneTaskManager;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.factory.GraphFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.UUID;
import static ai.grakn.engine.util.ConfigProperties.TASK_MANAGER_INSTANCE;


/**
 * <p>
 * Abstract test class that automatically starts the relevant graph database and provides a method to get a graph factory
 * </p>
 */
public abstract class AbstractGraphTest {

    protected GraphFactory factory;
    protected GraknGraph graph;

    @BeforeClass
    public static void initializeGraknTests() {
    	try {

            //TODO remove when Bug #12029 fixed
            ConfigProperties.getInstance().setConfigProperty(TASK_MANAGER_INSTANCE, StandaloneTaskManager.class.getName());

            ensureCassandraRunning();
            ensureHTTPRunning();
    	}
    	catch (Exception e) {
    		e.printStackTrace(System.err);
    		throw new RuntimeException(e);
    	}
    }

	@Before
	public void createGraph() {
        factory = GraphFactory.getInstance();
		graph = graphWithNewKeyspace();
	}

    @After
    public void closeGraph() throws Exception {
        graph.clear();
        graph.close();
    }

    protected GraknGraph graphWithNewKeyspace() {
        String keyspace;
        if (usingOrientDB()) {
            keyspace = "memory";
        } else {
            // Embedded Casandra has problems dropping keyspaces that start with a number
            keyspace = "a"+ UUID.randomUUID().toString().replaceAll("-", "");
        }

         return GraphFactory.getInstance().getGraph(keyspace);
    }
}
