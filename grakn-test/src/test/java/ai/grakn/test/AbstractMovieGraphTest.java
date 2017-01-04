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

import static ai.grakn.test.GraknTestEnv.ensureEngineRunning;
import static ai.grakn.test.GraknTestEnv.factoryWithNewKeyspace;
import static ai.grakn.test.GraknTestEnv.shutdownEngine;
import static ai.grakn.test.GraknTestEnv.usingTinker;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import ai.grakn.GraknGraph;
import ai.grakn.GraknGraphFactory;
import ai.grakn.engine.backgroundtasks.standalone.StandaloneTaskManager;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.example.MovieGraphFactory;

/**
 * Abstract test class that uses the movie graph, automatically rolling back after every test to a fresh movie graph.
 * Do not commit to this graph, because it is shared between all tests for performance!
 * 
 * (Note from Boris : The design as explained above seems sensible and clever, saving time etc., but in a large 
 * test suite, there must be some cleanup at some point. This design never cleans up that movie database and
 * that's a problem. Performs jUnit is not flexible enough, but there maybe plugins or other tools that allows
 * "test fixtures" to be shared by a large number of tests, for performance reasons, yet to clean them up afterwards. 
 */
public abstract class AbstractMovieGraphTest {
    protected static GraknGraphFactory factory;
    protected static GraknGraph graph;

    @BeforeClass
    public static void initializeGraknTests() {
    	try {
	        ConfigProperties.getInstance().setConfigProperty(ConfigProperties.TASK_MANAGER_INSTANCE, 
	        												 StandaloneTaskManager.class.getName());
	    	ensureEngineRunning();
    	}
    	catch (Exception e) {
    		e.printStackTrace(System.err);
    		throw new RuntimeException(e);
    	}
    }

    @AfterClass
    public static void cleanupGraknTests() {
    	try {
	    	shutdownEngine();
    	}
    	catch (Exception e) {
    		e.printStackTrace(System.err);
    		throw new RuntimeException(e);
    	}
    }
    
    @Before
    public void createGraph() {
        if (factory == null || graph == null) {
            factory = factoryWithNewKeyspace();
            graph = factory.getGraph();
            MovieGraphFactory.loadGraph(graph);
        }
    }

    @After
    public final void rollbackGraph() {
        if (usingTinker()) {
            // If using tinker, make a fresh graph
            factory = null;
            graph = null;
        } else {
            try {
                graph.rollback();
            } catch (UnsupportedOperationException e) {
                // If operation unsupported, make a fresh graph
                factory = null;
                graph = null;
            }
        }
    }

}
