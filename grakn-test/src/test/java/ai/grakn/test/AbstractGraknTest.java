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

import static ai.grakn.engine.util.ConfigProperties.TASK_MANAGER_INSTANCE;
import static ai.grakn.test.GraknTestEnv.ensureEngineRunning;
import static ai.grakn.test.GraknTestEnv.shutdownEngine;
import static java.lang.Thread.sleep;

import ai.grakn.engine.backgroundtasks.standalone.StandaloneTaskManager;
import ai.grakn.engine.util.ConfigProperties;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Abstract test class that automatically starts the relevant graph database and provides a method to get a graph factory
 */
public abstract class AbstractGraknTest {
	
    @BeforeClass
    public static void initializeGraknTests() {
    	try {
	        ConfigProperties.getInstance().setConfigProperty(TASK_MANAGER_INSTANCE, StandaloneTaskManager.class.getName());
	    	ensureEngineRunning();
	        sleep(5000);
    	}
    	catch (Exception e) {
    		e.printStackTrace(System.err);
    		throw new RuntimeException(e);
    	}
    }

    @AfterClass
    public static void cleanupGraknTests() {
    	try {
    		// TODO: clearing all graphs was added to the Engine tests with idea of starting
    		// each test with  a clean slate. However, a lot of test in various classes assume
    		// the presence of some graphs created and populated once per JVM execution (e.g.
    		// movie graph reference held in a static variable). The approach is obviously not ideal,
    		// akin to using global variables in a large program...a "no no"
//    		clearGraphs();
	    	shutdownEngine();
	        sleep(5000);
    	}
    	catch (Exception e) {
    		e.printStackTrace(System.err);
    		throw new RuntimeException(e);
    	}
    }
}
