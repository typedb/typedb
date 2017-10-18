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
package ai.grakn.engine.controller;

import ai.grakn.engine.EngineTestHelper;
import ai.grakn.GraknConfigKey;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.builder.RequestSpecBuilder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.ArrayList;

/**
 * Setup and cleanup for controller tests. 
 * 
 * @author borislav
 *
 */
public class ControllerFixture implements TestRule {

    protected ArrayList<Runnable> cleanupOperations = new ArrayList<Runnable>();
    
    /**
     * Add some operation do perform at the end of the test class execution to cleanup
     * state before other test classes are run.
     * @param op
     */
    public void cleanup(Runnable op) {
        cleanupOperations.add(op);
    }
    
    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            public void evaluate() throws Throwable {
                final String currentURI = RestAssured.baseURI;
                restAssuredSetup();
                EngineTestHelper.engineWithKBs();
                try {
                    base.evaluate();
                }
                finally {
                    doCleanup();
                    RestAssured.baseURI = currentURI;
                    RestAssured.requestSpecification = new RequestSpecBuilder().build(); 
                }
            }            
        };
    }

    public static final ControllerFixture INSTANCE = new ControllerFixture(); 
    
    public static String baseURI() {
        return "http://localhost:" + EngineTestHelper.config().getProperty(GraknConfigKey.SERVER_PORT);
    }
    
    private void restAssuredSetup() {
        RestAssured.baseURI = baseURI();
        RestAssured.requestSpecification = new RequestSpecBuilder()
                .addHeader("Accept", "application/json")
                .addHeader("Content-Type", "application/json").build();        
    }
    
    private void doCleanup() {
        final ArrayList<Throwable> cleanupFailures = new ArrayList<Throwable>();
        cleanupOperations.forEach(r -> {
            try {
                r.run(); 
            }
            catch (Throwable t) {
                cleanupFailures.add(t);
            }
        });
        cleanupOperations.clear();
        if (!cleanupFailures.isEmpty()) {
            cleanupFailures.forEach(t -> t.printStackTrace(System.err) );
            throw new RuntimeException("There are failures during test cleanup, see stderr for a printout of all of them.");
        }        
    }    
}
