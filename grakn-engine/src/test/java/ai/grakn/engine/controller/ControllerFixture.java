package ai.grakn.engine.controller;

import java.util.ArrayList;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.builder.RequestSpecBuilder;

import ai.grakn.engine.EngineTestHelper;
import ai.grakn.engine.GraknEngineConfig;

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
                restAssuredSetup();
                EngineTestHelper.engine();
                try {
                    base.evaluate();
                }
                finally {
                    doCleanup();
                }
            }            
        };
    }

    public static final ControllerFixture INSTANCE = new ControllerFixture(); 
    
    private void restAssuredSetup() {
        RestAssured.baseURI = "http://localhost:" + EngineTestHelper.config().getProperty(GraknEngineConfig.SERVER_PORT_NUMBER);
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
