package ai.grakn.engine.controller;

import java.util.ArrayList;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.builder.RequestSpecBuilder;

import ai.grakn.engine.EngineTestHelper;
import ai.grakn.engine.GraknEngineConfig;

/*
 * There are some setup steps that all classes testing engine controllers needs. Those are
 * collected here. Please keep the code here minimal and keep every step self-contained and
 * independent. 
 */
public class ControllerTestBase {

    @BeforeClass
    public static void initRestAssured() {
        RestAssured.baseURI = "http://localhost:" + EngineTestHelper.config().getProperty(GraknEngineConfig.SERVER_PORT_NUMBER);
        RestAssured.requestSpecification = new RequestSpecBuilder()
                .addHeader("Accept", "application/json")
                .addHeader("Content-Type", "application/json").build();        
    }
    
    @BeforeClass
    public static void ensureEngineRunning() {
        EngineTestHelper.engine();
    }
    
    protected static ArrayList<Runnable> cleanupOperations = new ArrayList<Runnable>();
    
    /**
     * Add some operation do perform at the end of the test class execution to cleanup
     * state before other test classes are run.
     * @param op
     */
    protected static void cleanup(Runnable op) {
        cleanupOperations.add(op);
    }
    
    /**
     * Execute all cleanup operations added by individual tests.
     */
    @AfterClass
    public static void cleanup() {
        cleanupOperations.forEach(r -> r.run() );
    }
}
