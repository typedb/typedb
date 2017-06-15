package ai.grakn.engine.controller;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.builder.RequestSpecBuilder;

import ai.grakn.engine.EngineTestHelper;
import ai.grakn.engine.GraknEngineConfig;

public class ControllerSetup implements TestRule {

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            public void evaluate() throws Throwable {
                RestAssured.baseURI = "http://localhost:" + EngineTestHelper.config().getProperty(GraknEngineConfig.SERVER_PORT_NUMBER);
                RestAssured.requestSpecification = new RequestSpecBuilder()
                        .addHeader("Accept", "application/json")
                        .addHeader("Content-Type", "application/json").build();
                base.evaluate();
            }            
        };
    }

    public static final ControllerSetup INSTANCE = new ControllerSetup(); 
}
