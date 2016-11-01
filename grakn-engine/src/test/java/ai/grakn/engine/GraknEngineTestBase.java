package ai.grakn.engine;

import com.jayway.restassured.RestAssured;
import ai.grakn.engine.util.ConfigProperties;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Properties;

public abstract class GraknEngineTestBase {
    @BeforeClass
    public static void setupControllers() throws InterruptedException {
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY, ConfigProperties.TEST_CONFIG_FILE);
        Properties prop = ConfigProperties.getInstance().getProperties();
        RestAssured.baseURI = "http://" + prop.getProperty("server.host") + ":" + prop.getProperty("server.port");
        GraknEngineServer.start();
        Thread.sleep(5000);
    }

    @AfterClass
    public static void takeDownControllers() throws InterruptedException {
        GraknEngineServer.stop();
        Thread.sleep(5000);
    }
}
