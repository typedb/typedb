package io.mindmaps.engine;

import com.jayway.restassured.RestAssured;
import io.mindmaps.engine.util.ConfigProperties;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Properties;

public abstract class MindmapsEngineTestBase {
    @BeforeClass
    public static void setupControllers() throws InterruptedException {
        System.setProperty(ConfigProperties.CONFIG_FILE_SYSTEM_PROPERTY, ConfigProperties.TEST_CONFIG_FILE);
        Properties prop = ConfigProperties.getInstance().getProperties();
        RestAssured.baseURI = "http://" + prop.getProperty("server.host") + ":" + prop.getProperty("server.port");
        MindmapsEngineServer.start();
        Thread.sleep(5000);
    }

    @AfterClass
    public static void takeDownControllers() throws InterruptedException {
        MindmapsEngineServer.stop();
        Thread.sleep(5000);
    }
}
