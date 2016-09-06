package io.mindmaps.engine;

import com.jayway.restassured.RestAssured;

import java.util.Properties;

public class Util {

    public static void setRestAssuredBaseURI(Properties prop) {
        RestAssured.baseURI = "http://" + prop.getProperty("server.host") + ":" + prop.getProperty("server.port");
    }
}
