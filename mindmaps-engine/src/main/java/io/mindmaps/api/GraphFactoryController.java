package io.mindmaps.api;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import static spark.Spark.get;

public class GraphFactoryController {
    private final Logger LOG = LoggerFactory.getLogger(GraphFactoryController.class);

    private final String GRAPH_CONFIG;

    public GraphFactoryController() {
        Properties prop = new Properties();
        try {
            prop.load(getClass().getClassLoader().getResourceAsStream("application.properties"));
        } catch (Exception e) {
            e.printStackTrace();
        }

        GRAPH_CONFIG=prop.getProperty("graphdatabase.config");

        get("/graph_factory", (req, res) -> {
            try {
                return new String(Files.readAllBytes(Paths.get(GRAPH_CONFIG)));
            } catch (IOException e) {
                LOG.error("Cannot find config file [" + GRAPH_CONFIG + "]", e);
                throw new IOException("Cannot find config file [" + GRAPH_CONFIG + "]");
            }
        });

    }
}
