package io.mindmaps.factory;

import io.mindmaps.core.implementation.MindmapsTransactionImpl;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.IOException;
import java.util.Properties;

public class GraphFactory {

    private String CONFIG;
    private String DEFAULT_NAME; //TO_DO: This should be parametrised

    private int idBlockSize;

    private static GraphFactory instance = null;

    private MindmapsGraphFactory titanGraphFactory;


    public static synchronized GraphFactory getInstance() {
        if (instance == null) {
            instance = new GraphFactory();
        }
        return instance;
    }

    private GraphFactory() {

//        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
//        Configuration conf = ctx.getConfiguration();
//        conf.getLoggerConfig(LogManager.ROOT_LOGGER_NAME).setLevel(Level.ERROR);


        titanGraphFactory = new MindmapsTitanGraphFactory();
        Properties prop = new Properties();
        try {
            prop.load(getClass().getClassLoader().getResourceAsStream("application.properties"));
            idBlockSize = Integer.parseInt(prop.getProperty("graph.block-size"));
            CONFIG = prop.getProperty("graphdatabase.config");
            DEFAULT_NAME = prop.getProperty("graphdatabase.name");
            System.out.println("hello config " + CONFIG);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public MindmapsTransactionImpl buildMindmapsGraphBatchLoading() {
        MindmapsTransactionImpl graph = buildGraph(DEFAULT_NAME, CONFIG);
        graph.enableBatchLoading();
        return graph;
    }

    public MindmapsTransactionImpl buildMindmapsGraph() {
        return buildGraph(DEFAULT_NAME, CONFIG);
    }

    private synchronized MindmapsTransactionImpl buildGraph(String name, String config) {

        MindmapsTransactionImpl mindmapsGraph = (MindmapsTransactionImpl) titanGraphFactory.getGraph(name, null, config).newTransaction();
        Graph graph = mindmapsGraph.getTinkerPopGraph();
        graph.configuration().setProperty("ids.block-size", idBlockSize);

        return mindmapsGraph;
    }
}
