package factory;

import io.mindmaps.core.implementation.MindmapsTransactionImpl;
import io.mindmaps.factory.MindmapsGraphFactory;
import io.mindmaps.factory.MindmapsTitanGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.IOException;
import java.util.Properties;

public class GraphFactory {

    private String CONFIG;

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


        titanGraphFactory = MindmapsTitanGraphFactory.getInstance();
        Properties prop = new Properties();
        try {
            prop.load(getClass().getClassLoader().getResourceAsStream("application.properties"));
            idBlockSize = Integer.parseInt(prop.getProperty("graph.block-size"));
            CONFIG = prop.getProperty("graphdatabase.config");
            System.out.println("hello config " + CONFIG);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public MindmapsTransactionImpl buildMindmapsGraphBatchLoading() {
        MindmapsTransactionImpl graph = buildGraph(CONFIG);
        graph.enableBatchLoading();
        return graph;
    }

    public MindmapsTransactionImpl buildMindmapsGraph() {
        return buildGraph(CONFIG);
    }

    private synchronized MindmapsTransactionImpl buildGraph(String config) {

        MindmapsTransactionImpl mindmapsGraph = (MindmapsTransactionImpl) titanGraphFactory.newGraph(config).newTransaction();
        Graph graph = mindmapsGraph.getTinkerPopGraph();
        graph.configuration().setProperty("ids.block-size", idBlockSize);

        return mindmapsGraph;
    }
}
