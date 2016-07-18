package api;

import factory.GraphFactory;
import io.mindmaps.core.implementation.MindmapsTransactionImpl;
import spark.Request;
import spark.Response;
import visualiser.HALConcept;

import static spark.Spark.get;

public class VisualiserController {

    MindmapsTransactionImpl graph;

    public VisualiserController() {

        graph = GraphFactory.getInstance().buildMindmapsGraph();

        get("/concepts", this::getConceptsByValue);

        get("/concept/:id", this::getConceptById);

    }

    private String getConceptsByValue(Request req, Response res) {
        graph.getConceptsByValue(req.queryParams("value"));
        return req.queryParams("value");
    }

    private String getConceptById(Request req, Response res) {
//        graph.getConcept(req.params(":id")).getValue();
        if (graph.getConcept(req.params(":id")) != null)
            return new HALConcept(graph.getConcept(req.params(":id"))).render();
        else {
            res.status(404);
            return "ID not found in the graph.";
        }
    }

}
