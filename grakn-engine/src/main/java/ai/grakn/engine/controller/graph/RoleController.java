package ai.grakn.engine.controller.graph;

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Role;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import com.codahale.metrics.MetricRegistry;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import java.util.Map;

import static ai.grakn.engine.controller.util.Requests.mandatoryBody;
import static ai.grakn.engine.controller.util.Requests.mandatoryQueryParameter;
import static ai.grakn.util.REST.Request.KEYSPACE;

public class RoleController {
    private final EngineGraknGraphFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(EntityTypeController.class);

    public RoleController(EngineGraknGraphFactory factory, Service spark,
                          MetricRegistry metricRegistry) {
        this.factory = factory;

        spark.post("/graph/role", this::postRole);
    }

    private Json postRole(Request request, Response response) {
        Map<String, Object> requestBody = Json.read(mandatoryBody(request)).asMap();
        String roleLabel = (String) requestBody.get("roleLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        try (GraknGraph graph = factory.getGraph(keyspace, GraknTxType.WRITE)) {
            Role role = graph.putRole(roleLabel);
            graph.commit();

            response.status(200);
            Json responseBody = Json.object(
                "conceptId", role.getId().getValue(),
                "roleLabel", role.getLabel().getValue()
            );

            return responseBody;
        }
    }
}