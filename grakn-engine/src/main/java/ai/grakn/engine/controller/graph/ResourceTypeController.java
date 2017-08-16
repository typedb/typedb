package ai.grakn.engine.controller.graph;

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.ResourceType;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.exception.GraknServerException;
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

public class ResourceTypeController {
    private final EngineGraknGraphFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(EntityTypeController.class);

    public ResourceTypeController(EngineGraknGraphFactory factory, Service spark,
                                MetricRegistry metricRegistry) {
        this.factory = factory;
        spark.post("/graph/resourceType", this::postResourceType);
    }

    private Json postResourceType(Request request, Response response) {
        Map<String, Object> requestBody = Json.read(mandatoryBody(request)).asMap();
        String resourceTypeLabel = (String) requestBody.get("resourceTypeLabel");
        String resourceTypeDataTypeRaw = (String) requestBody.get("resourceTypeDataType");
        ResourceType.DataType<?> resourceTypeDataType = fromString(resourceTypeDataTypeRaw);
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        try (GraknGraph graph = factory.getGraph(keyspace, GraknTxType.WRITE)) {
            ResourceType resourceType = graph.putResourceType(resourceTypeLabel, resourceTypeDataType);
            graph.commit();
            response.status(200);
            Json responseBody = Json.object(
                "conceptId", resourceType.getId().getValue(),
                "entityTypeLabel", resourceType.getLabel().getValue()
            );
            return responseBody;
        }
    }

    private ResourceType.DataType<?> fromString(String dataType) {
        if (dataType.equals("string")) {
            return ResourceType.DataType.STRING;
        } else if (dataType.equals("double")) {
            return ResourceType.DataType.DOUBLE;
        } else if (dataType.equals("boolean")) {
            return ResourceType.DataType.BOOLEAN;
        } else {
            throw GraknServerException.invalidQueryExplaination("invalid datatype supplied: '" + dataType + "'");
        }
    }
}
