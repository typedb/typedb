package ai.grakn.test.engine.postprocessing;

import ai.grakn.GraknTx;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.graph.internal.GraknTxAbstract;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

public class PostProcessingTestUtils {

    static boolean checkUnique(GraknTx graph, String key) {
        return graph.admin().getTinkerTraversal().V().has(Schema.VertexProperty.INDEX.name(), key).toList().size() < 2;
    }
    
    static <T> String indexOf(GraknTx graph, Resource<T> resource) {
        Vertex originalResource = graph.admin().getTinkerTraversal().V()
                                        .has(Schema.VertexProperty.ID.name(), resource.getId().getValue()).next();
        return originalResource.value(Schema.VertexProperty.INDEX.name());
    }
    
    @SuppressWarnings("unchecked")
    static <T> Resource<T> createDuplicateResource(GraknTx graknTx, Resource<T> resource) {
        ResourceType<T> resourceType = resource.type();
        GraknTxAbstract<?> graph = (GraknTxAbstract<?>) graknTx;
        Vertex originalResource = graph.getTinkerTraversal().V()
                .has(Schema.VertexProperty.ID.name(), resource.getId().getValue()).next();
        Vertex vertexResourceTypeShard = graph.getTinkerTraversal().V()
                .has(Schema.VertexProperty.ID.name(), resourceType.getId().getValue()).in(Schema.EdgeLabel.SHARD.getLabel()).next();
        Vertex resourceVertex = graph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE.name());
        resourceVertex.property(Schema.VertexProperty.INDEX.name(),originalResource.value(Schema.VertexProperty.INDEX.name()));
        resourceVertex.property(resourceType.getDataType().getVertexProperty().name(), resource.getValue());
        resourceVertex.property(Schema.VertexProperty.ID.name(), resourceVertex.id().toString());
        resourceVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), vertexResourceTypeShard);
        return (Resource<T>) graknTx.admin().buildConcept(resourceVertex);
    }
    
    @SuppressWarnings("unchecked")
    static <T> Set<Vertex> createDuplicateResource(GraknTx graknTx, ResourceType<T> resourceType, Resource<T> resource) {
        GraknTxAbstract<?> graph = (GraknTxAbstract<?>) graknTx;
        Vertex originalResource = graph.getTinkerTraversal().V()
                .has(Schema.VertexProperty.ID.name(), resource.getId().getValue()).next();
        Vertex vertexResourceTypeShard = graph.getTinkerTraversal().V().
                has(Schema.VertexProperty.ID.name(), resourceType.getId().getValue()).
                in(Schema.EdgeLabel.SHARD.getLabel()).next();

        Vertex resourceVertex = graph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE.name());
        resourceVertex.property(Schema.VertexProperty.INDEX.name(),originalResource.value(Schema.VertexProperty.INDEX.name()));
        resourceVertex.property(Schema.VertexProperty.VALUE_STRING.name(), originalResource.value(Schema.VertexProperty.VALUE_STRING.name()));
        resourceVertex.property(Schema.VertexProperty.ID.name(), Schema.PREFIX_VERTEX + resourceVertex.id());

        resourceVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), vertexResourceTypeShard);
        // This is done to push the concept into the cache
        //noinspection ResultOfMethodCallIgnored
        graknTx.admin().buildConcept(resourceVertex);
        return Sets.newHashSet(originalResource, resourceVertex);
    }
}
