package ai.grakn.test.engine.postprocessing;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

public class PostProcessingTestUtils {

    static boolean checkUnique(GraknGraph graph, String key) {
        return graph.admin().getTinkerTraversal().V().has(Schema.VertexProperty.INDEX.name(), key).toList().size() < 2;
    }
    
    static <T> String indexOf(GraknGraph graph, Attribute<T> attribute) {
        Vertex originalResource = graph.admin().getTinkerTraversal().V()
                                        .has(Schema.VertexProperty.ID.name(), attribute.getId().getValue()).next();
        return originalResource.value(Schema.VertexProperty.INDEX.name());
    }
    
    @SuppressWarnings("unchecked")
    static <T> Attribute<T> createDuplicateResource(GraknGraph graknGraph, Attribute<T> attribute) {
        AttributeType<T> attributeType = attribute.type();
        AbstractGraknGraph<?> graph = (AbstractGraknGraph<?>) graknGraph;
        Vertex originalResource = graph.getTinkerTraversal().V()
                .has(Schema.VertexProperty.ID.name(), attribute.getId().getValue()).next();
        Vertex vertexResourceTypeShard = graph.getTinkerTraversal().V()
                .has(Schema.VertexProperty.ID.name(), attributeType.getId().getValue()).in(Schema.EdgeLabel.SHARD.getLabel()).next();
        Vertex resourceVertex = graph.getTinkerPopGraph().addVertex(Schema.BaseType.ATTRIBUTE.name());
        resourceVertex.property(Schema.VertexProperty.INDEX.name(),originalResource.value(Schema.VertexProperty.INDEX.name()));
        resourceVertex.property(attributeType.getDataType().getVertexProperty().name(), attribute.getValue());
        resourceVertex.property(Schema.VertexProperty.ID.name(), resourceVertex.id().toString());
        resourceVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), vertexResourceTypeShard);
        return (Attribute<T>)graknGraph.admin().buildConcept(resourceVertex);
    }
    
    @SuppressWarnings("unchecked")
    static <T> Set<Vertex> createDuplicateResource(GraknGraph graknGraph, AttributeType<T> attributeType, Attribute<T> attribute) {
        AbstractGraknGraph<?> graph = (AbstractGraknGraph<?>) graknGraph;
        Vertex originalResource = graph.getTinkerTraversal().V()
                .has(Schema.VertexProperty.ID.name(), attribute.getId().getValue()).next();
        Vertex vertexResourceTypeShard = graph.getTinkerTraversal().V().
                has(Schema.VertexProperty.ID.name(), attributeType.getId().getValue()).
                in(Schema.EdgeLabel.SHARD.getLabel()).next();

        Vertex resourceVertex = graph.getTinkerPopGraph().addVertex(Schema.BaseType.ATTRIBUTE.name());
        resourceVertex.property(Schema.VertexProperty.INDEX.name(),originalResource.value(Schema.VertexProperty.INDEX.name()));
        resourceVertex.property(Schema.VertexProperty.VALUE_STRING.name(), originalResource.value(Schema.VertexProperty.VALUE_STRING.name()));
        resourceVertex.property(Schema.VertexProperty.ID.name(), Schema.PREFIX_VERTEX + resourceVertex.id());

        resourceVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), vertexResourceTypeShard);
        // This is done to push the concept into the cache
        //noinspection ResultOfMethodCallIgnored
        graknGraph.admin().buildConcept(resourceVertex);
        return Sets.newHashSet(originalResource, resourceVertex);
    }
}
