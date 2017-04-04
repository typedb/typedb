package ai.grakn.test.engine.postprocessing;

import ai.grakn.GraknGraph;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.exception.MoreThanOneConceptException;
import ai.grakn.graph.admin.ConceptCache;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class PostProcessingTestUtils {

    static boolean checkUnique(GraknGraph graph, String key) {
        try {
            graph.admin().getConcept(Schema.ConceptProperty.INDEX, key);
            return true;
        }
        catch (MoreThanOneConceptException ex) {
            return false;
        }
    }
    
    static <T> String indexOf(GraknGraph graph, Resource<T> resource) {
        Vertex originalResource = graph.admin().getTinkerTraversal()
                                        .hasId(resource.getId().getValue()).next();
        return originalResource.value(Schema.ConceptProperty.INDEX.name());
    }
    
    @SuppressWarnings("unchecked")
    static <T> Resource<T> createDuplicateResource(GraknGraph graknGraph, Resource<T> resource) {
        ResourceType<T> resourceType = resource.type();
        AbstractGraknGraph<?> graph = (AbstractGraknGraph<?>) graknGraph;
        Vertex originalResource = (Vertex) graph.getTinkerTraversal()
                .hasId(resource.getId().getValue()).next();
        Vertex vertexResourceType = (Vertex) graph.getTinkerTraversal()
                .hasId(resourceType.getId().getValue()).next();
        Vertex resourceVertex = graph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE.name());
        resourceVertex.property(Schema.ConceptProperty.INDEX.name(),originalResource.value(Schema.ConceptProperty.INDEX.name()));
        resourceVertex.property(resourceType.getDataType().getConceptProperty().name(), resource.getValue());
        resourceVertex.property(Schema.ConceptProperty.ID.name(), resourceVertex.id().toString());
        resourceVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), vertexResourceType);        
        return (Resource<T>)graknGraph.admin().buildConcept(resourceVertex);
    }
    
    @SuppressWarnings("unchecked")
    static <T> Resource<T> createDuplicateResource(GraknGraph graknGraph, ConceptCache cache, ResourceType<T> resourceType, Resource<T> resource) {
        AbstractGraknGraph<?> graph = (AbstractGraknGraph<?>) graknGraph;
        Vertex originalResource = (Vertex) graph.getTinkerTraversal()
                .hasId(resource.getId().getValue()).next();
        Vertex vertexResourceType = (Vertex) graph.getTinkerTraversal()
                .hasId(resourceType.getId().getValue()).next();

        Vertex resourceVertex = graph.getTinkerPopGraph().addVertex(Schema.BaseType.RESOURCE.name());
        resourceVertex.property(Schema.ConceptProperty.INDEX.name(),originalResource.value(Schema.ConceptProperty.INDEX.name()));
        resourceVertex.property(Schema.ConceptProperty.VALUE_STRING.name(), originalResource.value(Schema.ConceptProperty.VALUE_STRING.name()));
        resourceVertex.property(Schema.ConceptProperty.ID.name(), resourceVertex.id().toString());

        resourceVertex.addEdge(Schema.EdgeLabel.ISA.getLabel(), vertexResourceType);
        cache.addJobResource(graknGraph.getKeyspace(), resourceVertex.value(Schema.ConceptProperty.INDEX.name()).toString(), ConceptId.of(resourceVertex.id().toString()));        
        return (Resource<T>)graknGraph.admin().buildConcept(resourceVertex);
    }
}
