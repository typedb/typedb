package hypergraph.graph.adjacency;

import hypergraph.graph.edge.TypeEdge;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.TypeVertex;

public interface TypeAdjacency extends Adjacency<EDGE_SCHEMA, EDGE, VERTEX> {
    @Override
    TypeAdjacencyImpl.TypeIteratorBuilder edge(Schema.Edge.Type schema);

    @Override
    void put(Schema.Edge.Type schema, TypeVertex adjacent);

    @Override
    void deleteNonRecursive(TypeEdge edge);

    @Override
    void deleteAll();
}
