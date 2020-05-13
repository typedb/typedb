package hypergraph.graph;

import hypergraph.graph.vertex.TypeVertex;
import hypergraph.graph.vertex.Vertex;

public interface Graph<VERTEX extends Vertex> {

    Storage storage();

    VERTEX get(byte[] iid);

    void delete(TypeVertex vertex);

    void commit();

    void clear();
}
