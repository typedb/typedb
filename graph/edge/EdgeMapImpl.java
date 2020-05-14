package hypergraph.graph.edge;

import hypergraph.common.iterator.Iterators;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.Vertex;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

public abstract class EdgeMapImpl<
        DIR_VERTEX extends Vertex,
        DIR_EDGE_SCHEMA extends Schema.Edge,
        DIR_EDGE extends Edge<DIR_EDGE_SCHEMA, DIR_VERTEX>,
        DIR_VERTEX_ITER extends EdgeMapImpl.VertexIteratorBuilder<DIR_VERTEX, DIR_EDGE>> {

    protected final ConcurrentMap<DIR_EDGE_SCHEMA, Set<DIR_EDGE>> edges;
    protected final Direction direction;

    public enum Direction {
        OUT(true),
        IN(false);

        private final boolean isOut;

        Direction(boolean isOut) {
            this.isOut = isOut;
        }

        public boolean isOut() {
            return isOut;
        }

        public boolean isIn() {
            return !isOut;
        }
    }

    protected EdgeMapImpl(Direction direction) {
        this.direction = direction;
        edges = new ConcurrentHashMap<>();
    }

    public static class VertexIteratorBuilder<VERTEX_ITER extends Vertex, EDGE_ITER extends Edge<?, VERTEX_ITER>> {

        protected final Iterator<EDGE_ITER> edgeIterator;

        protected VertexIteratorBuilder(Iterator<EDGE_ITER> edgeIterator) {
            this.edgeIterator = edgeIterator;
        }

        public Iterator<VERTEX_ITER> to() {
            return Iterators.apply(edgeIterator, Edge::to);
        }

        public Iterator<VERTEX_ITER> from() {
            return Iterators.apply(edgeIterator, Edge::from);
        }
    }

    public abstract DIR_VERTEX_ITER edge(DIR_EDGE_SCHEMA schema);

    public abstract DIR_EDGE edge(DIR_EDGE_SCHEMA schema, DIR_VERTEX adjacent);

    public abstract void put(DIR_EDGE_SCHEMA schema, DIR_VERTEX adjacent);

    public abstract void delete(DIR_EDGE_SCHEMA schema, DIR_VERTEX adjacent);

    public abstract void delete(DIR_EDGE_SCHEMA schema);

    public abstract void deleteNonRecursive(DIR_EDGE edge);

    public abstract void deleteAll();

    public void putNonRecursive(DIR_EDGE edge) {
        edges.computeIfAbsent(edge.schema(), e -> ConcurrentHashMap.newKeySet()).add(edge);
    }

    public void forEach(Consumer<DIR_EDGE> function) {
        edges.forEach((key, set) -> set.forEach(function));
    }
}
