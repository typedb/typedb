package ai.grakn;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class Mocks {

    @SuppressWarnings("unchecked")
    public static <S, T> GraphTraversal<S, T> traversal() {
        GraphTraversal mockTraversal = mock(GraphTraversal.class);
        when(mockTraversal.union(any())).thenReturn(mockTraversal);
        when(mockTraversal.out(any())).thenReturn(mockTraversal);
        when(mockTraversal.outE(any())).thenReturn(mockTraversal);
        when(mockTraversal.has(any())).thenReturn(mockTraversal);
        when(mockTraversal.unfold()).thenReturn(mockTraversal);
        return mockTraversal;
    }
}
