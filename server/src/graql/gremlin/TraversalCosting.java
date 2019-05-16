package grakn.core.graql.gremlin;

public class TraversalCosting {

    /*

    The goal here is to consume available statistics and provide estimates of number of edge traversals and matching
    vertices that are going to be performed.

    Assuming we start from the query planner graph completed, and we retain the MST with middle nodes trick, and 0
    cost on the second leg of the directed edge from the middle node.

    Start with the structure of the traversal graph completed.

    1. For every node representing a vertex, we infer as much type information as we can - though this may just be `Thing`
    or `SchemaConcept`.
    2. Now we can get a count on that type using statistics. Thus, we know the quantity of every (non-middle)
    node in the graph (corresponding to Janus vertices).
    3. Calculate the number of edges that match each edge in the query planner graph, NORMALIZED per vertex. Eg.
    estimate the number of outgoing edges any individual attribute instance has. This calculation will require two/three types:
    the start, and end of the edge (middle nodes need be ignored here I think). The idea is to estimate the number of
    the given edge type per start vertex type (TODO will edge type differ from the end of edge type? Are they providing the same info?)
    4. Now that we have the number of each QP graph vertex, and the number of outgoing edge from each vertex's instance,
    we DE NORMALIZE the vertices. In other words, if we had 10 names, with 10 outgoing edges each (so normalized cost of the
    role player edge was 10 when normalized per name), we convert the edge cost to 100 (10x10). Do this across the entire graph.
    5. Because the MST should be multiplying costs, instead of adding them, to calculate number of edges/vertex touched (essentially)
    log every cost (if we were able to use multiplication and pick the lowest cost using multiplication, we could find the same order with
    log(a*b*c), which is the same as log(a) + log(b) + log(c)
    6. Perform MST on the weighted graph


    Steps 2, 3, 4, 5, should be handled in this class.


    * Handling the type hierarchy in this framework
    => This would change from each vertex maybe 1 type to it may be a set of types. To count the number of instances that may
    have the edge, start at the top of the hierarchy of the types (implicit in the set) and work downward, stopping at the first
    that may have that edge, summing up the number of instances in this sort of stopping-DFS approach

    Open questions:
    * Reified edges
    =>
    * Cost of edge traversal versus indexed lookups
    * Comparisons, filtering etc.


    This procedure also tells us that we can improve the QP accuracy by getting better estimates of the number of
    outgoing edges of a specific type from each instance. As statistics improve, we can improve the costing mechanism here

     */


    /*
    Then, we need to convert the tree acquired from the MSt algorithm into a linear plan.
    This has been the job of the greedy traversal planner, which can be improved by actually finding the optimal
    order of traversing the tree using a Dynamic Programming algorithm - if we start by assigning the cost
    of a traversal as: a*b + a*b*c + a*b*c*d... we want to find the optimal ordering of a, b, c, d etc. to reduce this sume
    This is the ordering that the tree should be traversed in. The values of a,b,c etc. can be estimated using instance counts
    to start with and made more accurate over time with first-order statistics (eg. # of this type/role per that type).
     */

}
