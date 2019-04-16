package grakn.core.graql.gremlin;

/**
 * Goal of this class is to micro-benchmark small JanusGraph operations
 * Eventually using this to derive constants for the query planner (eg. cost of an edge traversal)
 * we may want to pick the cheapest thing, for instance looking up a set of vertices by property as the base unit cost
 * and make everything else a multiplier based on this
 * The tests should fail if the constant multipliers we derive are no longer hold due to a Janus implementation change,
 * new indexing, etc.
 */
public class MicroBenchmark {

    /*

    - cost of filtering vertices on 1 property vs 2, 3, 4...
    - cost of batch writing 1, 2, 3 properties...
    - cost of filter not(has property) vs has(property)
    - cost of filtering by property vs 1 edge traversal (eg. type label lookup vs 1 isa-edge traversal)
    - cost of using `as` and then referring back to prior answer sets that janus found, versus linear chains of evaluation
    - cost of `last`, which we use a lot
    - cost of native V().id(x) versus a property lookup
    - cost of a filter on a property on janus versus retriving all vertices and doing filter on application level

     */
}
