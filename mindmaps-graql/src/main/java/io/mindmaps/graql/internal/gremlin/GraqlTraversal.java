package io.mindmaps.graql.internal.gremlin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.graql.internal.gremlin.fragment.Fragment;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.joining;

/**
 * A traversal over a Mindmaps graph, representing one of many ways to execute a {@code MatchQuery}.
 * Comprised of ordered {@code Fragment}s which are used to construct a TinkerPop {@code GraphTraversal}, which can be
 * retrieved and executed.
 */
public class GraqlTraversal {

    //            Set of disjunctions
    //             |
    //             |           List of fragments in order of execution
    //             |            |
    //             V            V
    private final ImmutableSet<ImmutableList<Fragment>> fragments;
    private final MindmapsGraph graph;

    private GraqlTraversal(MindmapsGraph graph, ImmutableSet<ImmutableList<Fragment>> fragments) {
        this.graph = graph;
        this.fragments = fragments;
    }

    public static GraqlTraversal create(MindmapsGraph graph, ImmutableSet<ImmutableList<Fragment>> fragments) {
        return new GraqlTraversal(graph, fragments);
    }

    /**
     * Get the {@code GraphTraversal} that this {@code GraqlTraversal} represents.
     */
    GraphTraversal<Vertex, Map<String, Vertex>> getGraphTraversal() {
        Traversal[] traversals =
                fragments.stream().map(this::getConjunctionTraversal).toArray(Traversal[]::new);

        // Because 'union' accepts an array, we can't use generics...
        //noinspection unchecked
        return graph.getTinkerTraversal().limit(1).union(traversals);
    }

    /**
     * @return a gremlin traversal that represents this inner query
     */
    private GraphTraversal<Vertex, Map<String, Vertex>> getConjunctionTraversal(ImmutableList<Fragment> fragmentList) {
        GraphTraversal<Vertex, Vertex> traversal = graph.getTinkerTraversal();

        Set<String> foundNames = new HashSet<>();

        // Apply fragments in order into one single traversal
        String currentName = null;

        for (Fragment fragment : fragmentList) {
            applyFragment(fragment, traversal, currentName, foundNames);
            currentName = fragment.getEnd().orElse(fragment.getStart());
        }

        // Select all the variable names
        String[] traversalNames = foundNames.toArray(new String[foundNames.size()]);
        return traversal.select(traversalNames[0], traversalNames[0], traversalNames);
    }

    /**
     * Apply the given fragment to the traversal. Keeps track of variable names so far so that it can decide whether
     * to use "as" or "select" steps in gremlin.
     * @param fragment the fragment to apply to the traversal
     * @param traversal the gremlin traversal to apply the fragment to
     * @param currentName the variable name that the traversal is currently at
     * @param names a set of variable names so far encountered in the query
     */
    private void applyFragment(
            Fragment fragment, GraphTraversal<Vertex, Vertex> traversal, String currentName, Set<String> names
    ) {
        String start = fragment.getStart();

        if (currentName != null) {
            if (!currentName.equals(start)) {
                if (names.contains(start)) {
                    // If the variable name has been visited but the traversal is not at that variable name, select it
                    traversal.select(start);
                } else {
                    // Restart traversal when fragments are disconnected
                    traversal.V().as(start);
                }
            }
        } else {
            // If the variable name has not been visited yet, remember it and use the 'as' step
            traversal.as(start);
        }

        names.add(start);

        // Apply fragment to traversal
        fragment.applyTraversal(traversal);

        fragment.getEnd().ifPresent(end -> {
            if (!names.contains(end)) {
                // This variable name has not been encountered before, remember it and use the 'as' step
                names.add(end);
                traversal.as(end);
            } else {
                // This variable name has been encountered before, confirm it is the same
                traversal.where(P.eq(end));
            }
        });
    }

    /**
     * Get the estimated complexity of the traversal.
     */
    public long getComplexity() {
        return fragments.stream().mapToLong(list -> {
            long listCost = 0;
            String currentName = null;

            for (Fragment fragment : list) {
                if (fragment.getStart().equals(currentName)) {
                    listCost += fragment.fragmentCost(listCost);
                } else {
                    listCost += fragment.indexCost();
                }

                currentName = fragment.getEnd().orElse(fragment.getStart());
            }

            return listCost;
        }).sum();
    }

    @Override
    public String toString() {
        return "{" + fragments.stream().map(list -> {
            StringBuilder sb = new StringBuilder();
            String currentName = null;

            for (Fragment fragment : list) {
                if (!fragment.getStart().equals(currentName)) {
                    if (currentName != null) sb.append(" ");

                    sb.append("$").append(fragment.getStart());
                    currentName = fragment.getStart();
                }

                sb.append(fragment.getName());

                Optional<String> end = fragment.getEnd();
                if (end.isPresent()) {
                    sb.append("$").append(end.get());
                    currentName = end.get();
                }
            }

            return sb.toString();
        }).collect(joining(", ")) + "}";
    }
}
