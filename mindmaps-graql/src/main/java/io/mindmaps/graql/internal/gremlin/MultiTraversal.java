package io.mindmaps.graql.internal.gremlin;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

/**
 * a pattern to match in the graph. comprised of {@code Fragments}, each describing one way to represent the traversal,
 * starting from different variables.
 * <p>
 * A {@code MultiTraversal} may contain only one {@code Fragment} (e.g. checking the 'id' property), while others may
 * be comprised of two fragments (e.g. $x isa $y, which may start from $x or $y).
 */
public class MultiTraversal {

    private final Collection<Fragment> fragments;

    /**
     * @param fragments an array of Fragments that this MultiTraversal contains
     */
    public MultiTraversal(Fragment... fragments) {
        this.fragments = Arrays.asList(fragments);
        this.fragments.forEach(f -> f.setMultiTraversal(this));
    }

    /**
     * @return a stream of fragments that this MultiTraversal contains
     */
    public Stream<Fragment> getFragments() {
        return fragments.stream();
    }
}
