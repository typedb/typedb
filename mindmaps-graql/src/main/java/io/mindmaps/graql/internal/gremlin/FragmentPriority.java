package io.mindmaps.graql.internal.gremlin;

import io.mindmaps.core.implementation.DataType;
import io.mindmaps.graql.api.query.ValuePredicate;

/**
 * represents the priority of a {@code Fragment}.
 * <p>
 * Higher priority patterns are expected to be more efficient and better filter the query and are executed first.
 */
public enum FragmentPriority {

    /**
     * Looking up instances by ID (not types), this is extremely fast due to indices
     */
    ID,

    /**
     * Looking up things by value. Values are non-unique so this is only slightly slower than lookup by ID
     */
    VALUE_SPECIFIC,

    /**
     * Looking up types by ID. This is lower priority due to a limitation of the system for sorting fragments.
     * Because types are often super-nodes, it is usually better to start from an instance (looked up by ID or value)
     * than from a type if possible.
     */
    TYPE_ID,

    /**
     * Looking up things by non-specific values. This includes predicates (e.g. 'value > 100') and very short strings,
     * which are assumed to be common.
     */
    VALUE_NONSPECIFIC,

    /**
     * Moving along an edge, where a concept can be expected to have at most one (e.g. ISA, AKO)
     */
    EDGE_UNIQUE,

    /**
     * Moving along an edge, where a concept can be expected to have a small number bounded by the ontology
     * (e.g. PLAYS_ROLE, HAS_ROLE)
     */
    EDGE_BOUNDED,

    /**
     * Moving along a relation edge, where a concept can be expected to have a large number of relations.
     */
    EDGE_RELATION,

    /**
     * Moving along an edge that a concept may have a HUGE number of, e.g. all instances of a type
     */
    EDGE_UNBOUNDED,

    /**
     * Confirming that castings are all distinct
     */
    // TODO: Should this have higher priority?
    DISTINCT_CASTING;

    /**
     * @param edgeLabel the edge label to get the priority of moving along
     * @param out whether we are going outbound on this edge
     * @return the priority of travelling along the given edge in the given direction in a gremlin traversal
     */
    public static FragmentPriority getEdgePriority(DataType.EdgeLabel edgeLabel, boolean out) {
        switch (edgeLabel) {
            case ISA:
                return out ? EDGE_UNIQUE : EDGE_UNBOUNDED;
            case AKO:
                return out ? EDGE_UNIQUE : EDGE_BOUNDED;
            case HAS_ROLE:
                return out ? EDGE_BOUNDED : EDGE_UNIQUE;
            case PLAYS_ROLE:
                return EDGE_BOUNDED;
            case HAS_SCOPE:
                return out ? EDGE_BOUNDED : EDGE_UNBOUNDED;
            case CASTING:
                return out ? EDGE_BOUNDED : EDGE_UNBOUNDED;
            default:
                return null;
        }
    }

    /**
     * @param predicate the predicate to get the priority of checking
     * @return the priority of checking the given value predicate in a gremlin traversal
     */
    public static FragmentPriority getValuePriority(ValuePredicate.Admin predicate) {
        if (predicate.isSpecific()) {
            return VALUE_SPECIFIC;
        } else {
            return VALUE_NONSPECIFIC;
        }
    }
}
