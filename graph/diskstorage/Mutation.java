/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graph.diskstorage;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Container for collection mutations against a data store.
 * Mutations are either additions or deletions.
 */

public abstract class Mutation<E,K> {

    private List<E> additions;

    private List<K> deletions;

    public Mutation(List<E> additions, List<K> deletions) {
        Preconditions.checkNotNull(additions);
        Preconditions.checkNotNull(deletions);
        if (additions.isEmpty()) this.additions=null;
        else this.additions = Lists.newArrayList(additions);
        if (deletions.isEmpty()) this.deletions=null;
        else this.deletions = Lists.newArrayList(deletions);
    }

    public Mutation() {
        this.additions = null;
        this.deletions = null;
    }

    /**
     * Whether this mutation has additions
     */
    public boolean hasAdditions() {
        return additions!=null && !additions.isEmpty();
    }

    /**
     * Whether this mutation has deletions
     */
    public boolean hasDeletions() {
        return deletions != null && !deletions.isEmpty();
    }

    /**
     * Returns the list of additions in this mutation
     */
    public List<E> getAdditions() {
        if (additions==null) return ImmutableList.of();
        return additions;
    }

    /**
     * Returns the list of deletions in this mutation.
     */
    public List<K> getDeletions() {
        if (deletions==null) return ImmutableList.of();
        return deletions;
    }

    /**
     * Adds a new entry as an addition to this mutation
     */
    public void addition(E entry) {
        if (additions==null) additions = new ArrayList<>();
        additions.add(entry);
    }

    /**
     * Adds a new key as a deletion to this mutation
     */
    public void deletion(K key) {
        if (deletions==null) deletions = new ArrayList<>();
        deletions.add(key);
    }

    /**
     * Merges another mutation into this mutation. Ensures that all additions and deletions
     * are added to this mutation. Does not remove duplicates if such exist - this needs to be ensured by the caller.
     */
    public void merge(Mutation<E,K> m) {
        Preconditions.checkNotNull(m);

        if (null != m.additions) {
            if (null == additions) additions = m.additions;
            else additions.addAll(m.additions);
        }

        if (null != m.deletions) {
            if (null == deletions) deletions = m.deletions;
            else deletions.addAll(m.deletions);
        }
    }

    public boolean isEmpty() {
        return getTotalMutations()==0;
    }

    public int getTotalMutations() {
        return (additions==null?0:additions.size()) + (deletions==null?0:deletions.size());
    }

    /**
     * Consolidates this mutation by removing redundant deletions. A deletion is considered redundant if
     * it is identical to some addition under the provided conversion functions since we consider additions to apply logically after deletions.
     * Hence, such a deletion would be applied and immediately overwritten by an addition. To avoid this
     * inefficiency, consolidation should be called.
     * <p>
     * The provided conversion functions map additions and deletions into some object space V for comparison.
     * An addition is considered identical to a deletion if both map to the same object (i.e. equals=true) with the respective
     * conversion functions.
     * <p>
     * It needs to be ensured that V objects have valid hashCode() and equals() implementations.
     *
     * @param convertAdditions Function which maps additions onto comparison objects.
     * @param convertDeletions Function which maps deletions onto comparison objects.
     */
    public<V> void consolidate(Function<E,V> convertAdditions, Function<K,V> convertDeletions) {
        if (hasDeletions() && hasAdditions()) {
            Set<V> adds = Sets.newHashSet(Iterables.transform(additions,convertAdditions));
            deletions.removeIf(k -> adds.contains(convertDeletions.apply(k)));
        }
    }

    public abstract void consolidate();

    /**
     * Checks whether this mutation is consolidated in the sense of #consolidate(com.google.common.base.Function, com.google.common.base.Function).
     * This should only be used in assertions and tests due to the performance penalty.
     *
     */
    public<V> boolean isConsolidated(Function<E,V> convertAdditions, Function<K,V> convertDeletions) {
        int delBefore = getDeletions().size();
        consolidate(convertAdditions,convertDeletions);
        return getDeletions().size()==delBefore;
    }

    public abstract boolean isConsolidated();

}
