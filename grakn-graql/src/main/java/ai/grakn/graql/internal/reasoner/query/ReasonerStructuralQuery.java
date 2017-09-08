/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.graql.admin.Atomic;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 *
 * <p>
 * TODO
 * </p>
 *
 * @param <Q>
 *
 * @author Kasper Piskorski
 *
 */
public class ReasonerStructuralQuery<Q extends ReasonerQueryImpl> {

    private final Q query;

    public ReasonerStructuralQuery(Q q){
        this.query = q;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        ReasonerStructuralQuery a2 = (ReasonerStructuralQuery) obj;
        return query.isEquivalent(a2.query, Atomic::isStructurallyEquivalent);
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        SortedSet<Integer> hashes = new TreeSet<>();
        query.getAtoms().forEach(atom -> hashes.add(atom.structuralEquivalenceHashCode()));
        for (Integer hash : hashes) hashCode = hashCode * 37 + hash;
        return hashCode;
    }

}
