/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concept.answer;

import com.vaticle.typedb.core.concept.Concept;

import java.util.List;
import java.util.Objects;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class ConceptMapGroup {
    private final Concept owner;
    private final List<? extends ConceptMap> conceptMaps;
    private final int hash;

    public ConceptMapGroup(Concept owner, List<? extends ConceptMap> conceptMaps) {
        this.owner = owner;
        this.conceptMaps = conceptMaps;
        this.hash = Objects.hash(this.owner, this.conceptMaps);
    }

    public Concept owner() {
        return this.owner;
    }

    public List<? extends ConceptMap> conceptMaps() {
        return this.conceptMaps;
    }

    @Override
    public String toString() {
        return owner + ":[" + String.join(", ", iterate(conceptMaps).map(ConceptMap::toString).toList()) + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ConceptMapGroup a2 = (ConceptMapGroup) obj;
        return this.owner.equals(a2.owner) &&
                this.conceptMaps.equals(a2.conceptMaps);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
