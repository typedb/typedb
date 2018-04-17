/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.matcher;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.Thing;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.StringUtil;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.stream.Stream;

import static ai.grakn.util.StringUtil.valueToString;

/**
 * Wraps a {@link Concept} in order to provide a prettier {@link Object#toString()} representation. This is done using
 * {@link MatchableConcept#NAME_TYPES}, a hard-coded set of common 'name' variables such as "name" and "title'.
 *
 * @author Felix Chapman
 */
public class MatchableConcept {

    static final ImmutableSet<Label> NAME_TYPES = ImmutableSet.of(Label.of("name"), Label.of("title"));

    private final Concept concept;

    private MatchableConcept(Concept concept) {
        this.concept = concept;
    }

    public static MatchableConcept of(Concept concept) {
        return new MatchableConcept(concept);
    }

    Concept get() {
        return concept;
    }

    @Override
    public String toString() {
        if (concept.isAttribute()) {
            return "hasValue(" + valueToString(concept.asAttribute().getValue()) + ")";
        } else if (concept.isThing()) {
            Thing thing = concept.asThing();
            Stream<Attribute<?>> resources = thing.attributes();
            Optional<?> value = resources
                    .filter(resource -> NAME_TYPES.contains(resource.type().getLabel()))
                    .map(Attribute::getValue).findFirst();

            return "instance(" + value.map(StringUtil::valueToString).orElse("") + ") isa " + thing.type().getLabel();
        } else if (concept.isType()) {
            return "type(" + concept.asType().getLabel() + ")";
        } else if (concept.isRole()) {
            return "role(" + concept.asRole().getLabel() + ")";
        } else if (concept.isRule()) {
            return "rule(" + concept.asRule().getLabel() + ")";
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised concept " + concept);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MatchableConcept that = (MatchableConcept) o;

        return concept.equals(that.concept);
    }

    @Override
    public int hashCode() {
        return concept.hashCode();
    }
}
