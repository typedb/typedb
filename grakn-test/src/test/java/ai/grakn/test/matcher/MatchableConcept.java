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
 *
 */

package ai.grakn.test.matcher;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Resource;
import ai.grakn.concept.TypeName;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Optional;

import static ai.grakn.graql.internal.util.StringConverter.typeNameToString;

/**
 * Wraps a {@link Concept} in order to provide a prettier {@link Object#toString()} representation. This is done using
 * {@link MatchableConcept#NAME_TYPES}, a hard-coded set of common 'name' variables such as "name" and "title'.
 */
class MatchableConcept {

    static final ImmutableSet<TypeName> NAME_TYPES = ImmutableSet.of(TypeName.of("name"), TypeName.of("title"));

    private final Concept concept;

    MatchableConcept(Concept concept) {
        this.concept = concept;
    }

    Concept get() {
        return concept;
    }

    @Override
    public String toString() {
        if (concept.isInstance()) {

            Collection<Resource<?>> resources = concept.asInstance().resources();
            Optional<?> value = resources.stream()
                    .filter(resource -> NAME_TYPES.contains(resource.type().getName()))
                    .map(Resource::getValue).findFirst();

            return "instance(" + value.map(StringConverter::valueToString).orElse("") + ")";
        } else {
            return "type(" + typeNameToString(concept.asType().getName()) + ")";
        }
    }
}
