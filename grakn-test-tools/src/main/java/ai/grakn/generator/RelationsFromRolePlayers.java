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

package ai.grakn.generator;

import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * A generator that produces {@link Relation}s from existing role-players.
 *
 * This means the relation is navigated to from another {@link Thing} attached to it. This will find relations even
 * if they are not returned by {@link RelationType#instances}.
 *
 * @author Felix Chapman
 */
public class RelationsFromRolePlayers extends FromGraphGenerator<Relation> {

    public RelationsFromRolePlayers() {
        super(Relation.class);
    }

    @Override
    protected Relation generateFromGraph() {
        Stream<? extends Thing> things = ((Type) graph().admin().getMetaConcept()).instances();

        Optional<Relation> relation = things.flatMap(thing -> thing.relations().stream()).findAny();

        if (relation.isPresent()) {
            return relation.get();
        } else {
            // Give up and fall back to normal generator
            return genFromGraph(Relations.class).generate(random, status);
        }
    }
}
