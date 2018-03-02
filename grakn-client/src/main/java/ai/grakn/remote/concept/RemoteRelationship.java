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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.remote.concept;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.grpc.ConceptProperty;
import ai.grakn.remote.RemoteGraknTx;
import com.google.auto.value.AutoValue;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.toSet;

/**
 * @author Felix Chapman
 */
@AutoValue
abstract class RemoteRelationship extends RemoteThing<Relationship, RelationshipType> implements Relationship {

    private static final Var ROLE = var("role");

    public static RemoteRelationship create(RemoteGraknTx tx, ConceptId id) {
        return new AutoValue_RemoteRelationship(tx, id);
    }

    @Override
    public final Map<Role, Set<Thing>> allRolePlayers() {
        return getProperty(ConceptProperty.ALL_ROLE_PLAYERS);
    }

    @Override
    public final Stream<Thing> rolePlayers(Role... roles) {
        Stream<Concept> concepts;
        if (roles.length != 0) {
            Set<VarPattern> patterns = Stream.of(roles).map(role -> ROLE.label(role.getLabel())).collect(toSet());
            concepts = query(ME.rel(ROLE, TARGET), or(patterns));
        } else {
            concepts = query(ME.rel(TARGET));
        }
        return concepts.map(Concept::asThing);
    }

    @Override
    public final Relationship addRolePlayer(Role role, Thing thing) {
        insert(ROLE.id(role.getId()), TARGET.id(thing.getId()), ME.rel(ROLE, TARGET));
        return asSelf(this);
    }

    @Override
    public final void removeRolePlayer(Role role, Thing thing) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    final RelationshipType asMyType(Concept concept) {
        return concept.asRelationshipType();
    }

    @Override
    final Relationship asSelf(Concept concept) {
        return concept.asRelationship();
    }
}
