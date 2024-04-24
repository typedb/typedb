/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.scanner.RelationIterator;
import com.vaticle.typedb.core.traversal.structure.StructureVertex;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class RelationTraversal extends Traversal {

    private final Identifier.Variable.Retrievable relation;
    private final Set<Identifier.Variable.Retrievable> players;
    private int relationPlayers;

    public RelationTraversal(Identifier.Variable.Retrievable relation, Set<Label> types) {
        super();
        this.relation = relation;
        this.structure.thingVertex(relation).props().types(types);
        this.players = new HashSet<>();
    }

    @Override
    FunctionalIterator<VertexMap> permutationIterator(GraphManager graphMgr) {
        return new RelationIterator(this, graphMgr);
    }

    public void player(Identifier.Variable.Retrievable thing, ByteArray iid, Set<Label> roleTypes) {
        VertexIID.Thing vertexIID = VertexIID.Thing.of(iid);
        if (parameters.getIID(thing) == null) {
            players.add(thing);
            structure.thingVertex(thing).props().hasIID(true);
            parameters.putIID(thing, vertexIID);
        } else assert parameters.getIID(thing).equals(vertexIID);
        structure.rolePlayer(structure.thingVertex(relation), structure.thingVertex(thing), roleTypes, relationPlayers);
        relationPlayers++;
    }

    public Identifier.Variable.Retrievable relationIdentifier() {
        return relation;
    }

    public StructureVertex.Thing relationVertex() {
        return structure.thingVertex(relation);
    }

    public Set<Identifier.Variable.Retrievable> players() {
        return players;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelationTraversal that = (RelationTraversal) o;
        return (this.structure.equals(that.structure) && this.parameters.equals(that.parameters));
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.structure, this.parameters);
    }
}
