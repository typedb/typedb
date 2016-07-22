/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.mindmaps.core.implementation.DataType.EdgeProperty.*;
import static io.mindmaps.core.implementation.DataType.EdgeLabel.SHORTCUT;

/**
 * some {@code VarTraversals} can be represented using shortcut edges.
 * {@code ShortcutTraversal} represents this pattern in order to improve query performance
 * <p>
 * A {@code VarTraversals} can be represented by a shortcut edge when:
 * <ul>
 *     <li>it represents a relation</li>
 *     <li>the relation has no internal properties or resources specified (id, value ...)</li>
 *     <li>the relation has exactly two roleplayers</li>
 *     <li>if the type and role-types are specified, they are specified only by id and no other properties</li>
 *     <li>the relation has not been given a variable name</li>
 * </ul>
 */
class ShortcutTraversal {

    private final List<Optional<String>> roletypes = new ArrayList<>();
    private final List<String> roleplayers = new ArrayList<>();
    private boolean valid = true;
    private Optional<String> type = Optional.empty();
    private MultiTraversal multiTraversal = null;

    /**
     * @return true if a shortcut edge can be used in the traversal
     */
    public boolean isValid() {
        return valid && (roleplayers.size() == 2);
    }

    /**
     * Make this ShortcutTraversal invalid, so it will not be used in the traversal
     */
    public void setInvalid() {
        valid = false;
    }

    /**
     * @return a MultiTraversal that follows shortcut edges
     */
    public MultiTraversal getMultiTraversal() {
        if (multiTraversal == null) makeMultiTraversal();
        return multiTraversal;
    }

    /**
     * Create a MultiTraversal that follows shortcut edges
     */
    private void makeMultiTraversal() {
        Optional<String> roleA = roletypes.get(0);
        String playerA = roleplayers.get(0);
        Optional<String> roleB = roletypes.get(1);
        String playerB = roleplayers.get(1);

        multiTraversal = new MultiTraversal(
                new Fragment(t -> makeTraversal(t, roleA, roleB), FragmentPriority.EDGE_RELATION, playerA, playerB),
                new Fragment(t -> makeTraversal(t, roleB, roleA), FragmentPriority.EDGE_RELATION, playerB, playerA)
        );
    }

    /**
     * @param traversal the traversal to start from
     * @param roleA the role type of A, if one is specified
     * @param roleB the role type of B, if one is specified
     * @return a traversal following a shortcut edge from A to B using the given roles
     */
    private GraphTraversal<Vertex, Vertex> makeTraversal(
            GraphTraversal<Vertex, Vertex> traversal, Optional<String> roleA, Optional<String> roleB
    ) {
        GraphTraversal<Vertex, Edge> edgeTraversal = traversal.outE(SHORTCUT.getLabel());
        roleA.ifPresent(ra -> edgeTraversal.has(FROM_ROLE.name(), ra));
        roleB.ifPresent(rb -> edgeTraversal.has(TO_ROLE.name(), rb));
        type.ifPresent(t -> edgeTraversal.has(RELATION_ID.name(), t));
        return edgeTraversal.inV();
    }

    /**
     * @param type the type of the variable this ShortcutTraversal represents
     */
    public void setType(String type) {
        if (!this.type.isPresent()) {
            this.type = Optional.of(type);
        } else {
            setInvalid();
        }
    }

    /**
     * @param roleplayer a roleplayer of the relation that this ShortcutTraversal represents
     */
    public void addRel(String roleplayer) {
        roletypes.add(Optional.empty());
        roleplayers.add(roleplayer);
    }

    /**
     * @param roletype the role type of the given roleplayer
     * @param roleplayer a roleplayer of the relation that this ShortcutTraversal represents
     */
    public void addRel(String roletype, String roleplayer) {
        roletypes.add(Optional.of(roletype));
        roleplayers.add(roleplayer);
    }
}
