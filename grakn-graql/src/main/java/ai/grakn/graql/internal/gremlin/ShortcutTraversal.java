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

package ai.grakn.graql.internal.gremlin;

import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
 *
 * @author Felix Chapman
 */
public class ShortcutTraversal {

    private final List<Optional<TypeLabel>> roletypes = new ArrayList<>();
    private final List<VarName> roleplayers = new ArrayList<>();
    private boolean valid = true;
    private Optional<TypeLabel> type = Optional.empty();
    private EquivalentFragmentSet equivalentFragmentSet = null;

    /**
     * @return true if a shortcut edge can be used in the traversal
     */
    boolean isValid() {
        return valid && (roleplayers.size() == 2);
    }

    /**
     * Make this ShortcutTraversal invalid, so it will not be used in the traversal
     */
    public void setInvalid() {
        valid = false;
    }

    /**
     * @return a EquivalentFragmentSet that follows shortcut edges
     */
    EquivalentFragmentSet getEquivalentFragmentSet() {
        if (equivalentFragmentSet == null) {
            equivalentFragmentSet = makeEquivalentFragmentSet();
        }
        return equivalentFragmentSet;
    }

    /**
     * Create a EquivalentFragmentSet that follows shortcut edges
     */
    private EquivalentFragmentSet makeEquivalentFragmentSet() {
        Optional<TypeLabel> roleA = roletypes.get(0);
        VarName playerA = roleplayers.get(0);
        Optional<TypeLabel> roleB = roletypes.get(1);
        VarName playerB = roleplayers.get(1);

        return EquivalentFragmentSets.shortcut(roleA, playerA, roleB, playerB, type);
    }

    /**
     * @param type the type of the variable this ShortcutTraversal represents
     */
    public void setType(TypeLabel type) {
        if (!this.type.isPresent()) {
            this.type = Optional.of(type);
        } else {
            setInvalid();
        }
    }

    /**
     * @param roleplayer a roleplayer of the relation that this ShortcutTraversal represents
     */
    public void addRel(VarName roleplayer) {
        roletypes.add(Optional.empty());
        roleplayers.add(roleplayer);
    }

    /**
     * @param roletype the role type of the given roleplayer
     * @param roleplayer a roleplayer of the relation that this ShortcutTraversal represents
     */
    public void addRel(TypeLabel roletype, VarName roleplayer) {
        roletypes.add(Optional.of(roletype));
        roleplayers.add(roleplayer);
    }
}
