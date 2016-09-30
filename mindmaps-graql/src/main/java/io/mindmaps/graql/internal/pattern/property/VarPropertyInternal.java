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

package io.mindmaps.graql.internal.pattern.property;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.admin.VarProperty;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.gremlin.ShortcutTraversal;

import java.util.Collection;

import static io.mindmaps.graql.internal.pattern.property.VarProperties.failDelete;

public interface VarPropertyInternal extends VarProperty {

    default void modifyShortcutTraversal(ShortcutTraversal shortcutTraversal) {
        shortcutTraversal.setInvalid();
    }

    Collection<MultiTraversal> getMultiTraversals(String start);

    @Override
    default Collection<VarAdmin> getInnerVars() {
        return Sets.newHashSet();
    }

    @Override
    default void deleteProperty(MindmapsGraph graph, Concept concept) {
        throw failDelete(this);
    }
}
