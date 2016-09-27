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
import io.mindmaps.graql.internal.gremlin.*;
import io.mindmaps.graql.internal.util.StringConverter;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.Collection;

import static io.mindmaps.util.Schema.ConceptProperty.ITEM_IDENTIFIER;

public class IdProperty extends AbstractNamedProperty {

    private final String id;

    public IdProperty(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    protected String getName() {
        return "id";
    }

    @Override
    protected String getProperty() {
        return StringConverter.valueToString(id);
    }

    @Override
    public boolean supportShortcuts() {
        return false;
    }

    @Override
    public Collection<MultiTraversal> getMultiTraversal(String start) {
        Fragment fragment = new FragmentImpl(t -> t.has(ITEM_IDENTIFIER.name(), P.eq(id)), FragmentPriority.ID, start);
        MultiTraversalImpl multiTraversal = new MultiTraversalImpl(fragment);
        return Sets.newHashSet(multiTraversal);
    }
}
