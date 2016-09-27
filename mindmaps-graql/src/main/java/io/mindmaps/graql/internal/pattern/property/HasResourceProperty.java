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
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.gremlin.FragmentImpl;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.gremlin.MultiTraversalImpl;

import java.util.Collection;

import static io.mindmaps.graql.internal.gremlin.FragmentPriority.EDGE_UNBOUNDED;
import static io.mindmaps.util.Schema.EdgeLabel.SHORTCUT;
import static io.mindmaps.util.Schema.EdgeProperty.TO_TYPE;

public class HasResourceProperty implements NamedProperty {

    private final String resourceType;
    private final VarAdmin resource;

    public HasResourceProperty(String resourceType, VarAdmin resource) {
        this.resourceType = resourceType;
        this.resource = resource.isa(resourceType).admin();
    }

    public String getType() {
        return resourceType;
    }

    public VarAdmin getResource() {
        return resource;
    }

    @Override
    public String getName() {
        return "has";
    }

    @Override
    public String getProperty() {
        String resourceRepr;
        if (resource.isUserDefinedName()) {
            resourceRepr = " " + resource.getPrintableName();
        } else if (resource.hasNoProperties()) {
            resourceRepr = "";
        } else {
            resourceRepr = " " + resource.getValuePredicates().iterator().next().toString();
        }
        return resourceType + resourceRepr;
    }

    @Override
    public Collection<MultiTraversal> getMultiTraversals(String start) {
        return Sets.newHashSet(new MultiTraversalImpl(
                new FragmentImpl(t ->
                        t.outE(SHORTCUT.getLabel()).has(TO_TYPE.name(), resourceType).inV(),
                        EDGE_UNBOUNDED, start, resource.getName()
                ),
                new FragmentImpl(t ->
                        t.inE(SHORTCUT.getLabel()).has(TO_TYPE.name(), resourceType).outV(),
                        EDGE_UNBOUNDED, resource.getName(), start
                )
        ));
    }

    @Override
    public Collection<VarAdmin> getInnerVars() {
        return Sets.newHashSet(resource);
    }
}
