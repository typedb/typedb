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
import io.mindmaps.concept.Resource;
import io.mindmaps.graql.admin.ValuePredicateAdmin;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.gremlin.FragmentImpl;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.gremlin.MultiTraversalImpl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;

import static io.mindmaps.graql.internal.gremlin.FragmentPriority.EDGE_UNBOUNDED;
import static io.mindmaps.graql.internal.util.CommonUtil.tryAny;
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
        } else {
            resourceRepr = tryAny(resource.getValuePredicates()).map(predicate -> " " + predicate).orElse("");
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

    @Override
    public void deleteProperty(MindmapsGraph graph, Concept concept) {
        Optional<ValuePredicateAdmin> predicate = resource.getValuePredicates().stream().findAny();

        resources(concept).stream()
                .filter(r -> r.type().getId().equals(resourceType))
                .filter(r -> predicate.map(p -> p.getPredicate().test(r.getValue())).orElse(true))
                .forEach(Concept::delete);
    }

    /**
     * @return all resources on the given concept
     */
    private Collection<Resource<?>> resources(Concept concept) {
        // Get resources attached to a concept
        // This method is necessary because the 'resource' method appears in 3 separate interfaces
        if (concept.isEntity()) {
            return concept.asEntity().resources();
        } else if (concept.isRelation()) {
            return concept.asRelation().resources();
        } else if (concept.isRule()) {
            return concept.asRule().resources();
        } else {
            return new HashSet<>();
        }
    }
}
