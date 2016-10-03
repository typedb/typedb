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
import io.mindmaps.concept.Concept;
import io.mindmaps.graql.admin.UniqueVarProperty;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.gremlin.Fragment;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.gremlin.Traversals;
import io.mindmaps.graql.internal.query.InsertQueryExecutor;
import io.mindmaps.util.ErrorMessage;

import java.util.Collection;
import java.util.stream.Stream;

import static io.mindmaps.graql.internal.gremlin.FragmentPriority.EDGE_BOUNDED;
import static io.mindmaps.graql.internal.gremlin.FragmentPriority.EDGE_UNIQUE;

public class AkoProperty extends AbstractVarProperty implements NamedProperty, UniqueVarProperty {

    private final VarAdmin superType;

    public AkoProperty(VarAdmin superType) {
        this.superType = superType;
    }

    public VarAdmin getSuperType() {
        return superType;
    }

    @Override
    public String getName() {
        return "ako";
    }

    @Override
    public String getProperty() {
        return superType.getPrintableName();
    }

    @Override
    public Collection<MultiTraversal> matchProperty(String start) {
        return Sets.newHashSet(MultiTraversal.create(
                Fragment.create(Traversals::outAkos, EDGE_UNIQUE, start, superType.getName()),
                Fragment.create(Traversals::inAkos, EDGE_BOUNDED, superType.getName(), start)
        ));
    }

    @Override
    public Stream<VarAdmin> getTypes() {
        return Stream.of(superType);
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return Stream.of(superType);
    }

    @Override
    public void insertProperty(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
        Concept superConcept = insertQueryExecutor.getConcept(superType);

        if (concept.isEntityType()) {
            concept.asEntityType().superType(superConcept.asEntityType());
        } else if (concept.isRelationType()) {
            concept.asRelationType().superType(superConcept.asRelationType());
        } else if (concept.isRoleType()) {
            concept.asRoleType().superType(superConcept.asRoleType());
        } else if (concept.isResourceType()) {
            concept.asResourceType().superType(superConcept.asResourceType());
        } else if (concept.isRuleType()) {
            concept.asRuleType().superType(superConcept.asRuleType());
        } else {
            throw new IllegalStateException(ErrorMessage.INSERT_METATYPE.getMessage(concept.getId(), superType.getId()));
        }
    }
}
