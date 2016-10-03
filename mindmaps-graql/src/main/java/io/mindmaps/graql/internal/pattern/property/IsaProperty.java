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
import io.mindmaps.concept.Type;
import io.mindmaps.graql.admin.UniqueVarProperty;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.gremlin.Fragment;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.gremlin.ShortcutTraversal;
import io.mindmaps.util.ErrorMessage;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

import static io.mindmaps.graql.internal.gremlin.FragmentPriority.EDGE_UNBOUNDED;
import static io.mindmaps.graql.internal.gremlin.FragmentPriority.EDGE_UNIQUE;
import static io.mindmaps.graql.internal.gremlin.Traversals.inAkos;
import static io.mindmaps.graql.internal.gremlin.Traversals.outAkos;
import static io.mindmaps.util.Schema.EdgeLabel.ISA;

public class IsaProperty extends AbstractVarProperty implements UniqueVarProperty, NamedProperty {

    private final VarAdmin type;

    public IsaProperty(VarAdmin type) {
        this.type = type;
    }

    public VarAdmin getType() {
        return type;
    }

    @Override
    public String getName() {
        return "isa";
    }

    @Override
    public String getProperty() {
        return type.getPrintableName();
    }

    @Override
    public void modifyShortcutTraversal(ShortcutTraversal shortcutTraversal) {
        Optional<String> id = type.getId();
        if (id.isPresent()){
            shortcutTraversal.setType(id.get());
        } else {
            shortcutTraversal.setInvalid();
        }
    }

    @Override
    public Collection<MultiTraversal> matchProperty(String start) {
        return Sets.newHashSet(MultiTraversal.create(
                Fragment.create(
                        t -> outAkos(outAkos(t).out(ISA.getLabel())),
                        EDGE_UNIQUE, start, type.getName()
                ),
                Fragment.create(
                        t -> inAkos(inAkos(t).in(ISA.getLabel())),
                        EDGE_UNBOUNDED, type.getName(), start
                )
        ));
    }

    @Override
    public Stream<VarAdmin> getTypes() {
        return Stream.of(type);
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return Stream.of(type);
    }

    @Override
    public void checkValidProperty(MindmapsGraph graph, VarAdmin var) throws IllegalStateException {
        type.getIdOnly().map(graph::getType).filter(Type::isRoleType).ifPresent(type -> {
            throw new IllegalStateException(ErrorMessage.INSTANCE_OF_ROLE_TYPE.getMessage(type.getId()));
        });
    }
}
