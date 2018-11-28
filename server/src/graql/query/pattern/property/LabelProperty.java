/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.query.pattern.property;

import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.util.StringUtil;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;

/**
 * Represents the {@code label} property on a {@link Type}.
 *
 * This property can be queried and inserted. If used in an insert query and there is an existing type with the give
 * label, then that type will be retrieved.
 *
 */
@AutoValue
public abstract class LabelProperty extends AbstractVarProperty implements VarPropertyInternal, VarProperty {

    public static final String NAME = "label";

    public static LabelProperty of(Label label) {
        return new AutoValue_LabelProperty(label);
    }

    public abstract Label label();

    @Override
    public boolean isUnique() {
        return true;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperty() {
        return StringUtil.typeLabelToString(label());
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Variable start) {
        return ImmutableSet.of(EquivalentFragmentSets.label(this, start, ImmutableSet.of(label())));
    }

    @Override
    public Collection<PropertyExecutor> insert(Variable var) throws GraqlQueryException {
        // This is supported in insert queries in order to allow looking up schema concepts by label
        return define(var);
    }

    @Override
    public Collection<PropertyExecutor> define(Variable var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            executor.builder(var).label(label());
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).produces(var).build());
    }

    @Override
    public Collection<PropertyExecutor> undefine(Variable var) throws GraqlQueryException {
        // This is supported in undefine queries in order to allow looking up schema concepts by label
        return define(var);
    }

    @Override
    public boolean uniquelyIdentifiesConcept() {
        return true;
    }

    @Override
    public Atomic mapToAtom(Statement var, Set<Statement> vars, ReasonerQuery parent) {
        SchemaConcept schemaConcept = parent.tx().getSchemaConcept(label());
        if (schemaConcept == null)  throw GraqlQueryException.labelNotFound(label());
        return IdPredicate.create(var.var().asUserDefined(), label(), parent);
    }
}
