/*
 *  GRAKN.AI - THE KNOWLEDGE GRAPH
 *  Copyright (C) 2018 Grakn Labs Ltd
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.antlr.GraqlParser;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.query.executor.ConceptBuilder;
import ai.grakn.graql.internal.reasoner.atom.binary.SubAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getIdPredicate;

/**
 * Represents the {@code sub!} property on a {@link ai.grakn.concept.Type}.
 *
 * This property can be queried or inserted.
 *
 * This property relates a {@link ai.grakn.concept.Type} and another {@link ai.grakn.concept.Type}. It indicates
 * that every instance of the left type is also an instance of the right type.
 * When matching, subtyping is ignored:
 * if
 * `man sub person`,
 * `person sub entity`,
 * then
 * `$x sub! entity` only returns `person` but not `man`
 *
 * @author Joshua Send
 */
@AutoValue
public abstract class SubExplicitProperty extends AbstractSubProperty implements NamedProperty, UniqueVarProperty {

    public static final String NAME = "sub!";

    public static SubExplicitProperty of(VarPatternAdmin superType) {
        return new AutoValue_SubExplicitProperty(superType);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(EquivalentFragmentSets.sub(this, start, superType().var(), true));
    }
}
