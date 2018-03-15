/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.remote.concept;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.VarPattern;
import ai.grakn.grpc.ConceptMethod;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

import static ai.grakn.util.Schema.MetaSchema.THING;

/**
 * @author Felix Chapman
 *
 * @param <Self> The exact type of this class
 */
abstract class RemoteSchemaConcept<Self extends SchemaConcept> extends RemoteConcept<Self> implements SchemaConcept {

    public final Self sup(Self type) {
        return define(type, ME.sub(TARGET));
    }

    public final Self sub(Self type) {
        return define(type, TARGET.sub(ME));
    }

    @Override
    public final Label getLabel() {
        return runMethod(ConceptMethod.GET_LABEL);
    }

    @Override
    public final Boolean isImplicit() {
        return runMethod(ConceptMethod.IS_IMPLICIT);
    }

    @Override
    public final Self setLabel(Label label) {
        return define(ME.label(label));
    }

    @Nullable
    @Override
    public final Self sup() {
        Concept concept = runNullableMethod(ConceptMethod.GET_DIRECT_SUPER);
        if (concept != null && notMetaThing(concept)) {
            return asSelf(concept);
        } else {
            return null;
        }
    }

    @Override
    public final Stream<Self> sups() {
        return tx().admin().sups(this).filter(RemoteSchemaConcept::notMetaThing).map(this::asSelf);
    }

    private static boolean notMetaThing(Concept concept) {
        return !concept.isSchemaConcept() || !concept.asSchemaConcept().getLabel().equals(THING.getLabel());
    }

    @Override
    public final Stream<Self> subs() {
        return query(TARGET.sub(ME)).map(this::asSelf);
    }

    @Override
    public final LabelId getLabelId() {
        throw new UnsupportedOperationException(); // TODO: remove from API
    }

    @Override
    public final Stream<Rule> getRulesOfHypothesis() {
        throw new UnsupportedOperationException(); // TODO: remove from API
    }

    @Override
    public final Stream<Rule> getRulesOfConclusion() {
        throw new UnsupportedOperationException(); // TODO: remove from API
    }

    protected final Self define(Concept target, VarPattern... patterns) {
        return define(ImmutableList.<VarPattern>builder().add(TARGET.id(target.getId())).add(patterns).build());
    }

    protected final Self define(VarPattern... patterns) {
        return define(Arrays.asList(patterns));
    }

    private Self define(Collection<? extends VarPattern> patterns) {
        Collection<VarPattern> patternCollection =
                ImmutableList.<VarPattern>builder().add(me()).addAll(patterns).build();

        tx().graql().define(patternCollection).execute();
        return asSelf(this);
    }

    protected final Self undefine(Concept target, VarPattern... patterns) {
        return undefine(ImmutableList.<VarPattern>builder().add(TARGET.id(target.getId())).add(patterns).build());
    }

    protected final Self undefine(VarPattern... patterns) {
        return undefine(Arrays.asList(patterns));
    }

    private Self undefine(Collection<? extends VarPattern> patterns) {
        Collection<VarPattern> patternCollection =
                ImmutableList.<VarPattern>builder().add(me()).addAll(patterns).build();

        tx().graql().undefine(patternCollection).execute();
        return asSelf(this);
    }
}
