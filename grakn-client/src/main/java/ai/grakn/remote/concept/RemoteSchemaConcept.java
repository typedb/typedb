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
import ai.grakn.grpc.ConceptMethod;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * @author Felix Chapman
 *
 * @param <Self> The exact type of this class
 */
abstract class RemoteSchemaConcept<Self extends SchemaConcept> extends RemoteConcept<Self> implements SchemaConcept {

    public final Self sup(Self type) {
        return runVoidMethod(ConceptMethod.setDirectSuperConcept(type));
    }

    public final Self sub(Self type) {
        tx().client().runConceptMethod(type.getId(), ConceptMethod.setDirectSuperConcept(this));
        return asSelf(this);
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
        return runVoidMethod(ConceptMethod.setLabel(label));
    }

    @Nullable
    @Override
    public final Self sup() {
        Optional<Concept> concept = runMethod(ConceptMethod.GET_DIRECT_SUPER);
        return concept.filter(this::isSelf).map(this::asSelf).orElse(null);
    }

    @Override
    public final Stream<Self> sups() {
        return tx().admin().sups(this).filter(this::isSelf).map(this::asSelf);
    }

    @Override
    public final Stream<Self> subs() {
        return runMethod(ConceptMethod.GET_SUB_CONCEPTS).map(this::asSelf);
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

    abstract boolean isSelf(Concept concept);
}
