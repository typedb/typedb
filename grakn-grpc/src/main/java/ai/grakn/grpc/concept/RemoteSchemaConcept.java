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

package ai.grakn.grpc.concept;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * @author Felix Chapman
 */
@AutoValue
public abstract class RemoteSchemaConcept extends RemoteConcept implements SchemaConcept {

    public static RemoteSchemaConcept create(GraknTx tx, ConceptId id, Label label, boolean isImplicit) {
        return new AutoValue_RemoteSchemaConcept(tx, id, label, isImplicit);
    }

    @Override
    public abstract Label getLabel();

    @Override
    public abstract Boolean isImplicit();

    @Override
    public SchemaConcept setLabel(Label label) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public SchemaConcept sup() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public Stream<? extends SchemaConcept> sups() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public Stream<? extends SchemaConcept> subs() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public LabelId getLabelId() {
        throw new UnsupportedOperationException(); // TODO: remove from API
    }

    @Override
    public Stream<Rule> getRulesOfHypothesis() {
        throw new UnsupportedOperationException(); // TODO: remove from API
    }

    @Override
    public Stream<Rule> getRulesOfConclusion() {
        throw new UnsupportedOperationException(); // TODO: remove from API
    }
}
