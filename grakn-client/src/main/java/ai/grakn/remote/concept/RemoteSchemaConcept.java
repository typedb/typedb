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

import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;

import java.util.stream.Stream;

/**
 * @author Felix Chapman
 *
 * @param <Self> The exact type of this class
 */
abstract class RemoteSchemaConcept<Self extends SchemaConcept> extends RemoteConcept implements SchemaConcept {

    @Override
    public final Label getLabel() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Boolean isImplicit() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Self setLabel(Label label) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Self sup() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Stream<Self> sups() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Stream<Self> subs() {
        throw new UnsupportedOperationException(); // TODO: implement
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
}
