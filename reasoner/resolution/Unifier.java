/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.reasoner.resolution;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.Rule;
import grakn.core.logic.concludable.ConjunctionConcludable;
import grakn.core.logic.concludable.HeadConcludable;

public class Unifier implements ConceptMapTransformer {

    private final ConjunctionConcludable<?, ?> fromConcludable;
    private final HeadConcludable<?, ?> toConcludable;
    private Rule rule;

    public Unifier(ConjunctionConcludable<?, ?> fromConcludable, HeadConcludable<?, ?> toConcludable, Rule rule) {
        this.fromConcludable = fromConcludable;
        this.toConcludable = toConcludable;
        this.rule = rule;
    }

    public ConjunctionConcludable<?, ?> fromConcludable() {
        return fromConcludable;
    }

    public HeadConcludable<?, ?> toConcludable() {
        return toConcludable;
    }

    public Rule rule() {
        return rule;
    }

    public static Unifier identity() {
        return null; // TODO A unifier that performs trivial mapping - unneeded now, remove.
    }

    @Override
    public ConceptMap transform(ConceptMap conceptMap) {
        return null; // TODO
    }

    @Override
    public ConceptMap unTransform(ConceptMap unified) {
        return null; // TODO
    }
}
