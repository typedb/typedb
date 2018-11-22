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

package grakn.core.graql.internal.reasoner.iterator;

import grakn.core.graql.admin.MultiUnifier;
import grakn.core.graql.answer.ConceptMap;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Specific iterator for iterating over graql answers.
 * </p>
 *
 *
 */
public class LazyAnswerIterator extends LazyIterator<ConceptMap> {

    public LazyAnswerIterator(Stream<ConceptMap> stream){ super(stream);}
    private LazyAnswerIterator(Iterator<ConceptMap> iterator){ super(iterator);}

    public LazyAnswerIterator unify(MultiUnifier unifier){
        if (unifier.isEmpty()) return this;
        return new LazyAnswerIterator(stream().flatMap(a -> a.unify(unifier)).iterator());
    }

    public LazyAnswerIterator merge (Stream<ConceptMap> stream){
        return new LazyAnswerIterator(Stream.concat(this.stream(), stream));
    }
}
