/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.graql.internal.reasoner.iterator;

import ai.grakn.concept.Concept;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.reasoner.query.QueryAnswers;
import com.google.common.collect.Iterators;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Specific iterator for iterating over graql answers.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class LazyAnswerIterator extends LazyIterator<Map<VarName, Concept>> {

    public LazyAnswerIterator(Stream<Map<VarName, Concept>> stream){ super(stream);}
    private LazyAnswerIterator(Iterator<Map<VarName, Concept>> iterator){ super(iterator);}

    public LazyAnswerIterator unify(Map<VarName, VarName> unifiers){
        if (unifiers.isEmpty()) return this;
        Iterator<Map<VarName, Concept>> transform = Iterators.transform(iterator(), input -> QueryAnswers.unify(input, unifiers));
        return new LazyAnswerIterator(transform);
    }

    public LazyAnswerIterator merge (Stream<Map<VarName, Concept>> stream){
        return new LazyAnswerIterator(Stream.concat(this.stream(), stream));
    }
    public LazyAnswerIterator merge (LazyAnswerIterator iter) {
        return new LazyAnswerIterator(Stream.concat(this.stream(), iter.stream()));
    }
}
