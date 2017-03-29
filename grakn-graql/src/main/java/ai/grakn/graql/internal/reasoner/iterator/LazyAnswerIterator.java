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

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.admin.Unifier;
import com.google.common.collect.Iterators;
import java.util.Iterator;
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
public class LazyAnswerIterator extends LazyIterator<Answer> {

    public LazyAnswerIterator(Stream<Answer> stream){ super(stream);}
    private LazyAnswerIterator(Iterator<Answer> iterator){ super(iterator);}

    public LazyAnswerIterator unify(Unifier unifier){
        if (unifier.isEmpty()) return this;
        Iterator<Answer> transform = Iterators.transform(iterator(), input -> {
            if (input == null) return null;
            return input.unify(unifier);
        });
        return new LazyAnswerIterator(transform);
    }

    public LazyAnswerIterator explain(AnswerExplanation exp){
        Iterator<Answer> transform = Iterators.transform(iterator(), input -> {
            if (input == null) return null;
            if (input.getExplanation() == null || input.getExplanation().isLookupExplanation()){
                input.explain(exp);
            } else {
                input.getExplanation().setQuery(exp.getQuery());
            }
            return input;
        });
        return new LazyAnswerIterator(transform);
    }

    public LazyAnswerIterator merge (Stream<Answer> stream){
        return new LazyAnswerIterator(Stream.concat(this.stream(), stream));
    }
    public LazyAnswerIterator merge (LazyAnswerIterator iter) {
        return new LazyAnswerIterator(Stream.concat(this.stream(), iter.stream()));
    }
}
