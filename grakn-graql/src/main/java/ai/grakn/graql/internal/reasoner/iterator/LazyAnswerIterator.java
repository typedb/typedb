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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.iterator;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MultiUnifier;

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

    public LazyAnswerIterator unify(MultiUnifier unifier){
        if (unifier.isEmpty()) return this;
        return new LazyAnswerIterator(stream().flatMap(a -> a.unify(unifier)).iterator());
    }

    public LazyAnswerIterator merge (Stream<Answer> stream){
        return new LazyAnswerIterator(Stream.concat(this.stream(), stream));
    }
}
