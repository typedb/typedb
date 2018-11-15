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

package grakn.core.graql.answer;

import grakn.core.graql.concept.Concept;
import grakn.core.graql.admin.Explanation;

import javax.annotation.Nullable;
import java.util.List;

/**
 * A type of {@link Answer} object that contains a {@link List} of {@link Answer}s as the members and a {@link Concept}
 * as the owner.
 * @param <T> the type of {@link Answer} being grouped
 */
public class AnswerGroup<T extends Answer> implements Answer<AnswerGroup<T>> {

    private final Concept owner;
    private final List<T> answers;
    private final Explanation explanation;

    public AnswerGroup(Concept owner, List<T> answers) {
        this(owner, answers, null);
    }

    public AnswerGroup(Concept owner, List<T> answers, Explanation explanation) {
        this.owner = owner;
        this.answers = answers;
        this.explanation = explanation;
    }


    @Override
    public AnswerGroup<T> asAnswerGroup() {
        return this;
    }

    @Nullable
    @Override
    public Explanation explanation() {
        return explanation;
    }

    public Concept owner() {
        return this.owner;
    }

    public List<T> answers() {
        return this.answers;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        AnswerGroup a2 = (AnswerGroup) obj;
        return this.owner.equals(a2.owner) &&
                this.answers.equals(a2.answers);
    }

    @Override
    public int hashCode(){
        int hash = owner.hashCode();
        hash = 31 * hash + answers.hashCode();

        return hash;
    }
}
