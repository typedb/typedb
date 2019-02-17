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

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * An object that contains the answer of every Graql Query.
 */
public abstract class Answer {

    /**
     * @return an explanation object indicating how this answer was obtained
     */
    @Nullable
    @CheckReturnValue
    public abstract Explanation explanation();

    /**
     * @return all explanations taking part in the derivation of this answer
     */
    @CheckReturnValue
    public Set<Explanation> explanations() {
        if (this.explanation() == null) return Collections.emptySet();
        Set<Explanation> explanations = new HashSet<>();
        explanations.add(this.explanation());
        this.explanation().getAnswers().forEach(ans -> explanations.addAll(ans.explanations()));
        return explanations;
    }
}
