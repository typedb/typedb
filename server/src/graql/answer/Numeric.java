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

import grakn.core.graql.internal.reasoner.explanation.QueryExplanation;

/**
 * A type of Answer object that contains a Number.
 */
public class Numeric extends Answer {

    private final Number number;
    private final Explanation explanation;

    public Numeric(Number number) {
        this(number, new QueryExplanation());
    }

    public Numeric(Number number, Explanation explanation) {
        this.number = number;
        this.explanation = explanation;
    }

    @Override
    public Explanation explanation() {
        return explanation;
    }

    public Number number() {
        return number;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Numeric a2 = (Numeric) obj;
        return this.number.toString().equals(a2.number.toString());
    }

    @Override
    public int hashCode() {
        return number.hashCode();
    }
}
