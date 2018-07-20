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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.answer;

import ai.grakn.graql.admin.Explanation;

/**
 * A type of {@link Answer} object that contains a {@link Number}.
 */
public class Numeric implements Answer<Numeric>{

    private final Number value;
    private final Explanation explanation;

    public Numeric(Number value) {
        this(value, null);
    }

    public Numeric(Number value, Explanation explanation) {
        this.value = value;
        this.explanation = explanation;
    }


    @Override
    public Numeric get() {
        return this;
    }

    @Override
    public Explanation explanation() {
        return explanation;
    }

    public Number number() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Numeric a2 = (Numeric) obj;
        return this.value.toString().equals(a2.value.toString());
    }

    @Override
    public int hashCode(){
        return value.hashCode();
    }
}
