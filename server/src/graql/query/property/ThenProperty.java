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

package grakn.core.graql.query.property;

import grakn.core.graql.query.Token;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.statement.StatementType;

import static java.util.stream.Collectors.joining;

/**
 * Represents the {@code then} (right-hand side) property on a rule.
 * This property can be inserted and not queried.
 * The then side describes the right-hand of an implication, stating that when the when side of a rule is
 * true the then side must hold.
 */
public class ThenProperty extends VarProperty {

    private final Pattern pattern;

    public ThenProperty(Pattern pattern) {
        if (pattern == null) {
            throw new NullPointerException("Null pattern");
        }
        this.pattern = pattern;
    }

    public Pattern pattern() {
        return pattern;
    }

    @Override
    public String keyword() {
        return Token.Property.THEN.toString();
    }

    @Override @SuppressWarnings("Duplicates")
    public String property() {
        StringBuilder then = new StringBuilder();

        then.append(Token.Char.CURLY_OPEN).append(Token.Char.SPACE);

        if (pattern instanceof Conjunction) {
            then.append(((Conjunction<?>) pattern).getPatterns().stream()
                                .map(Object::toString)
                                .collect(joining(Token.Char.SPACE.toString())));
        } else {
            then.append(pattern.toString());
        }

        then.append(Token.Char.SPACE).append(Token.Char.CURLY_CLOSE);
        return then.toString();
    }

    @Override
    public boolean isUnique() {
        return true;
    }

    @Override
    public Class statementClass() {
        return StatementType.class;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof ThenProperty) {
            ThenProperty that = (ThenProperty) o;
            return (this.pattern.equals(that.pattern()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.pattern.hashCode();
        return h;
    }
}
