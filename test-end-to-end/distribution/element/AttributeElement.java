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

package grakn.core.distribution.element;

import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Variable;

import java.util.Objects;

public class AttributeElement implements Element{
    private final String type;
    private final Object value;

    public AttributeElement(String type, Object val){
        this.type = type;
        this.value = val;
    }

    public String getType(){ return type;}
    public Object getValue(){ return value;}

    public String index(){
        String index;
        if (value instanceof String) index = type + "-" + ((String) value).replace("\"", "'");
        else index = type + "-" + value;
        return index;
    }

    public static String index(String label, String value){
        return label + "-" + value;
    }

    @Override
    public String toString(){ return getType() + ": " + getValue();}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AttributeElement attribute = (AttributeElement) o;
        return type.equals(attribute.type) &&
                value.equals(attribute.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }

    @Override
    public int conceptSize() { return 1; }

    @Override
    public Pattern patternise() {
        return patternise(Graql.var().var().asReturnedVar());
    }

    @Override
    public Pattern patternise(Variable var) {
        Object value = getValue();
        Pattern pattern;
        if (value instanceof String) {
            value = ((String) value).replace("\"", "'");
            pattern = Graql.parsePattern(var + " \"" + value + "\" isa " + type + ";");
        } else {
            pattern = Graql.parsePattern(var + " " + value + " isa " + type + ";");
        }
        return pattern;
    }
}