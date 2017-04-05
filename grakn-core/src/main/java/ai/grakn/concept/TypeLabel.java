/*
 *  Grakn - A Distributed Semantic Database
 *  Copyright (C) 2016  Grakn Labs Limited
 *
 *  Grakn is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Grakn is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.concept;

import java.io.Serializable;
import java.util.function.Function;

/**
 * <p>
 *     A Type Label
 * </p>
 *
 * <p>
 *     A class which represents the unique label of any {@link Type} in the {@link ai.grakn.GraknGraph}.
 *     Also contains a static method for producing {@link TypeLabel}s from Strings.
 * </p>
 *
 * @author fppt
 */
public class TypeLabel implements Comparable<TypeLabel>, Serializable {
    private static final long serialVersionUID = 2051578406740868932L;

    private String label;
    private TypeLabel(String label){
        this.label = label;
    }

    public String getValue(){
        return label;
    }

    /**
     * Rename a {@link TypeLabel} (does not modify the original {@link TypeLabel})
     * @param mapper a function to apply to the underlying type label
     * @return the new type label
     */
    public TypeLabel map(Function<String, String> mapper) {
        return TypeLabel.of(mapper.apply(label));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TypeLabel typeLabel = (TypeLabel) o;

        return label.equals(typeLabel.label);
    }

    @Override
    public int hashCode() {
        return label.hashCode();
    }

    @Override
    public int compareTo(TypeLabel o) {
        return getValue().compareTo(o.getValue());
    }

    @Override
    public String toString(){
        return label;
    }

    /**
     *
     * @param value The string which potentially represents a Type
     * @return The matching Type Label
     */
    public static TypeLabel of(String value){
        return new TypeLabel(value);
    }
}
