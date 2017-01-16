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
 *     A Name
 * </p>
 *
 * <p>
 *     A class which represents the unique name of any {@link Type} in the {@link ai.grakn.GraknGraph}.
 *     Also contains a static method for producing TypeNames from Strings.
 * </p>
 *
 * @author fppt
 */
public class TypeName implements Comparable<TypeName>, Serializable {
    private String name;

    private TypeName(String name){
        this.name = name;
    }

    public String getValue(){
        return name;
    }

    /**
     * Rename a type name (does not modify the original {@code TypeName})
     * @param mapper a function to apply to the underlying type name
     * @return the new type name
     */
    public TypeName map(Function<String, String> mapper) {
        return TypeName.of(mapper.apply(name));
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof TypeName && ((TypeName) object).getValue().equals(name);
    }

    @Override
    public int compareTo(TypeName o) {
        return getValue().compareTo(o.getValue());
    }

    @Override
    public String toString(){
        return name;
    }

    @Override
    public int hashCode(){
        return name.hashCode();
    }

    /**
     *
     * @param value The string which potentially represents a Type
     * @return The matching Type Name
     */
    public static TypeName of(String value){
        return new TypeName(value);
    }
}
