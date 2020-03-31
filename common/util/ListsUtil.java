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
 *
 */


package grakn.core.common.util;

import java.util.ArrayList;
import java.util.List;

public class ListsUtil {

    /**
     * @param a subtraction left operand
     * @param b subtraction right operand
     * @param <T> collection type
     * @return new Collection containing a minus a - b.
     * The cardinality of each element e in the returned Collection will be the cardinality of e in a minus the cardinality of e in b, or zero, whichever is greater.
     */
    public static <T> List<T> listDifference(List<T> a, List<T> b){
        ArrayList<T> list = new ArrayList<>(a);
        b.forEach(list::remove);
        return list;
    }

    /**
     *
     * @param a union left operand
     * @param b union right operand
     * @param <T> list type
     * @return new list being a union of the two operands
     */
    public static <T> List<T> listUnion(List<T> a, List<T> b){
        List<T> union = new ArrayList<>(a);
        union.addAll(b);
        return union;
    }
}
