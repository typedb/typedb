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
