package io.mindmaps.graql;

import io.mindmaps.concept.Concept;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Interface describing a way to print Graql objects.
 */
public interface Printer<T> {

    default String graqlString(Object object) {
        T builder = graqlString(false, object);
        return build(builder);
    }

    default T graqlString(boolean inner, Object object) {
        if (object instanceof Concept) {
            return graqlString(inner, (Concept) object);
        } else if (object instanceof Boolean) {
            return graqlString(inner, (boolean) object);
        } else if (object instanceof Optional) {
            return graqlString(inner, (Optional<?>) object);
        } else if (object instanceof Collection) {
            return graqlString(inner, (Collection<?>) object);
        } else if (object instanceof Map) {
            return graqlString(inner, (Map<?, ?>) object);
        } else {
            return graqlStringDefault(inner, object);
        }
    }

    String build(T builder);

    T graqlString(boolean inner, Concept concept);

    T graqlString(boolean inner, boolean bool);

    T graqlString(boolean inner, Optional<?> optional);

    T graqlString(boolean inner, Collection<?> collection);

    T graqlString(boolean inner, Map<?, ?> map);

    T graqlStringDefault(boolean inner, Object object);
}
