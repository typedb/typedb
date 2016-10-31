package io.mindmaps.graql;

import io.mindmaps.concept.Concept;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Interface describing a way to print Graql objects.
 */
public interface Printer {

    default String graqlString(Object object) {
        StringBuilder sb = new StringBuilder();
        graqlString(sb, false, object);
        return sb.toString();
    }

    default StringBuilder graqlString(StringBuilder sb, boolean inner, Object object) {
        if (object instanceof Concept) {
            graqlString(sb, inner, (Concept) object);
        } else if (object instanceof Boolean) {
            graqlString(sb, inner, (boolean) object);
        } else if (object instanceof Optional) {
            graqlString(sb, inner, (Optional<?>) object);
        } else if (object instanceof Collection) {
            graqlString(sb, inner, (Collection<?>) object);
        } else if (object instanceof Map) {
            graqlString(sb, inner, (Map<?, ?>) object);
        } else if (object instanceof Map.Entry) {
            graqlString(sb, inner, (Map.Entry<?, ?>) object);
        } else {
            graqlStringDefault(sb, inner, object);
        }

        return sb;
    }

    StringBuilder graqlString(StringBuilder sb, boolean inner, Concept concept);

    StringBuilder graqlString(StringBuilder sb, boolean inner, boolean bool);

    StringBuilder graqlString(StringBuilder sb, boolean inner, Optional<?> optional);

    StringBuilder graqlString(StringBuilder sb, boolean inner, Collection<?> collection);

    StringBuilder graqlString(StringBuilder sb, boolean inner, Map<?, ?> map);

    StringBuilder graqlString(StringBuilder sb, boolean inner, Map.Entry<?, ?> entry);

    StringBuilder graqlStringDefault(StringBuilder sb, boolean inner, Object object);
}
