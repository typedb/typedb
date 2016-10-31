package io.mindmaps.graql.internal.printer;

import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Type;
import io.mindmaps.graql.Printer;
import io.mindmaps.graql.internal.util.ANSI;
import io.mindmaps.graql.internal.util.StringConverter;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static io.mindmaps.graql.internal.query.match.MatchQueryInternal.colorKeyword;
import static io.mindmaps.graql.internal.query.match.MatchQueryInternal.colorType;

/**
 * Default printer that prints results in Graql syntax
 */
class GraqlPrinter implements Printer {

    @Override
    public StringBuilder graqlString(StringBuilder sb, boolean inner, Concept concept) {
        // Display values for resources and ids for everything else
        if (concept.isResource()) {
            sb.append(colorKeyword("value "));
            sb.append(StringConverter.valueToString(concept.asResource().getValue()));
        } else {
            sb.append(colorKeyword("id "));
            sb.append("\"").append(StringConverter.escapeString(concept.getId())).append("\"");
        }

        // Display type of each concept
        Type type = concept.type();
        if (type != null) {
            sb.append(colorKeyword(" isa ")).append(colorType(StringConverter.idToString(type.getId())));
        }

        // Display lhs and rhs for rules
        if (concept.isRule()) {
            sb.append(colorKeyword(" lhs ")).append("{ ").append(concept.asRule().getLHS()).append(" }");
            sb.append(colorKeyword(" rhs ")).append("{ ").append(concept.asRule().getRHS()).append(" }");
        }

        return sb;
    }

    @Override
    public StringBuilder graqlString(StringBuilder sb, boolean inner, boolean bool) {
        if (bool) {
            return sb.append(ANSI.color("True", ANSI.GREEN));
        } else {
            return sb.append(ANSI.color("False", ANSI.RED));
        }
    }

    @Override
    public StringBuilder graqlString(StringBuilder sb, boolean inner, Optional<?> optional) {
        if (optional.isPresent()) {
            return graqlString(sb, inner, optional.get());
        } else {
            return sb.append("Nothing");
        }
    }

    @Override
    public StringBuilder graqlString(StringBuilder sb, boolean inner, Collection<?> collection) {
        if (inner) {
            sb.append("{");
            collection.stream().findFirst().ifPresent(item -> graqlString(sb, true, item));
            collection.stream().skip(1).forEach(item -> graqlString(sb.append(", "), true, item));
            sb.append("}");
        } else {
            collection.forEach(item -> graqlString(sb, true, item).append("\n"));
        }

        return sb;
    }

    @Override
    public StringBuilder graqlString(StringBuilder sb, boolean inner, Map<?, ?> map) {
        if (!map.entrySet().isEmpty()) {
            Map.Entry<?, ?> entry = map.entrySet().iterator().next();

            // If this looks like a graql result, assume the key is a variable name
            if (entry.getKey() instanceof String && entry.getValue() instanceof Concept) {
                map.forEach((name, concept) ->
                        sb.append("$").append(name).append(" ").append(graqlString(concept)).append("; ")
                );
                return sb;
            }
        }

        return graqlString(sb, inner, map.entrySet());
    }

    @Override
    public StringBuilder graqlString(StringBuilder sb, boolean inner, Map.Entry<?, ?> entry) {
        graqlString(sb, true, entry.getKey()).append(":\t");
        graqlString(sb, true, entry.getValue());
        return sb;
    }

    @Override
    public StringBuilder graqlStringDefault(StringBuilder sb, boolean inner, Object object) {
        return sb.append(object.toString());
    }
}
