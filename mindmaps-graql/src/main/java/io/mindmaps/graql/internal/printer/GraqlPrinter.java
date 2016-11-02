package io.mindmaps.graql.internal.printer;

import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Type;
import io.mindmaps.graql.Printer;
import io.mindmaps.graql.internal.util.ANSI;
import io.mindmaps.graql.internal.util.StringConverter;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static io.mindmaps.graql.internal.query.match.MatchQueryInternal.colorKeyword;
import static io.mindmaps.graql.internal.query.match.MatchQueryInternal.colorType;

/**
 * Default printer that prints results in Graql syntax
 */
class GraqlPrinter implements Printer<Function<StringBuilder, StringBuilder>> {

    @Override
    public String build(Function<StringBuilder, StringBuilder> builder) {
        return builder.apply(new StringBuilder()).toString();
    }

    @Override
    public Function<StringBuilder, StringBuilder> graqlString(boolean inner, Concept concept) {
        return sb -> {
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
        };
    }

    @Override
    public Function<StringBuilder, StringBuilder> graqlString(boolean inner, boolean bool) {
        if (bool) {
            return sb -> sb.append(ANSI.color("True", ANSI.GREEN));
        } else {
            return sb -> sb.append(ANSI.color("False", ANSI.RED));
        }
    }

    @Override
    public Function<StringBuilder, StringBuilder> graqlString(boolean inner, Optional<?> optional) {
        if (optional.isPresent()) {
            return graqlString(inner, optional.get());
        } else {
            return sb -> sb.append("Nothing");
        }
    }

    @Override
    public Function<StringBuilder, StringBuilder> graqlString(boolean inner, Collection<?> collection) {
        return sb -> {
            if (inner) {
                sb.append("{");
                collection.stream().findFirst().ifPresent(item -> graqlString(true, item).apply(sb));
                collection.stream().skip(1).forEach(item -> graqlString(true, item).apply(sb));
                sb.append("}");
            } else {
                collection.forEach(item -> graqlString(true, item).apply(sb).append("\n"));
            }

            return sb;
        };
    }

    @Override
    public Function<StringBuilder, StringBuilder> graqlString(boolean inner, Map<?, ?> map) {
        if (!map.entrySet().isEmpty()) {
            Map.Entry<?, ?> entry = map.entrySet().iterator().next();

            // If this looks like a graql result, assume the key is a variable name
            if (entry.getKey() instanceof String && entry.getValue() instanceof Concept) {
                return sb -> {
                    map.forEach((name, concept) ->
                            sb.append("$").append(name).append(" ").append(graqlString(concept)).append("; ")
                    );
                    return sb;
                };
            }
        }

        return graqlString(inner, map.entrySet());
    }

    @Override
    public Function<StringBuilder, StringBuilder> graqlStringDefault(boolean inner, Object object) {
        if (object instanceof Map.Entry<?, ?>) {
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) object;
            return graqlString(true, entry.getKey())
                    .andThen(sb -> sb.append(":\t"))
                    .andThen(graqlString(true, entry.getValue()));
        } else {
            return sb -> sb.append(object.toString());
        }
    }
}
