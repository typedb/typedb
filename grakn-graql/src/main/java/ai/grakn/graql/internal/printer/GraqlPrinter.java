/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.printer;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.graql.Printer;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.util.ANSI;
import ai.grakn.graql.internal.util.CommonUtil;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.util.StringConverter.idToString;
import static ai.grakn.graql.internal.util.StringConverter.typeLabelToString;
import static ai.grakn.graql.internal.util.StringConverter.valueToString;

/**
 * Default printer that prints results in Graql syntax
 */
class GraqlPrinter implements Printer<Function<StringBuilder, StringBuilder>> {

    /**
     * @param keyword a keyword to color-code using ANSI colors
     * @return the keyword, color-coded
     */
    private static String colorKeyword(String keyword) {
        return ANSI.color(keyword, ANSI.BLUE);
    }

    /**
     * @param type a type to color-code using ANSI colors
     * @return the type, color-coded
     */
    private static String colorType(Type type) {
        return ANSI.color(typeLabelToString(type.getLabel()), ANSI.PURPLE);
    }

    private final ResourceType[] resourceTypes;

    GraqlPrinter(ResourceType... resourceTypes) {
        this.resourceTypes = resourceTypes;
    }

    @Override
    public String build(Function<StringBuilder, StringBuilder> builder) {
        return builder.apply(new StringBuilder()).toString();
    }

    @Override
    public Function<StringBuilder, StringBuilder> graqlString(boolean inner, Concept concept) {
        return sb -> {
            // Display values for resources and ids for everything else
            if (concept.isResource()) {
                sb.append(colorKeyword("value ")).append(valueToString(concept.asResource().getValue()));
            } else if (concept.isType()) {
                Type type = concept.asType();
                sb.append(colorKeyword("label ")).append(colorType(type));

                Type superType = type.superType();

                if (superType != null) {
                    sb.append(colorKeyword(" sub ")).append(colorType(superType));
                }
            } else {
                sb.append(colorKeyword("id ")).append(idToString(concept.getId()));
            }

            if (concept.isRelation()) {
                String relationString = concept.asRelation().allRolePlayers().entrySet().stream().flatMap(entry -> {
                    RoleType roleType = entry.getKey();
                    Set<Instance> instances = entry.getValue();

                    return instances.stream().map(instance ->
                        Optional.of(colorType(roleType) + ": id " + idToString(instance.getId()))
                    );
                }).flatMap(CommonUtil::optionalToStream).collect(Collectors.joining(", "));

                sb.append(" (").append(relationString).append(")");
            }

            // Display type of each instance
            if (concept.isInstance()) {
                Type type = concept.asInstance().type();
                sb.append(colorKeyword(" isa ")).append(colorType(type));
            }

            // Display lhs and rhs for rules
            if (concept.isRule()) {
                sb.append(colorKeyword(" lhs ")).append("{ ").append(concept.asRule().getLHS()).append(" }");
                sb.append(colorKeyword(" rhs ")).append("{ ").append(concept.asRule().getRHS()).append(" }");
            }

            // Display any requested resources
            if (concept.isInstance() && resourceTypes.length > 0) {
                concept.asInstance().resources(resourceTypes).forEach(resource -> {
                    String resourceType = colorType(resource.type());
                    String value = valueToString(resource.getValue());
                    sb.append(colorKeyword(" has ")).append(resourceType).append(" ").append(value);
                });
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
                collection.stream().skip(1).forEach(item -> graqlString(true, item).apply(sb.append(", ")));
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
            if (entry.getKey() instanceof VarName && entry.getValue() instanceof Concept) {
                return sb -> {
                    map.forEach((name, concept) ->
                            sb.append(name).append(" ").append(graqlString(concept)).append("; ")
                    );
                    return sb;
                };
            } else {
                return graqlString(inner, map.entrySet());
            }
        } else {
            return sb -> sb.append("{}");
        }
    }

    @Override
    public Function<StringBuilder, StringBuilder> graqlStringDefault(boolean inner, Object object) {
        if (object instanceof Map.Entry<?, ?>) {
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) object;
            return graqlString(true, entry.getKey())
                    .andThen(sb -> sb.append(": "))
                    .andThen(graqlString(true, entry.getValue()));
        } else {
            return sb -> sb.append(object.toString());
        }
    }
}
