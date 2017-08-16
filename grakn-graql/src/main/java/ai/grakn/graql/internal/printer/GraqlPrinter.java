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
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.graql.Printer;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.util.ANSI;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.StringUtil;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.util.StringConverter.idToString;
import static ai.grakn.graql.internal.util.StringConverter.typeLabelToString;

/**
 * Default printer that prints results in Graql syntax
 */
class GraqlPrinter implements Printer<Function<StringBuilder, StringBuilder>> {

    private final AttributeType[] attributeTypes;
    private final boolean colorize;

    GraqlPrinter(boolean colorize, AttributeType... attributeTypes) {
        this.colorize = colorize;
        this.attributeTypes = attributeTypes;
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
                sb.append(colorKeyword("val ")).append(StringUtil.valueToString(concept.asResource().getValue()));
            } else if (concept.isOntologyConcept()) {
                OntologyConcept ontoConcept = concept.asOntologyConcept();
                sb.append(colorKeyword("label ")).append(colorType(ontoConcept));

                OntologyConcept superConcept = ontoConcept.sup();

                if (superConcept != null) {
                    sb.append(colorKeyword(" sub ")).append(colorType(superConcept));
                }
            } else {
                sb.append(colorKeyword("id ")).append(idToString(concept.getId()));
            }

            if (concept.isRelation()) {
                String relationString = concept.asRelation().allRolePlayers().entrySet().stream().flatMap(entry -> {
                    Role role = entry.getKey();
                    Set<Thing> things = entry.getValue();

                    return things.stream().map(instance ->
                        Optional.of(colorType(role) + ": id " + idToString(instance.getId()))
                    );
                }).flatMap(CommonUtil::optionalToStream).collect(Collectors.joining(", "));

                sb.append(" (").append(relationString).append(")");
            }

            // Display type of each instance
            if (concept.isThing()) {
                Type type = concept.asThing().type();
                sb.append(colorKeyword(" isa ")).append(colorType(type));
            }

            // Display when and then for rules
            if (concept.isRule()) {
                sb.append(colorKeyword(" when ")).append("{ ").append(concept.asRule().getWhen()).append(" }");
                sb.append(colorKeyword(" then ")).append("{ ").append(concept.asRule().getThen()).append(" }");
            }

            // Display any requested resources
            if (concept.isThing() && attributeTypes.length > 0) {
                concept.asThing().resources(attributeTypes).forEach(resource -> {
                    String resourceType = colorType(resource.type());
                    String value = StringUtil.valueToString(resource.getValue());
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
        return graqlString(inner, map.entrySet());
    }

    @Override
    public Function<StringBuilder, StringBuilder> graqlString(boolean inner, Answer answer) {
        return sb -> {
            if (answer.isEmpty()) {
                sb.append("{}");
            } else {
                answer.forEach((name, concept) ->
                        sb.append(name).append(" ").append(graqlString(concept)).append("; ")
                );
            }
            return sb;
        };
    }

    @Override
    public Function<StringBuilder, StringBuilder> graqlStringDefault(boolean inner, Object object) {
        if (object instanceof Map.Entry<?, ?>) {
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) object;
            return graqlString(true, entry.getKey())
                    .andThen(sb -> sb.append(": "))
                    .andThen(graqlString(true, entry.getValue()));
        } else {
            return sb -> sb.append(Objects.toString(object));
        }
    }


    /**
     * Color-codes the keyword if colorization enabled
     * @param keyword a keyword to color-code using ANSI colors
     * @return the keyword, color-coded
     */
    private String colorKeyword(String keyword) {
        if(colorize) {
            return ANSI.color(keyword, ANSI.BLUE);
        } else {
            return keyword;
        }
    }

    /**
     * Color-codes the given type if colorization enabled
     * @param ontologyConcept a type to color-code using ANSI colors
     * @return the type, color-coded
     */
    private String colorType(OntologyConcept ontologyConcept) {
        if(colorize) {
            return ANSI.color(typeLabelToString(ontologyConcept.getLabel()), ANSI.PURPLE);
        } else {
            return typeLabelToString(ontologyConcept.getLabel());
        }
    }
}
