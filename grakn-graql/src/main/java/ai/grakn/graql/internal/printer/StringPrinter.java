/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.printer;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.util.ANSI;
import ai.grakn.util.StringUtil;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.util.StringConverter.idToString;
import static ai.grakn.graql.internal.util.StringConverter.typeLabelToString;

/**
 * Default printer that prints results in Graql syntax
 *
 * @author Grakn Warriors
 */
class StringPrinter extends Printer<StringBuilder> {

    private final AttributeType[] attributeTypes;
    private final boolean colorize;

    StringPrinter(boolean colorize, AttributeType... attributeTypes) {
        this.colorize = colorize;
        this.attributeTypes = attributeTypes;
    }

    @Override
    protected String complete(StringBuilder output) {
        return output.toString();
    }

    @Override
    protected StringBuilder concept(Concept concept) {
        StringBuilder output = new StringBuilder();

        // Display values for resources and ids for everything else
        if (concept.isAttribute()) {
            output.append(colorKeyword("val ")).append(StringUtil.valueToString(concept.asAttribute().value()));
        } else if (concept.isSchemaConcept()) {
            SchemaConcept ontoConcept = concept.asSchemaConcept();
            output.append(colorKeyword("label ")).append(colorType(ontoConcept));

            SchemaConcept superConcept = ontoConcept.sup();

            if (superConcept != null) {
                output.append(colorKeyword(" sub ")).append(colorType(superConcept));
            }
        } else {
            output.append(colorKeyword("id ")).append(idToString(concept.id()));
        }

        if (concept.isRelationship()) {
            List<String> rolePlayerList = new LinkedList<>();
            for (Map.Entry<Role, Set<Thing>> rolePlayers : concept.asRelationship().rolePlayersMap().entrySet()) {
                Role role = rolePlayers.getKey();
                Set<Thing> things = rolePlayers.getValue();

                for (Thing thing : things) {
                    rolePlayerList.add(colorType(role) + ": id " + idToString(thing.id()));
                }
            }

            String relationString = rolePlayerList.stream().collect(Collectors.joining(", "));
            output.append(" (").append(relationString).append(")");
        }

        // Display type of each instance
        if (concept.isThing()) {
            Type type = concept.asThing().type();
            output.append(colorKeyword(" isa ")).append(colorType(type));
        }

        // Display when and then for rules
        if (concept.isRule()) {
            output.append(colorKeyword(" when ")).append("{ ").append(concept.asRule().when()).append(" }");
            output.append(colorKeyword(" then ")).append("{ ").append(concept.asRule().then()).append(" }");
        }

        // Display any requested resources
        if (concept.isThing() && attributeTypes.length > 0) {
            concept.asThing().attributes(attributeTypes).forEach(resource -> {
                String resourceType = colorType(resource.type());
                String value = StringUtil.valueToString(resource.value());
                output.append(colorKeyword(" has ")).append(resourceType).append(" ").append(value);
            });
        }

        return output;
    }

    @Override
    protected StringBuilder bool(boolean bool) {
        StringBuilder builder = new StringBuilder();

        if (bool) {
            return builder.append(ANSI.color("True", ANSI.GREEN));
        } else {
            return builder.append(ANSI.color("False", ANSI.RED));
        }
    }

    @Override
    protected StringBuilder collection(Collection<?> collection) {
        StringBuilder builder = new StringBuilder();

        builder.append("{");
        collection.stream().findFirst().ifPresent(item -> builder.append(build(item)));
        collection.stream().skip(1).forEach(item -> builder.append(",").append(build(item)));
        builder.append("}");

        return builder;
    }

    @Override
    protected StringBuilder map(Map<?, ?> map) {
        return collection(map.entrySet());
    }

    @Override
    public StringBuilder queryAnswer(Answer answer) {
        StringBuilder builder = new StringBuilder();

        if (answer.isEmpty()) builder.append("{}");
        else answer.forEach((name, concept) -> builder.append(name).append(" ").append(concept(concept)).append("; "));

        return builder;
    }

    //TODO: implement StringPrinter for ComputeAnswer properly!
    @Override
    protected StringBuilder computeAnswer(ComputeQuery.Answer computeAnswer) {
        StringBuilder builder = new StringBuilder();

        if (computeAnswer.getNumber().isPresent()) {
            builder.append(computeAnswer.getNumber().get());
        }
        else if (computeAnswer.getCentrality().isPresent()) {
            builder.append(map(computeAnswer.getCentrality().get()));
        }
        else if (computeAnswer.getClusters().isPresent()) {
            builder.append(collection(computeAnswer.getClusters().get()));
        }
        else if (computeAnswer.getClusterSizes().isPresent()) {
            builder.append(collection(computeAnswer.getClusterSizes().get()));
        }
        else if (computeAnswer.getPaths().isPresent()) {
            builder.append(collection(computeAnswer.getPaths().get()));
        }

        return builder;
    }

    @Override
    protected StringBuilder object(Object object) {
        StringBuilder builder = new StringBuilder();

        if (object instanceof Map.Entry<?, ?>) {
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) object;

            builder.append(build(entry.getKey()));
            builder.append(": ");
            builder.append(build(entry.getValue()));
        } else if (object != null) {
            builder.append(Objects.toString(object));
        }

        return builder;
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
     * @param schemaConcept a type to color-code using ANSI colors
     * @return the type, color-coded
     */
    private String colorType(SchemaConcept schemaConcept) {
        if(colorize) {
            return ANSI.color(typeLabelToString(schemaConcept.label()), ANSI.PURPLE);
        } else {
            return typeLabelToString(schemaConcept.label());
        }
    }
}
