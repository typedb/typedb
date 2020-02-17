/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graql.reasoner.operator;

import com.google.common.collect.Sets;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.property.RelationProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public class RemoveRoleplayerOperator implements Operator {

    @Override
    public Stream<Pattern> apply(Pattern src, TypeContext ctx) {
        if (!src.statements().stream().flatMap(s -> s.getProperties(RelationProperty.class)).findFirst().isPresent()){
            return Stream.of(src);
        }

        List<Set<Statement>> transformedStatements = src.statements().stream()
                .map(this::transformStatement)
                .collect(Collectors.toList());
        return Sets.cartesianProduct(transformedStatements).stream()
                .map(Graql::and)
                .filter(p -> !p.equals(src))
                .map(p -> Graql.and(
                        p.statements().stream()
                                .filter(st -> !st.properties().isEmpty())
                                .collect(toSet())
                        )
                );
    }

    private Set<Statement> transformStatement(Statement src){
        Variable var = src.var();
        RelationProperty relProperty = src.getProperty(RelationProperty.class).orElse(null);
        if (relProperty == null) return Sets.newHashSet(src);

        Set<Optional<RelationProperty>> transformedRelationsProps = transformRelationProperty(relProperty);

        Set<Statement> transformedStatements = Sets.newHashSet(src);
        transformedRelationsProps.stream()
                .map(o -> {
                    LinkedHashSet<VarProperty> properties = new LinkedHashSet<>(src.properties());
                    properties.remove(relProperty);
                    o.ifPresent(properties::add);
                    return Statement.create(var, properties);
                })
                .forEach(transformedStatements::add);

        return transformedStatements;
    }

    private Set<Optional<RelationProperty>> transformRelationProperty(RelationProperty prop){
        List<Set<Optional<RelationProperty.RolePlayer>>> rPconfigurations = new ArrayList<>();

        prop.relationPlayers().forEach(rp -> {
            Set<Optional<RelationProperty.RolePlayer>> rps = Sets.newHashSet(Optional.of(rp));
            rps.add(Optional.empty());
            rPconfigurations.add(rps);
        });

        return Sets.cartesianProduct(rPconfigurations).stream()
                .map(rpSet -> rpSet.stream().map(o -> o.orElse(null)).filter(Objects::nonNull).collect(toSet()))
                .map(rpSet -> Optional.ofNullable(Utils.relationProperty(rpSet)))
                .collect(toSet());
    }
}
