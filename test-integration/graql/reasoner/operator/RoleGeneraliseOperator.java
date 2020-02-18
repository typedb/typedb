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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Generates a set of generalised patterns by removing generalising roles within relations.
 * The set is computed from a Cartesian product of sets of statements each containing a single role generalisation.
 */
public class RoleGeneraliseOperator implements Operator {

    private static String TYPE_POSTFIX = "role";

    @Override
    public Stream<Pattern> apply(Pattern src, TypeContext ctx) {
        //transform each statement into a set of its possible generalisations
        List<Set<Statement>> transformedStatements = src.statements().stream()
                .map(s -> transformStatement(s, ctx))
                .collect(Collectors.toList());
        //we obtain a set of all possible patterns by computing a CP between sets of possible generalisation
        //of all statements
        return Sets.cartesianProduct(transformedStatements).stream().map(Graql::and);
    }

    /**
     * transform: single statement -> multiple statements each with different change in its relationProperty
     * @param src
     * @param ctx
     * @return
     */
    private Set<Statement> transformStatement(Statement src, TypeContext ctx){
        Variable var = src.var();
        RelationProperty relProperty = src.getProperty(RelationProperty.class).orElse(null);
        if (relProperty == null) return Sets.newHashSet(src);

        Set<RelationProperty> transformedRelationsProps = transformRelationProperty(relProperty, ctx);

        return transformedRelationsProps.stream()
                .map(prop -> {
                    LinkedHashSet<VarProperty> properties = new LinkedHashSet<>(src.properties());
                    properties.remove(relProperty);
                    properties.add(prop);
                    return Statement.create(var, properties);
        }).collect(Collectors.toSet());
    }

    /**
     * transform: single rel prop -> multiple rel prop each with one RP generalised
     **/
    private Set<RelationProperty> transformRelationProperty(RelationProperty prop, TypeContext ctx){
        List<RelationProperty.RolePlayer> originalRPs = prop.relationPlayers();
        Set<RelationProperty> transformedProperties = new HashSet<>();

        originalRPs.forEach(rp -> {
            RelationProperty.RolePlayer transformed = transformRolePlayer(rp, ctx);
            Set<RelationProperty.RolePlayer> rps = new HashSet<>(originalRPs);
            rps.remove(rp);
            if (transformed != null) rps.add(transformed);
            if (!rps.isEmpty()) transformedProperties.add(Utils.relationProperty(rps));
        });

        return transformedProperties;
    }

    private RelationProperty.RolePlayer transformRolePlayer(RelationProperty.RolePlayer rp, TypeContext ctx){
        Statement roleStatement = rp.getRole().orElse(null);
        if (roleStatement == null) return rp;

        String type = roleStatement.getType().orElse(null);
        if (type == null) return null;

        Statement player = rp.getPlayer();
        if (ctx.isMetaType(type)){
            return new RelationProperty.RolePlayer(Graql.var(player.var().name() + TYPE_POSTFIX), player);
        }
        String superType = ctx.sup(type);
        return new RelationProperty.RolePlayer(Graql.var().type(superType), player);
    }


}
