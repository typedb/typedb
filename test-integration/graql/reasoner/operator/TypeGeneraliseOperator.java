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

import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.property.IsaProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Stream;

public class TypeGeneraliseOperator implements Operator {

    private static String TYPE_POSTFIX = "type";

    @Override
    public Stream<Pattern> apply(Pattern src, TypeContext ctx) {
        Set<Statement> originalStatements = src.statements();
        Set<Pattern> transformedPatterns = new HashSet<>();

        originalStatements.forEach(s -> {
            Statement transformed = transformStatement(s, ctx);
            Set<Statement> statements = new HashSet<>(originalStatements);
            statements.remove(s);
            if (transformed != null) statements.add(transformed);
            if (!statements.isEmpty()) transformedPatterns.add(Graql.and(statements));
        });
        return transformedPatterns.stream()
                .filter(p -> !p.equals(src));
    }

    private Statement transformStatement(Statement src, TypeContext ctx){
        Variable var = src.var();
        IsaProperty isaProperty = src.getProperty(IsaProperty.class).orElse(null);
        if (isaProperty == null) return src;

        String type = isaProperty.type().getType().orElse(null);
        if (type == null) return null;

        LinkedHashSet<VarProperty> properties = new LinkedHashSet<>(src.properties());
        properties.remove(isaProperty);
        Statement newStatement = Statement.create(var, properties);

        if(ctx.isMetaType(type)){
            newStatement = newStatement.isa(Graql.var(var.name() + TYPE_POSTFIX));
        } else {
            String superType = ctx.sup(type);
            newStatement = newStatement.isa(superType);
        }
        return newStatement;
    }
}
