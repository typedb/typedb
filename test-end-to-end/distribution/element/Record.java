/*
 * Copyright (C) 2020 Grakn Labs
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
 */

package grakn.core.distribution.element;

import com.google.common.collect.ImmutableMap;
import grakn.client.concept.ConceptId;
import grakn.common.util.Pair;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import graql.lang.statement.StatementInstance;
import graql.lang.statement.Variable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class Record implements Element{
    final private List<AttributeElement> attributes;
    final private String type;
    private final ImmutableMap<String, ConceptId> indexToId;

    public Record(String type, Collection<AttributeElement> attributes){
        this.type = type;
        this.attributes = new ArrayList<>(attributes);
        this.indexToId = null;
    }
    public Record(String type, Collection<AttributeElement> attributes, Map<String, ConceptId> map){
        this.type = type;
        this.attributes = new ArrayList<>(attributes);
        this.indexToId = ImmutableMap.copyOf(map);
    }

    public Record withIds(Map<String, ConceptId> map){
        return new Record(this.getType(), this.getAttributes(), map);
    }

    public String getType(){ return type;}
    public List<AttributeElement> getAttributes(){ return attributes;}

    @Override
    public String toString(){ return getType() + ": " + getAttributes();}

    @Override
    public int conceptSize() { return getAttributes().size() + 1; }

    @Override
    public Pattern patternise(Variable var) {
        if (indexToId == null) return patterniseWithoutIds(var);
        return patterniseWithIds(var);
    }

    private Pattern patterniseWithoutIds(Variable var){
        Statement base = Graql.var(var);
        StatementInstance pattern = base.isa(getType());
        for (AttributeElement attribute : getAttributes()) {
            Object value = attribute.getValue();
            if (value instanceof String) {
                value = ((String) value).replace("\"", "'");
                pattern = pattern.has(attribute.getType(), (String) value);
            } else if (value instanceof Long) {
                pattern = pattern.has(attribute.getType(), (long) value);
            } else if (value instanceof Double) {
                pattern = pattern.has(attribute.getType(), (double) value);
            } else if (value instanceof Integer) {
                pattern = pattern.has(attribute.getType(), (int) value);
            } else {
                throw new IllegalArgumentException();
            }
        }
        return pattern;
    }

    private Pattern patterniseWithIds(Variable var){
        Statement base = Graql.var(var);
        StatementInstance basePattern = base.isa(getType());
        List<Pattern> patterns = new ArrayList<>();

        List<Pair<Variable, ConceptId>> varToId = new ArrayList<>();
        for (AttributeElement attribute : getAttributes()) {
            Variable attrVar = Graql.var().var().asReturnedVar();
            Statement statement = Graql.var(attrVar);

            ConceptId id = indexToId.get(attribute.index());
            if (id == null){
                System.out.println("id null for key: " + attribute.index());
            } else {
                basePattern = basePattern.has(attribute.getType(), statement);
                varToId.add(new Pair<>(attrVar, id));
            }
        }
        patterns.add(basePattern);
        varToId.forEach(p -> patterns.add(Graql.var(p.first()).id(p.second().getValue())));
        return Graql.and(patterns);
    }
}
