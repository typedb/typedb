/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.reasoner.pattern;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import grakn.core.graql.internal.reasoner.utils.Pair;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class RelationPattern extends QueryPattern {

    private final List<Pattern> patterns;

    public RelationPattern(Multimap<Label, Label> rpConf, List<ConceptId> ids, List<ConceptId> relIds){
        ImmutableMultimap.Builder<Label, Pair<Label, List<ConceptId>>> builder = ImmutableMultimap.builder();
        rpConf.forEach((key, value) -> builder.put(key, new Pair<>(value, ids)));
        this.patterns = generateRelationPatterns(
                builder.build(),
                relIds
        );

    }

    @Override
    public List<String> patterns() {
        return patterns.stream().map(Object::toString).collect(Collectors.toList());
    }

    @Override
    public int size(){ return patterns.size();}


    /**
     * Generates different relation patterns variants as a cartesian products of provided id configurations.
     *
     * ( (role label): $x, (role label): $y, ...) {T1(x), T2(y), ...}, {[x/...], [y/...], ...}
     *
     * NB: only roleplayer ids are involved in the Cartesian Product
     *
     * Example:
     * [someRole, someType, {V123, V456, V789} ]
     * [anotherRole, x, {V123}]
     * [yetAnotherRole, x, {V456}]
     *
     * Will generate the following relations:
     *
     * {Type cartesian product},
     * {Role Player Id cartesian product}
     * {Rel id variants}
     *
     * (someRole: $genVarA, anotherRole: $genVarB, yetAnotherRole: $genVarC), someType($genVarA)
     * (someRole: $genVarA, anotherRole: $genVarB, yetAnotherRole: $genVarC), [$genVarA/V123], [$genVarB/V123], [$genVarC/V456]
     * (someRole: $genVarA, anotherRole: $genVarB, yetAnotherRole: $genVarC), [$genVarA/V456], [$genVarB/V123], [$genVarC/V456]
     * (someRole: $genVarA, anotherRole: $genVarB, yetAnotherRole: $genVarC), [$genVarA/V789], [$genVarB/V123], [$genVarC/V456]
     *
     * @param spec roleplayer configuration in the form {role} -> {type label, {ids...}}
     * @param relationIds list of id mappings for relation variable
     * @return list of generated patterns as strings
     */
    private static List<Pattern> generateRelationPatterns(
            Multimap<Label, Pair<Label, List<ConceptId>>> spec,
            List<ConceptId> relationIds){
        Statement relationVar = !relationIds.isEmpty()? new Statement(new Variable().asUserDefined()) : Graql.var();
        Statement[] basePattern = {relationVar};
        List<List<Pattern>> rpTypePatterns = new ArrayList<>();
        List<List<Pattern>> rpIdPatterns = new ArrayList<>();
        Multimap<Label, Statement> rps = HashMultimap.create();
        spec.entries().forEach(entry -> {
            Statement rolePlayer = new Statement(new Variable().asUserDefined());
            Label role = entry.getKey();
            Label type = entry.getValue().getKey();
            List<ConceptId> ids = entry.getValue().getValue();
            basePattern[0] = basePattern[0].rel(role.getValue(), rolePlayer);
            rps.put(role, rolePlayer);
            List<Pattern> rpPattern = Lists.newArrayList(rolePlayer);
            List<Pattern> typePattern = Lists.newArrayList(rolePlayer);
            if(type != null) typePattern.add(rolePlayer.isa(type.getValue()));

            ids.forEach(id -> {
                Statement idPattern = rolePlayer.id(id.getValue());
                rpPattern.add(idPattern);
            });

            rpIdPatterns.add(rpPattern);
            rpTypePatterns.add(typePattern);
        });
        List<Pattern> relIdPatterns = new ArrayList<>();
        relationIds.forEach(relId -> relIdPatterns.add(Graql.and(basePattern[0], relationVar.id(relId.getValue()))));

        List<Pattern> patterns = new ArrayList<>();

        Stream.concat(
                Lists.cartesianProduct(rpTypePatterns).stream(),
                Lists.cartesianProduct(rpIdPatterns).stream()
        )
                //filter trivial patterns
                .map(l -> l.stream()
                        .filter(p -> (p instanceof Conjunction) ||
                                        ((Statement) p).properties().stream().findFirst().isPresent())
                        .collect(Collectors.toList()))
                .forEach(product -> {
                    Pattern[] pattern = {basePattern[0]};
                    product.forEach(p -> pattern[0] = Graql.and(pattern[0], p));
                    if (!patterns.contains(pattern[0])) patterns.add(pattern[0]);
                });
        return Stream.concat(
                patterns.stream(),
                relIdPatterns.stream()
        )
                .collect(Collectors.toList());
    }

}
