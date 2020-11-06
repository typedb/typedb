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
 *
 */

package grakn.core.traversal;

import graql.lang.common.GraqlArg;
import graql.lang.common.GraqlToken;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.util.regex.Pattern;

public class Traversal {

    private TraversalPlan plan;
    private final TraversalParameters parameters;
    private int generatedIdentifierCount;

    public Traversal() {
        plan = new TraversalPlan();
        parameters = new TraversalParameters();
        generatedIdentifierCount = 0;
    }

    TraversalPlan plan() {
        return plan;
    }

    public void replacePlan(TraversalPlan optimisedPlan) {
        assert optimisedPlan.isPlanned();
        this.plan = optimisedPlan;
    }

    public Identifier newIdentifier() {
        return Identifier.Generated.of(generatedIdentifierCount++);
    }

    public void has(Identifier thing, Identifier attribute) {
        TraversalVertex thingVertex = plan.vertex(thing);
        TraversalVertex attributeVertex = plan.vertex(attribute);


    }

    public void isa(Identifier thing, Identifier type) {
        isa(thing, type, true);
    }

    public void isa(Identifier thing, Identifier type, boolean isTransitive) {

    }

    public void is(Identifier first, Identifier second) {

    }

    public void relating(Identifier relation, Identifier role) {

    }

    public void playing(Identifier thing, Identifier role) {

    }

    public void rolePlayer(Identifier relation, Identifier player) {
        rolePlayer(relation, player, null);
    }

    public void rolePlayer(Identifier relation, Identifier player, @Nullable String roleType) {

    }

    public void owns(Identifier thingType, Identifier attributeType) {

    }

    public void plays(Identifier thingType, Identifier roleType) {

    }

    public void relates(Identifier relationType, Identifier roleType) {

    }

    public void sub(Identifier subType, Identifier superType, boolean isTransitive) {

    }

    public void iid(Identifier thing, byte[] iid) {

    }

    public void type(Identifier thing, String[] labels) {
        plan.vertex(thing).type(labels);
    }

    public void isAbstract(Identifier type) {
        plan.vertex(type).isAbstract();
    }

    public void label(Identifier type, String label, @Nullable String scope) {

    }

    public void regex(Identifier type, Pattern regex) {

    }

    public void valueType(Identifier attributeType, GraqlArg.ValueType valueType) {

    }

    public void value(Identifier attribute, GraqlToken.Comparator comparator, Boolean value) {

    }

    public void value(Identifier attribute, GraqlToken.Comparator comparator, Long value) {

    }

    public void value(Identifier attribute, GraqlToken.Comparator comparator, Double value) {

    }

    public void value(Identifier attribute, GraqlToken.Comparator comparator, String value) {

    }

    public void value(Identifier attribute, GraqlToken.Comparator comparator, LocalDateTime value) {

    }

    public void value(Identifier attribute1, GraqlToken.Comparator comparator, Identifier attribute2) {

    }
}
