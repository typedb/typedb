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

package grakn.core.reasoner.inference;

import grakn.core.common.parameters.Label;
import grakn.core.concept.type.Type;
import grakn.core.concept.type.impl.TypeImpl;
import grakn.core.graph.GraphManager;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.variable.Variable;
import graql.lang.Graql;
import graql.lang.query.GraqlQuery;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertTrue;

@Ignore
public class TypeInferenceTest {

    @Test
    public void isa_inference() {
        String input = "match $p isa person; ";

    }

    @Test
    public void isa_bang_inference() {
        String input = "match $p isa! person; ";
    }

    @Test
    public void is_inference() {
        String input = "match $p sub entity; $q sub entity; $p is $q;";
    }

    @Test
    public void has_inference() {
        String input = "match $p isa person, has name 'bob'; ";
    }

    @Test
    public void has_type_variable() {
        String input = "match $p has name $a;";
    }

    @Test
    public void relation_concrete_role_concrete_relation_hidden_variable() {
        String input = "match (wife: $yoko) isa marriage;";
    }

    @Test
    public void relation_concrete_role_concrete_relation_named_variable() {
        String input = "match $r (wife: $yoko) isa marriage;";
    }

    @Test
    public void relation_variable_role_concrete_relation_hidden_variable() {
        String input = "match ($role: $yoko) isa marriage;";
    }

    @Test
    public void relation_variable_role_variable_relation_named_variable() {
        String input = "match $r (wife: $yoko) isa $m;";
    }

    @Test
    public void relation_variable_relation_not_visible() {
        String input = "match (wife: $yoko);";
    }

    @Test
    public void relation_variable_minimal() {
        String input = "match ($yoko);";
    }

    @Test
    public void relation_named_minimal() {
        String input = "match $r ($yoko) isa $m;";
    }

    @Test
    public void relation_multiple_isa() {
        String input = "match $r (employee: $a) isa employment; $r (contractee: $b) isa contract;";
    }

    @Test
    public void relation_multiple_roles() {
        String input = "match $r (husband: $john, $role: $yoko, $a) isa marriage;";
    }

    //EDGE CASES

    @Test
    public void all_the_things() {
        String input = "match $x isa thing;";
    }

    @Test
    public void just_attribute() {
        String input = "match $a=7;";
    }

    @Test
    public void hanging_has() {
        String input = "match $x has $a;";
    }

    @Test
    public void has_cycle() {
        String input = "match $a has $b; $b has $a; ";
    }

    @Test
    public void variable_relation_with_more_info() {
        String input = "match ($a) isa $r; $r sub marriage;";
    }

    @Test
    public void mix_thing_and_type() {
        String input = "match $p isa $t; $t sub person;";
    }

    //ONE HOP TESTING

    @Test
    public void detect_owns() {
        String input = "match $p isa person, has $d; $d isa weight; $d=7; ";
    }

    @Test
    public void avoid_2_hops() {
        String input = "match $p isa $q, has $d; $d isa weight; $d=7; $q sub! person;";
    }

    @Test
    public void one_hop_relation() {
        String input = "match $r (husband: $john) isa $m; ";
    }

    @Test
    public void one_hop_has_cycle() {
        String input = "match $a has $b; $b has $c; $c has $d; $d has $a;";
    }


    //LABEL INSERTION

    @Test
    public void basic_insertion() {
        String input = "match $x isa thing;";
    }

    @Test
    public void no_isa() {
        String input = "match $x has name 'bob';";
    }

    @Test
    public void correct_labels() {
        String input = "match $p isa person; $d isa dog;";
    }

    @Test
    public void labels_roles() {
        String input = "match $r (wife: $yoko) isa marriage;";
    }

    @Test
    public void label_conform_to_supers() {
        String input = "match $a isa $t;\n" +
                "  $b isa $t;\n" +
                "  $t owns $c;\n" +
                "  $t sub entity;\n" +
                "  ($a, $b) isa $rel;\n" +
                "  $rel owns $c\n" +
                "  \n" +
                "  $a has left-attr true;\n" +
                "  $b has right-attr true;";
        GraqlQuery graqlQuery = Graql.parseQuery(input);
        //TODO: this test needs to go into integration
        GraphManager graphManager = new GraphManager(null, null);
        Conjunction conjunction = Disjunction.create(graqlQuery.asMatch().conjunction().normalise()).conjunctions().iterator().next();
        TypeInference.full(conjunction, graphManager);


        for (Variable variable : conjunction.variables()) {
            if (variable.isThing() && variable.asThing().isa().isPresent() && variable.asThing().isa().get().type().sub().isPresent()) {
                Set<Label> subHints = variable.asThing().isa().get().typeHints();
                Set<Label> hintsOfSuper = variable.asThing().isa().get().type().sub().get().getTypeHints();
                for (Label hint : subHints) {
                    Type hintType = TypeImpl.of(graphManager, graphManager.schema().getType(hint));
                    Stream<Label> supersOfHint = hintType.getSupertypes().map(Type::getLabel).map(Label::of);
                    assertTrue(supersOfHint.anyMatch(hintsOfSuper::contains));
                }
            } else if (variable.isType() && variable.asType().sub().isPresent() && variable.asType().sub().get().type().sub().isPresent()) {
                Set<Label> subHints = variable.asType().sub().get().getTypeHints();
                Set<Label> hintsOfSuper = variable.asType().sub().get().type().sub().get().getTypeHints();
                for (Label hint : subHints) {
                    Type hintType = TypeImpl.of(graphManager, graphManager.schema().getType(hint));
                    Stream<Label> supersOfHint = hintType.getSupertypes().map(Type::getLabel).map(Label::of);
                    assertTrue(supersOfHint.anyMatch(hintsOfSuper::contains));
                }
            }
        }
    }


}
