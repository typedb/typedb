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

package grakn.core.logic;

import grakn.common.collection.Pair;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Label;
import grakn.core.logic.tool.TypeHinter;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.variable.Variable;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlMatch;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.set;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;

@Ignore
public class TypeHinterTest {
    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("type-hinter-test");
    private static String database = "type-hinter-test";
    private static RocksGrakn grakn;
    private static RocksSession session;
    private static RocksTransaction transaction;

    @BeforeClass
    public static void open_session() throws IOException {
        Util.resetDirectory(directory);
        grakn = RocksGrakn.open(directory);
        grakn.databases().create(database);
        session = grakn.session(database, Arguments.Session.Type.SCHEMA);
    }

    @AfterClass
    public static void close_session() {
        session.close();
        grakn.close();
    }

    private static void define_standard_schema(String fileName) throws IOException {
        transaction = session.transaction(Arguments.Transaction.Type.WRITE);
        final GraqlDefine query = Graql.parseQuery(
                new String(Files.readAllBytes(Paths.get("test/integration/reasoner/" + fileName + ".gql")), UTF_8));
        transaction.query().define(query);
    }

    private static void define_custom_schema(String schema) {
        transaction = session.transaction(Arguments.Transaction.Type.WRITE);
        final GraqlDefine query = Graql.parseQuery(schema);
        transaction.query().define(query);
    }

    private Set<Label> getHints(Variable variable) {
        if (variable.isType() && variable.asType().sub().isPresent()) {
            return variable.asType().sub().get().getTypeHints();
        } else if (variable.isThing() && variable.asThing().isa().isPresent()) {
            return variable.asThing().isa().get().getTypeHints();
        } else {
            return new HashSet<>();
        }
    }

    private Map<String, Set<String>> getHintMap(Conjunction conjunction) {
        return conjunction.variables().stream().collect(Collectors.toMap(
                variable -> variable.reference().syntax(),
                variable -> getHints(variable).stream().map(Label::scopedName).collect(Collectors.toSet())
        ));
    }

    private Map<Pair<String, String>, Set<String>> getRoleHints(Conjunction conjunction) {
        Map<Pair<String, String>, Set<String>> ans = new HashMap<>();
        conjunction.variables().stream().filter(Variable::isThing).map(Variable::asThing)
                .filter(variable -> !variable.relation().isEmpty())
                .flatMap(variable -> variable.relation().stream())
                .flatMap(relationConstraint -> relationConstraint.players().stream())
                .forEach(rolePlayer -> {
                    if (rolePlayer.roleType().isPresent() && rolePlayer.roleType().get().reference().isName()) {
                        ans.put(new Pair<>(
                                        rolePlayer.roleType().get().reference().syntax(),
                                        rolePlayer.player().reference().syntax()
                                ),
                                rolePlayer.roleTypeHints().stream().map(Label::scopedName).collect(Collectors.toSet()));
                    } else {
                        ans.put(new Pair<>("", rolePlayer.player().reference().syntax()),
                                rolePlayer.roleTypeHints().stream().map(Label::scopedName).collect(Collectors.toSet()));
                    }
                });

        return ans;
    }

    private Conjunction createConjunction(String matchString) {
        GraqlMatch query = Graql.parseQuery(matchString);
        return Disjunction.create(query.conjunction().normalise()).conjunctions().iterator().next();
    }

    private Conjunction runExhaustiveHinter(TypeHinter typeHinter, String matchString) {
        return typeHinter.computeHintsExhaustive(createConjunction(matchString));
    }

    private Conjunction runSimpleHinter(TypeHinter typeHinter, String matchString) {
        return typeHinter.computeHints(createConjunction(matchString));
    }

    @Test
    public void isa_inference() throws IOException {
        define_standard_schema("basic-schema");
        TypeHinter typeHinter = transaction.logic().typeHinter();

        String queryString = "match $p isa person; ";
        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$p", set("person", "man", "woman"));
        }};

        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));
    }

    @Test
    public void isa_explicit_inference() throws IOException {
        define_standard_schema("basic-schema");
        TypeHinter typeHinter = transaction.logic().typeHinter();

        String queryString = "match $p isa! person; ";
        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$p", set("person"));
        }};

        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));
    }

    @Test
    public void is_inference() throws IOException {
        define_standard_schema("basic-schema");
        TypeHinter typeHinter = transaction.logic().typeHinter();

        String queryString = "match" +
                "  $p sub entity;" +
                "  $p is $q;" +
                "  $q sub mammal";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$p", set("mammal", "person", "man", "woman", "dog"));
            put("$q", set("mammal", "person", "man", "woman", "dog"));
        }};

        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));
    }

    @Test
    public void has_inference() throws IOException {
        define_standard_schema("basic-schema");
        TypeHinter typeHinter = transaction.logic().typeHinter();

        String queryString = "match $p has name 'bob';";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$p", set("person", "man", "woman", "dog"));
        }};

        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));
    }

    @Test
    public void has_inference_variable_with_attribute_type() throws IOException {
        define_standard_schema("basic-schema");
        TypeHinter typeHinter = transaction.logic().typeHinter();

        String queryString = "match $p has name $a;";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$p", set("person", "man", "woman", "dog"));
            put("$a", set("name", "dog-name"));
        }};
        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));
    }

    @Test
    public void has_inference_variable_without_attribute_type() throws IOException {
        define_standard_schema("basic-schema");
        TypeHinter typeHinter = transaction.logic().typeHinter();

        String queryString = "match" +
                "  $p isa shape;" +
                "  $p has $a;";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$a", set("perimeter", "area"));
        }};
        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));
    }

    @Test
    public void relation_concrete_role_concrete() throws IOException {
        define_standard_schema("basic-schema");
        TypeHinter typeHinter = transaction.logic().typeHinter();

        String queryString = "match $r (wife: $yoko) isa marriage;";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$yoko", set("person", "man", "woman"));
            put("$r", set("marriage"));
        }};

        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));

        Map<Pair<String, String>, Set<String>> expectedRoles = new HashMap<Pair<String, String>, Set<String>>() {{
            put(new Pair<>("$yoko", ""), set("marriage:husband", "marriage:wife", "marriage:spouse"));
        }};

        assertTrue(getRoleHints(exhaustiveConjunction).entrySet().containsAll(expectedRoles.entrySet()));
        assertTrue(getRoleHints(simpleConjunction).entrySet().containsAll(expectedRoles.entrySet()));
    }

    @Test
    public void relation_variable_role_concrete_relation_hidden_variable() throws IOException {
        define_standard_schema("basic-schema");
        TypeHinter typeHinter = transaction.logic().typeHinter();

        String queryString = "match $r ($role: $yoko) isa marriage;";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$yoko", set("person", "man", "woman"));
            put("$role", set("marriage:husband", "marriage:wife", "marriage:spouse"));
            put("$r", set("marriage"));
        }};

        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));

        Map<Pair<String, String>, Set<String>> expectedRoles = new HashMap<Pair<String, String>, Set<String>>() {{
            put(new Pair<>("$yoko", "$role"), set("marriage:husband", "marriage:wife", "marriage:spouse"));
        }};

        assertTrue(getRoleHints(exhaustiveConjunction).entrySet().containsAll(expectedRoles.entrySet()));
        assertTrue(getRoleHints(simpleConjunction).entrySet().containsAll(expectedRoles.entrySet()));
    }

    @Test
    public void relation_variable_role_variable_relation_named_variable() throws IOException {
        define_standard_schema("basic-schema");
        TypeHinter typeHinter = transaction.logic().typeHinter();

        String queryString = "match $r (wife: $yoko) isa $m;";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$yoko", set("person", "man", "woman"));
            put("$r", set("marriage"));
            put("$m", set("marriage"));
        }};

        Map<Pair<String, String>, Set<String>> expectedRoles = new HashMap<Pair<String, String>, Set<String>>() {{
            put(new Pair<>("$yoko", ""), set("marriage:husband", "marriage:wife", "marriage:spouse"));
        }};

        assertTrue(getRoleHints(exhaustiveConjunction).entrySet().containsAll(expectedRoles.entrySet()));
        assertTrue(getRoleHints(simpleConjunction).entrySet().containsAll(expectedRoles.entrySet()));
        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));
    }

    @Test
    public void minimal_relation() throws IOException {
        define_standard_schema("basic-schema");
        TypeHinter typeHinter = transaction.logic().typeHinter();

        String queryString = "match $r (wife: $yoko);";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$yoko", set("person", "man", "woman"));
            put("$r", set("marriage"));
        }};

        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));
        Map<Pair<String, String>, Set<String>> expectedRoles = new HashMap<Pair<String, String>, Set<String>>() {{
            put(new Pair<>("$yoko", ""), set("marriage:husband", "marriage:wife", "marriage:spouse"));
        }};

        assertTrue(getRoleHints(exhaustiveConjunction).entrySet().containsAll(expectedRoles.entrySet()));
        assertTrue(getRoleHints(simpleConjunction).entrySet().containsAll(expectedRoles.entrySet()));
    }

    @Test
    public void relation_multiple_roles() throws IOException {
        define_standard_schema("basic-schema");
        TypeHinter typeHinter = transaction.logic().typeHinter();

        String queryString = "match $r (husband: $john, $role: $yoko, $a) isa marriage;";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$yoko", set("person", "man", "woman"));
            put("$john", set("person", "man", "woman"));
            put("$role", set("marriage:husband", "marriage:wife", "marriage:spouse"));
            put("$r", set("marriage"));
        }};

        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));

        Map<Pair<String, String>, Set<String>> expectedRoles = new HashMap<Pair<String, String>, Set<String>>() {{
            put(new Pair<>("$yoko", "$role"), set("marriage:husband", "marriage:wife", "marriage:spouse"));
            put(new Pair<>("$john", ""), set("marriage:husband", "marriage:wife", "marriage:spouse"));
        }};

        assertTrue(getRoleHints(exhaustiveConjunction).entrySet().containsAll(expectedRoles.entrySet()));
        assertTrue(getRoleHints(simpleConjunction).entrySet().containsAll(expectedRoles.entrySet()));
    }

    @Test
    public void has_reverse() throws IOException {
        define_standard_schema("basic-schema");
        TypeHinter typeHinter = transaction.logic().typeHinter();

        String queryString = "match" +
                "  $p isa! person;" +
                "  $p has $a;";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$a", set("name, dog-name"));
        }};

        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));
    }

    @Test
    public void negations_ignored() throws IOException {
        define_standard_schema("basic-schema");
        TypeHinter typeHinter = transaction.logic().typeHinter();
        String queryString = "match" +
                "  $p isa person;" +
                "  not {$p isa man;};";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$p", set("person", "man", "woman"));
        }};

        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));
    }

    @Test
    public void up_down_hierarchy_isa() throws IOException {
        define_custom_schema(
                "define" +
                        "  animal sub entity;" +
                        "  person sub animal;" +
                        "  man sub person;" +
                        "  greek sub man;" +
                        "  socrates sub greek;"
        );
        TypeHinter typeHinter = transaction.logic().typeHinter();
        String queryString = "match" +
                "  $p isa man;" +
                "  man sub $q;";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$p", set("man", "greek", "socrates"));
            put("$q", set("thing", "entity", "person", "man"));
        }};

        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));
    }

    @Test
    public void infer_from_value_type() throws IOException {
        define_custom_schema(
                "define" +
                        "  dog sub entity, owns weight;" +
                        "  person sub entity, owns name;" +
                        "  weight sub attribute, value double;" +
                        "  name sub attribute, value string;"
        );
        TypeHinter typeHinter = transaction.logic().typeHinter();
        String queryString = "match" +
                "  $p has $a;" +
                "  $a='bob';";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$p", set("person"));
            put("$a", set("name"));
        }};

        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));
    }

    @Test
    public void has_hierarchy() throws IOException {
        define_custom_schema(
                "define" +
                        "  animal sub entity, owns weight;" +
                        "  person sub animal, owns leg-weight" +
                        "  chair sub entity, owns leg-weight;" +
                        "  dog sub animal;" +
                        "  weight sub attribute, value long;" +
                        "  leg-weight, sub weight;"
        );
        TypeHinter typeHinter = transaction.logic().typeHinter();
        String queryString = "match" +
                "  $a has weight $c;" +
                "  $b has leg-weight 5;" +
                "  $p has weight $c;";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$a", set("animal, dog, person, chair"));
            put("$b", set("person, chair"));
            put("$c", set("weight, leg-weight"));
        }};

        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));
    }


    @Test
    public void has_with_cycle() throws IOException {
        define_custom_schema(
                "define" +
                        "  person sub entity, owns name, owns height;" +
                        "  name sub attribute, value string, owns nickname;" +
                        "  nickname sub attribute, value string, owns name;" +
                        "  surname sub attribute, value string, owns name;"
        );
        TypeHinter typeHinter = transaction.logic().typeHinter();
        String queryString = "match" +
                "  $a has $b" +
                "  $b has $a";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$a", set("name", "surname"));
            put("$b", set("name", "surname"));
        }};

        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));
    }

    @Test
    public void has_with_big_cycle() throws IOException {
        define_custom_schema(
                "define" +
                        "  person sub entity, owns name, owns height;" +
                        "  name sub attribute, value string, owns nickname;" +
                        "  nickname sub attribute, value string, owns surname;" +
                        "  surname sub attribute, value string, owns middlename;" +
                        "  middlename sub attribute, value string, owns name;" +
                        "  weight sub attribute, value double, owns measure-system;" +
                        "  measure-system sub attribute, owns conversion-rate;"
        );
        TypeHinter typeHinter = transaction.logic().typeHinter();
        String queryString = "match" +
                "  $a has $b" +
                "  $b has $c" +
                "  $c has $d" +
                "  $d has $a";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expectedExhaustive = new HashMap<String, Set<String>>() {{
            put("$a", set("name", "surname", "nickname", "middlename"));
        }};
        Map<String, Set<String>> expectedSimple = new HashMap<String, Set<String>>() {{
            put("$a", set("name", "surname", "nickname", "middlename", "measure-system"));
        }};

        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expectedExhaustive.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expectedSimple.entrySet()));
    }

    @Test
    public void you_know_the_thing() throws IOException {
        define_standard_schema("schema-basic");
        TypeHinter typeHinter = transaction.logic().typeHinter();

        String queryString = "match $x isa thing;";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", Collections.emptySet());
        }};

        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));
    }

    @Test
    public void simple_always_infers_its_supers() {
        define_custom_schema(
                "define" +
                        "  animal sub entity;" +
                        "  person sub animal;" +
                        "  man sub person;" +
                        "  greek sub man;" +
                        "  socrates sub greek;"
        );
        TypeHinter typeHinter = transaction.logic().typeHinter();

        String queryString = "match $x isa $y;" +
                "  $y sub $z;" +
                "  $z sub $w;" +
                "  $w sub! man;";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);
        Conjunction simpleConjunction = runSimpleHinter(typeHinter, queryString);


        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$x", set("man", "greek", "socrates"));
            put("$y", set("man", "greek", "socrates"));
            put("$z", set("man", "greek", "socrates"));
            put("$w", set("man", "greek"));
        }};

        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
        assertTrue(getHintMap(simpleConjunction).entrySet().containsAll(expected.entrySet()));
    }

    // It might be assumed that the set of hint labels will have to conform to certain forms.
    // The tests below show that any structure is possible.

    // When a hint label exists on a variable, none, some or all of its children may be type variables as well
    // "$x isa person;" and "$x isa! person;" trivially show that all or none of the children are possible.
    // But getting only some of the children requires a more complicate query, which is shown below.
    @Test
    public void can_infer_some_but_not_all_subtypes() throws IOException {
        define_custom_schema(
                "define " +
                        "  animal sub entity;" +
                        "  person sub animal, owns head-weight, owns arm-weight, owns hand-weight, owns leg-weight, owns weight;" +
                        "  horse sub animal, owns head-weight, owns tail-weight, owns leg-weight, owns weight" +
                        "  weight sub entity;" +
                        "  head-weight sub weight;" +
                        "  arm-weight sub weight;" +
                        "  leg-weight weight;" +
                        "  hand-weight sub weight;" +
                        "  tail-weight sub weight"
        );
        TypeHinter typeHinter = transaction.logic().typeHinter();

        String queryString = "match" +
                "  $a has $c;" +
                "  $b has $c;" +
                "  $a isa person;" +
                "  $b isa worm;" +
                "  ";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);


        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$c", set("weight", "head-weight", "leg-weight"));
        }};
        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
    }

    // When a hint label exists, it can "skip" a generation, meaning a hint and the hint's descendent is possible, yet
    // none of the hint's direct children are possible.
    // We show this below on the hint labels of $t
    // We also show on $a that hints can be isolated form each other completely hierarchy-wise.
    @Test
    public void hierarchy_hint_gap() throws IOException {
        define_custom_schema(
                "define " +
                        "  animal sub entity;" +
                        "  left-attr sub attribute, value boolean;" +
                        "  right-attr sub attribute, value boolean;" +
                        "  ownership-attr sub attribute, value boolean;" +
                        "  marriage-attr sub attribute, value boolean;" +
                        "  animal sub entity, owns ownership-attr; " +
                        "  mammal sub animal; " +
                        "  person sub mammal, plays ownership:owner, owns marriage-attr; " +
                        "  man sub person, plays marriage:husband, owns left-attr; " +
                        "  woman sub person, plays marriage:wife, owns right-attr; " +
                        "  tortoise sub animal, plays ownership:pet, owns left-attr; " +
                        "  marriage sub relation, relates husband, relates wife, owns marriage-attr; " +
                        "  ownership sub relation, relates pet, relates owner, owns ownership-attr;"
        );
        TypeHinter typeHinter = transaction.logic().typeHinter();
        String queryString = "match " +
                "  $a isa $t; " +
                "  $b isa $t; " +
                "  $t owns $c; " +
                "  $t sub entity; " +
                "  ($a, $b) isa $rel; " +
                "  $rel owns $c " +
                "  $a has left-attr true; " +
                "  $b has right-attr true;";

        Conjunction exhaustiveConjunction = runExhaustiveHinter(typeHinter, queryString);

        Map<String, Set<String>> expected = new HashMap<String, Set<String>>() {{
            put("$t", set("thing", "animal", "person"));
            put("$a", set("tortoise", "man"));
        }};
        assertTrue(getHintMap(exhaustiveConjunction).entrySet().containsAll(expected.entrySet()));
    }
}
