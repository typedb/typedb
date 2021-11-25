/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.logic;

import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Options.Database;
import com.vaticle.typedb.core.logic.tool.TypeInference;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.rocks.RocksTypeDB;
import com.vaticle.typedb.core.test.integration.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.test.Util.assertThrows;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TypeInferenceTest {
    private static final Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("type-resolver-test");
    private static final Path logDir = dataDir.resolve("logs");
    private static final Database options = new Database().dataDir(dataDir).reasonerDebuggerDir(logDir);
    private static final String database = "type-resolver-test";
    private static RocksTypeDB typedb;
    private static RocksSession session;
    private static RocksTransaction transaction;

    @BeforeClass
    public static void open_session() throws IOException {
        Util.resetDirectory(dataDir);
        typedb = RocksTypeDB.open(options);
        typedb.databases().create(database);
        session = typedb.session(database, Arguments.Session.Type.SCHEMA);
    }

    @AfterClass
    public static void close_session() {
        session.close();
        typedb.close();
    }

    @Before
    public void setup() {
        transaction = session.transaction(Arguments.Transaction.Type.WRITE);
    }

    @After
    public void tearDown() {
        transaction.close();
    }

    private static void define_standard_schema(String fileName) throws IOException {
        TypeQLDefine query = TypeQL.parseQuery(
                new String(Files.readAllBytes(Paths.get("test/integration/logic/" + fileName + ".tql")), UTF_8));
        transaction.query().define(query);
    }

    private static void define_custom_schema(String schema) {
        TypeQLDefine query = TypeQL.parseQuery(schema);
        transaction.query().define(query);
    }

    private Map<String, Set<String>> resolvedTypeMap(Conjunction conjunction) {
        return conjunction.variables().stream().collect(Collectors.toMap(
                variable -> variable.id().toString(),
                variable -> variable.inferredTypes().stream().map(Label::scopedName).collect(Collectors.toSet())
        ));
    }

    private Disjunction createDisjunction(String matchString) {
        TypeQLMatch query = TypeQL.parseQuery(matchString);
        return Disjunction.create(query.conjunction().normalise());
    }

    @Test
    public void schema_query_not_resolved_beyond_labels() throws IOException {
        define_standard_schema("basic-schema");
        String queryString = "match $p sub person, owns $a;";
        TypeInference typeInference = transaction.logic().typeInference();

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);
        assertTrue(disjunction.isCoherent());

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$_person", set("person"));
            put("$p", set());
            put("$a", set());
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void isa_inference() throws IOException {
        define_standard_schema("basic-schema");
        String queryString = "match $p isa person;";
        TypeInference typeInference = transaction.logic().typeInference();

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$p", set("person", "man", "woman"));
            put("$_person", set("person"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void isa_explicit_inference() throws IOException {
        define_standard_schema("basic-schema");
        TypeInference typeInference = transaction.logic().typeInference();

        String queryString = "match $p isa! person; ";
        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$p", set("person"));
            put("$_person", set("person"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void is_inference() throws IOException {
        define_standard_schema("basic-schema");
        TypeInference typeInference = transaction.logic().typeInference();

        String queryString = "match" +
                "  $p isa entity;" +
                "  $p is $q;" +
                "  $q isa mammal;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expectedExhaustive = new HashMap<>() {{
            put("$p", set("mammal", "person", "man", "woman", "dog"));
            put("$q", set("mammal", "person", "man", "woman", "dog"));
            put("$_entity", set("entity"));
            put("$_mammal", set("mammal"));
        }};

        assertEquals(expectedExhaustive, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void has_inference() throws IOException {
        define_standard_schema("basic-schema");
        TypeInference typeInference = transaction.logic().typeInference();

        String queryString = "match $p has name 'bob';";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$p", set("person", "man", "woman", "dog"));
            put("$_name", set("name"));
            put("$_0", set("name"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void has_with_cycle() throws IOException {
        define_custom_schema(
                "define" +
                        "  person sub entity, owns name, owns height;" +
                        "  name sub attribute, value string, owns nickname;" +
                        "  nickname sub attribute, value string, owns name;" +
                        "  surname sub attribute, value string, owns name;" +
                        "  name sub attribute, value string;" +
                        "  surname sub attribute, value string;" +
                        "  nickname sub attribute, value string;" +
                        "  height sub attribute, value double;" +
                        "  "
        );
        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match" +
                "  $a has $b;" +
                "  $b has $a;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$a", set("nickname", "name"));
            put("$b", set("nickname", "name"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void has_with_big_cycle() {
        define_custom_schema(
                "define" +
                        "  person sub entity, owns name, owns height;" +
                        "  name sub attribute, value string, owns nickname;" +
                        "  nickname sub attribute, value string, owns surname;" +
                        "  surname sub attribute, value string, owns middlename;" +
                        "  middlename sub attribute, value string, owns name;" +
                        "  weight sub attribute, value double, owns measure-system;" +
                        "  measure-system sub attribute, value string, owns conversion-rate;" +
                        "  conversion-rate sub attribute, value double;" +
                        "  height sub attribute, value double;"
        );
        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match" +
                "  $a has $b;" +
                "  $b has $c;" +
                "  $c has $d;" +
                "  $d has $a;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expectedExhaustive = new HashMap<>() {{
            put("$a", set("name", "surname", "nickname", "middlename"));
            put("$b", set("name", "surname", "nickname", "middlename"));
            put("$c", set("name", "surname", "nickname", "middlename"));
            put("$d", set("name", "surname", "nickname", "middlename"));
        }};

        assertEquals(expectedExhaustive, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void has_inference_variable_with_attribute_type() throws IOException {
        define_standard_schema("basic-schema");
        TypeInference typeInference = transaction.logic().typeInference();

        String queryString = "match $p has name $a;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$p", set("person", "man", "woman", "dog"));
            put("$a", set("name"));
            put("$_name", set("name"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void has_inference_variable_without_attribute_type() throws IOException {
        define_standard_schema("basic-schema");
        TypeInference typeInference = transaction.logic().typeInference();

        String queryString = "match" +
                "  $p isa shape;" +
                "  $p has $a;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$p", set("triangle", "right-angled-triangle", "square"));
            put("$a", set("perimeter", "area", "label", "hypotenuse-length"));
            put("$_shape", set("shape"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void infer_from_value_type() {
        define_custom_schema(
                "define" +
                        "  dog sub entity, owns weight;" +
                        "  person sub entity, owns name;" +
                        "  weight sub attribute, value double;" +
                        "  name sub attribute, value string;"
        );
        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match" +
                "  $p has $a;" +
                "  $a = 'bob';";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$p", set("person"));
            put("$a", set("name"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void value_comparision_between_double_long() throws IOException {
        define_custom_schema("define" +
                                     " house-number sub attribute, value long;" +
                                     " length sub attribute, value double;" +
                                     " name sub attribute, value string;"
        );

        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match $x = 1; $y = 1.0; $z = 'bob';";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$x", set("house-number", "length"));
            put("$y", set("house-number", "length"));
            put("$z", set("name"));
        }};
        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void relation_concrete_role_concrete() throws IOException {
        define_standard_schema("basic-schema");
        TypeInference typeInference = transaction.logic().typeInference();

        String queryString = "match $r (wife: $yoko) isa marriage;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$yoko", set("woman"));
            put("$r", set("marriage"));
            put("$_marriage:wife", set("marriage:wife"));
            put("$_marriage", set("marriage"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void relation_variable_role_concrete_relation_hidden_variable() throws IOException {
        define_standard_schema("basic-schema");
        TypeInference typeInference = transaction.logic().typeInference();

        String queryString = "match $r ($role: $yoko) isa marriage;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$yoko", set("person", "man", "woman"));
            put("$role", set("marriage:husband", "marriage:wife", "marriage:spouse", "relation:role"));
            put("$r", set("marriage"));
            put("$_marriage", set("marriage"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void relation_variable_role_variable_relation_named_variable() throws IOException {
        define_standard_schema("basic-schema");
        TypeInference typeInference = transaction.logic().typeInference();

        String queryString = "match $r (wife: $yoko) isa $m;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$yoko", set("woman"));
            put("$r", set("marriage"));
            put("$m", set("marriage", "relation", "thing"));
            put("$_relation:wife", set("marriage:wife"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void relation_staggered_role_hierarchy() {
        define_custom_schema("define" +
                                     " person sub entity," +
                                     "  plays partnership:partner," +
                                     "  plays marriage:spouse;" +
                                     "" +
                                     " man sub person," +
                                     "  plays hetero-marriage:husband;" +
                                     "" +
                                     " woman sub person," +
                                     "   plays hetero-marriage:wife;" +
                                     "" +
                                     " partnership sub relation," +
                                     "  relates partner;" +
                                     "" +
                                     " marriage sub partnership," +
                                     "  relates spouse as partner;" +
                                     "" +
                                     " hetero-marriage sub marriage," +
                                     "  relates husband as spouse," +
                                     "  relates wife as spouse;");
        TypeInference typeInference = transaction.logic().typeInference();

        String queryString = "match $r (spouse: $yoko, $role: $john) isa $m; $john isa man;";
        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$yoko", set("person", "woman", "man"));
            put("$john", set("man"));
            put("$role", set("hetero-marriage:husband", "marriage:spouse", "partnership:partner", "relation:role"));
            put("$r", set("hetero-marriage", "marriage"));
            put("$m", set("hetero-marriage", "marriage", "partnership", "relation", "thing"));
            put("$_relation:spouse", set("marriage:spouse"));
            put("$_man", set("man"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void relation_anon_isa() throws IOException {
        define_standard_schema("basic-schema");
        TypeInference typeInference = transaction.logic().typeInference();

        String queryString = "match (wife: $yoko);";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$yoko", set("woman"));
            put("$_0", set("marriage"));
            put("$_relation:wife", set("marriage:wife"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void no_role_type() throws IOException {
        define_standard_schema("basic-schema");
        TypeInference typeInference = transaction.logic().typeInference();

        String queryString = "match ($yoko) isa marriage;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$yoko", set("man", "woman", "person"));
            put("$_0", set("marriage"));
            put("$_marriage", set("marriage"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void relation_multiple_roles() throws IOException {
        define_standard_schema("basic-schema");
        TypeInference typeInference = transaction.logic().typeInference();

        String queryString = "match $r (husband: $john, $role: $yoko, $a) isa marriage;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$yoko", set("person", "man", "woman"));
            put("$john", set("man"));
            put("$role", set("marriage:husband", "marriage:wife", "marriage:spouse", "relation:role"));
            put("$r", set("marriage"));
            put("$a", set("person", "man", "woman"));
            put("$_marriage:husband", set("marriage:husband"));
            put("$_marriage", set("marriage"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void has_reverse() throws IOException {
        define_standard_schema("basic-schema");
        TypeInference typeInference = transaction.logic().typeInference();

        String queryString = "match" +
                "  $p isa! person;" +
                "  $p has $a;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$a", set("name", "email"));
            put("$p", set("person"));
            put("$_person", set("person"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void negations_ignored() throws IOException {
        define_standard_schema("basic-schema");
        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match" +
                "  $p isa person;" +
                "  not {$p isa man;};";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$p", set("person", "man", "woman"));
            put("$_person", set("person"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void up_down_hierarchy_isa() {
        define_custom_schema(
                "define" +
                        "  animal sub entity;" +
                        "  person sub animal;" +
                        "  man sub person;" +
                        "  greek sub man;" +
                        "  socrates sub greek;"
        );
        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match" +
                "  $p isa man;" +
                "  man sub $q;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$p", set("man", "greek", "socrates"));
            put("$q", set("thing", "entity", "animal", "person", "man"));
            put("$_man", set("man"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void test_type_var_with_label() throws IOException {
        define_standard_schema("basic-schema");
        TypeInference typeInference = transaction.logic().typeInference();

        String queryString = "match $t type shape;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$t", set("shape"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void plays_hierarchy() throws IOException {
        define_standard_schema("basic-schema");
        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match (spouse: $john) isa marriage;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$john", set("person", "man", "woman"));
            put("$_0", set("marriage"));
            put("$_marriage:spouse", set("marriage:spouse"));
            put("$_marriage", set("marriage"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void has_hierarchy() {
        define_custom_schema(
                "define" +
                        "  animal sub entity, abstract, owns weight;" +
                        "  person sub animal, owns leg-weight as weight;" +
                        "  chair sub entity, owns leg-weight;" +
                        "  dog sub animal;" +
                        "  weight sub attribute, value long, abstract;" +
                        "  leg-weight sub weight;"
        );
        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match" +
                "  $a has weight $c;" +
                "  $b has leg-weight 5;" +
                "  $p has weight $c;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$a", set("person", "chair"));
            put("$b", set("person", "chair"));
            put("$c", set("leg-weight"));
            put("$p", set("person", "chair"));
            put("$_0", set("leg-weight"));
            put("$_weight", set("weight"));
            put("$_leg-weight", set("leg-weight"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void has_with_minimal_cycle() {
        define_custom_schema("define " +
                                     "unit sub attribute, value string, owns unit, owns ref;" +
                                     "ref sub attribute, value long;");
        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match" +
                "  $a has $a;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);
        Map<String, Set<String>> expectedExhaustive = new HashMap<>() {{
            put("$a", set("unit"));
        }};

        assertEquals(expectedExhaustive, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void all_things_is_empty_set() throws IOException {
        define_standard_schema("basic-schema");
        TypeInference typeInference = transaction.logic().typeInference();

        String queryString = "match $x isa thing;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$x", set("animal", "mammal", "reptile", "tortoise", "person", "man", "woman", "dog", "name", "email",
                          "marriage", "triangle", "right-angled-triangle", "square", "perimeter", "area", "hypotenuse-length", "label"));
            put("$_thing", set("thing"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void branched_isa() throws IOException {
        define_custom_schema(
                "define" +
                        "  person sub entity;" +
                        "  man sub person, owns man-name;" +
                        "  woman sub person, owns woman-name;" +
                        "  man-name sub attribute, value string;" +
                        "  woman-name sub attribute, value string;" +
                        ""
        );
        TypeInference typeInference = transaction.logic().typeInference();

        String queryString = "match $x isa $t; $y isa $t; $x has man-name 'bob'; $y has woman-name 'alice';";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expectedExhaustive = new HashMap<>() {{
            put("$x", set("man"));
            put("$y", set("woman"));
            put("$t", set("thing", "entity", "person"));
            put("$_0", set("man-name"));
            put("$_1", set("woman-name"));
            put("$_man-name", set("man-name"));
            put("$_woman-name", set("woman-name"));
        }};

        assertEquals(expectedExhaustive, resolvedTypeMap(disjunction.conjunctions().get(0)));
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
        TypeInference typeInference = transaction.logic().typeInference();

        String queryString = "match $x isa $y;" +
                "  $y sub $z;" +
                "  $z sub $w;" +
                "  $w sub person;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$x", set("person", "man", "greek", "socrates"));
            put("$y", set("person", "man", "greek", "socrates"));
            put("$z", set("person", "man", "greek", "socrates"));
            put("$w", set("person", "man", "greek", "socrates"));
            put("$_person", set("person"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    // When a hint label exists, it can "skip" a generation, meaning a hint and the hint's descendent is possible, yet
    // none of the hint's direct children are possible.
    // We show this below on the hint labels of $t
    // We also show on $a that hints can be isolated form each other completely hierarchy-wise.
    @Test
    //TODO: ignored as the gap doesn't appear. Unclear if this is a resolver or traversal bug.
    @Ignore
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
        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match " +
                "  $a isa $t; " +
                "  $b isa $t; " +
                "  $t owns $c; " +
                "  $t sub entity; " +
                "  ($a, $b) isa $rel; " +
                "  $rel owns $c; " +
                "  $a has left-attr true; " +
                "  $b has right-attr true;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$t", set("animal", "person"));
            put("$a", set("tortoise", "man"));
            put("$b", set("woman"));
            put("$rel", set("ownership", "marriage"));
            put("$c", set("ownership-attr", "marriage-attr"));
        }};
        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void multiple_anonymous_vars() throws IOException {
        define_standard_schema("basic-schema");
        String queryString = "match $a has name 'fido'; $a has label 'poodle';";
        TypeInference typeInference = transaction.logic().typeInference();
        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$a", set("dog"));
            put("$_name", set("name"));
            put("$_label", set("label"));
            put("$_0", set("name"));
            put("$_1", set("label"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void matching_rp_in_relation_that_cant_play_that_role_sets_conjunction_not_satisfiable() throws IOException {
        define_standard_schema("test-type-inference");

        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match " +
                " $x isa company;" +
                " ($x) isa friendship;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);
        assertFalse(disjunction.isCoherent());
    }

    @Test
    public void overridden_relates_are_valid() {
        define_custom_schema("define" +
                                     " marriage sub relation, relates spouse;" +
                                     " hetero-marriage sub marriage," +
                                     "   relates husband as spouse, relates wife as spouse;" +
                                     " person sub entity, plays marriage:spouse, plays hetero-marriage:husband," +
                                     "   plays hetero-marriage:wife;"
        );
        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match $m (spouse: $x, spouse: $y) isa marriage;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$x", set("person"));
            put("$y", set("person"));
            put("$m", set("marriage", "hetero-marriage"));
            put("$_marriage:spouse", set("marriage:spouse"));
            put("$_marriage", set("marriage"));
        }};
        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void converts_root_types() throws IOException {
        define_standard_schema("test-type-inference");
        TypeInference typeInference = transaction.logic().typeInference();
        String relationString = "match $x isa relation;";

        Disjunction relationDisjunction = createDisjunction(relationString);
        typeInference.infer(relationDisjunction);
        Map<String, Set<String>> relationExpected = new HashMap<>() {{
            put("$x", set("friendship", "employment"));
            put("$_relation", set("relation"));
        }};
        assertEquals(relationExpected, resolvedTypeMap(relationDisjunction.conjunctions().get(0)));

        String attributeString = "match $x isa attribute;";
        Disjunction attributeDisjunction = createDisjunction(attributeString);
        typeInference.infer(attributeDisjunction);
        Map<String, Set<String>> attributeExpected = new HashMap<>() {{
            put("$x", set("name", "age", "ref"));
            put("$_attribute", set("attribute"));
        }};

        assertEquals(attributeExpected, resolvedTypeMap(attributeDisjunction.conjunctions().get(0)));

        String entityString = "match $x isa entity;";
        Disjunction entityDisjunction = createDisjunction(entityString);
        typeInference.infer(entityDisjunction);
        Map<String, Set<String>> entityExpected = new HashMap<>() {{
            put("$x", set("person", "company"));
            put("$_entity", set("entity"));
        }};
        assertEquals(entityExpected, resolvedTypeMap(entityDisjunction.conjunctions().get(0)));

        String roleString = "match ($role: $x) isa relation;";
        Disjunction roleDisjunction = createDisjunction(roleString);
        typeInference.infer(roleDisjunction);
        Map<String, Set<String>> roleExpected = new HashMap<>() {{
            put("$role", set("friendship:friend", "employment:employer", "employment:employee", "relation:role"));
            put("$x", set("person", "company"));
            put("$_0", set("friendship", "employment"));
            put("$_relation", set("relation"));
        }};
        assertEquals(roleExpected, resolvedTypeMap(roleDisjunction.conjunctions().get(0)));

        String thingString = "match $x isa thing;";
        Disjunction thingDisjunction = createDisjunction(thingString);
        typeInference.infer(thingDisjunction);

        Map<String, Set<String>> thingExpected = new HashMap<>() {{
            put("$x", set("person", "company", "friendship", "employment", "name", "age", "ref"));
            put("$_thing", set("thing"));
        }};
        assertEquals(thingExpected, resolvedTypeMap(thingDisjunction.conjunctions().get(0)));
    }

    @Test
    public void infers_value_type_recursively() throws IOException {
        define_standard_schema("basic-schema");

        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match " +
                " $x = $y;" +
                " $y = $z;" +
                " $z = $w;" +
                " $w = 1;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$x", set("perimeter", "area", "hypotenuse-length"));
            put("$y", set("perimeter", "area", "hypotenuse-length"));
            put("$z", set("perimeter", "area", "hypotenuse-length"));
            put("$w", set("perimeter", "area", "hypotenuse-length"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void infers_value_cycle() throws IOException {
        define_custom_schema(
                "define" +
                        "  person sub entity, owns name, owns height;" +
                        "  name sub attribute, value string, owns nickname, owns height;" +
                        "  nickname sub attribute, value string, owns name;" +
                        "  surname sub attribute, value string, owns name;" +
                        "  name sub attribute, value string;" +
                        "  surname sub attribute, value string;" +
                        "  nickname sub attribute, value string;" +
                        "  height sub attribute, value double, owns weight, owns name;" +
                        "  weight sub attribute, value double, owns height;" +
                        "  ");

        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match " +
                " $x = 'bob';" +
                " $y = $x;" +
                " $x has $y;" +
                " $y has $x;";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$x", set("nickname", "name"));
            put("$y", set("nickname", "name"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void multiple_value_types_returns_unsatisfiable_error() throws IOException {
        define_standard_schema("basic-schema");
        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match $x = 2; $x = 'bob';";
        Disjunction disjunction = createDisjunction(queryString);

        assertThrows(
                () -> typeInference.infer(disjunction)
        );
    }

    @Test
    public void infer_is_attribute_from_ownership() throws IOException {
        define_standard_schema("test-type-inference");
        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match $x has $y;";
        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$x", set("person", "company", "friendship", "employment"));
            put("$y", set("name", "age", "ref"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void infer_is_attribute_from_having_value() throws IOException {
        define_standard_schema("test-type-inference");
        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match $x = $y;";
        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$x", set("name", "age", "ref"));
            put("$y", set("name", "age", "ref"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void infer_key_attributes() {
        define_custom_schema("define" +
                                     " person sub entity, owns name @key;" +
                                     " name sub attribute, value string;"
        );

        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match $x has name 'bob';";

        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);

        Map<String, Set<String>> expected = new HashMap<>() {{
            put("$x", set("person"));
            put("$_0", set("name"));
            put("$_name", set("name"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

    @Test
    public void role_labels_reduced_by_full_type_resolver() {
        define_custom_schema("define" +
                                     " person sub entity, plays partnership:partner;" +
                                     " partnership sub relation, relates partner;" +
                                     " business sub relation, relates partner;"
        );

        TypeInference typeInference = transaction.logic().typeInference();
        String queryString = "match (partner: $x);";
        Disjunction disjunction = createDisjunction(queryString);
        Conjunction conjunction = disjunction.conjunctions().get(0);
        typeInference.propagateLabels(conjunction);

        Set<String> expectedLabels = set("partnership:partner", "business:partner");

        assertEquals(expectedLabels, resolvedTypeMap(conjunction).get("$_relation:partner"));

        typeInference.infer(conjunction, false);
        Set<String> expectedResolvedTypes = set("partnership:partner");

        assertEquals(expectedResolvedTypes, resolvedTypeMap(disjunction.conjunctions().get(0)).get("$_relation:partner"));
    }

    @Test
    public void roles_can_handle_type_constraints() throws IOException {
        define_standard_schema("basic-schema");
        String queryString = "match ($role: $x) isa $y; $role type marriage:wife;";
        TypeInference typeInference = transaction.logic().typeInference();
        Disjunction disjunction = createDisjunction(queryString);
        typeInference.infer(disjunction);
        HashMap<String, Set<String>> expected = new HashMap<>() {{
            put("$y", set("marriage", "relation", "thing"));
            put("$x", set("woman"));
            put("$role", set("marriage:wife"));
            put("$_0", set("marriage"));
        }};

        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0)));
    }

//    TEST RULE INSERT TODO: should be moved to rule test

    @Test
    public void cannot_insert_unsatisfiable_rule() throws IOException {
        define_standard_schema("basic-schema");
        assertThrows(() -> transaction.logic().putRule(
                "animals-are-named-fido",
                TypeQL.parsePattern("{$x isa animal;}").asConjunction(),
                TypeQL.parseVariable("$x has name 'fido'").asThing()));
    }

    @Test
    public void cannot_insert_abstract_attributes() {
        define_custom_schema("define " +
                                     " person sub entity, owns name;" +
                                     " woman sub person, owns maiden-name as name;" +
                                     " name sub attribute, value string, abstract;" +
                                     " maiden-name sub name, value string;");
        String queryString = "match $x isa woman, has name 'smith';";
        Disjunction disjunction = createDisjunction(queryString);
        transaction.logic().typeInference().infer(disjunction);

        assertThrows(() -> transaction.logic().putRule(
                "women-called-smith",
                TypeQL.parsePattern("{$x isa woman;}").asConjunction(),
                TypeQL.parseVariable("$x has name 'smith'").asThing()));
    }

    @Test
    public void cannot_insert_if_relation_type_too_general() {
        define_custom_schema("define" +
                                     " person sub entity, plays marriage:husband, plays marriage:wife;" +
                                     " partnership sub relation, relates partner;" +
                                     " marriage sub partnership, relates husband as partner, relates wife as partner;");
        String queryString = "match $x isa person; (wife: $x) isa partnership;";
        Disjunction disjunction = createDisjunction(queryString);
        transaction.logic().typeInference().infer(disjunction);

        assertThrows(() -> transaction.logic().putRule(
                "marriage-rule",
                TypeQL.parsePattern("{$x isa person;}").asConjunction(),
                TypeQL.parseVariable("(wife: $x) isa partnership").asThing()));
    }

    @Test
    public void cannot_insert_if_role_is_too_general() {
        define_custom_schema("define" +
                                     " person sub entity, plays marriage:husband, plays marriage:wife;" +
                                     " partnership sub relation, relates partner;" +
                                     " marriage sub partnership, relates husband as partner, relates wife as partner;");
        String queryString = "match $x isa person; (partner: $x) isa marriage;";
        Disjunction disjunction = createDisjunction(queryString);
        transaction.logic().typeInference().infer(disjunction);

        assertThrows(() -> transaction.logic().putRule(
                "marriage-rule",
                TypeQL.parsePattern("{$x isa person;}").asConjunction(),
                TypeQL.parseVariable("(partner: $x) isa marriage").asThing()));
    }

    @Test
    public void variable_relations_allowed_only_if_all_possibilities_are_insertable() {
        define_custom_schema("define" +
                                     " person sub entity, plays marriage:husband, plays marriage:wife;" +
                                     " partnership sub relation, relates partner;" +
                                     " marriage sub partnership, relates husband as partner, relates wife as partner;");
        transaction.logic().putRule(
                "marriage-rule",
                TypeQL.parsePattern("{$x isa person; $t isa marriage;}").asConjunction(),
                TypeQL.parseVariable("(wife: $x) isa $t").asThing());
    }


    @Test
    public void nested_negation_outer_scope_correctly_reduces_resolved_types() {
        define_custom_schema("define " +
                                     "person sub entity, plays marriage:spouse;" +
                                     "woman sub person;" +
                                     "marriage sub relation, relates spouse;");

        String minimallyRestricted = "match $x isa person; not { ($x) isa marriage; };";
        Disjunction disjunction = createDisjunction(minimallyRestricted);
        transaction.logic().typeInference().infer(disjunction);
        HashMap<String, Set<String>> expected = new HashMap<>() {{
            put("$x", set("person", "woman"));
            put("$_0", set("marriage"));
            put("$_marriage", set("marriage"));
        }};
        // test the inner negation
        assertEquals(expected, resolvedTypeMap(disjunction.conjunctions().get(0).negations().iterator().next().disjunction().conjunctions().get(0)));

        String restricted = "match $x isa woman; not { ($x) isa marriage; };";
        Disjunction restrictedDisjunction = createDisjunction(restricted);
        transaction.logic().typeInference().infer(restrictedDisjunction);
        expected = new HashMap<>() {{
            put("$x", set("woman"));
            put("$_0", set("marriage"));
            put("$_marriage", set("marriage"));
        }};
        // test the inner negation
        assertEquals(expected, resolvedTypeMap(restrictedDisjunction.conjunctions().get(0).negations().iterator().next().disjunction().conjunctions().get(0)));
    }


    /**
     * If the type resolver conflates anonymous or generated variables between the inner or outer nestings,
     * we will see it here
     */
    @Test
    public void nested_negation_is_satisfiable() {
        define_custom_schema("define session sub entity,\n" +
                                     "          plays reported-fault:parent-session,\n" +
                                     "          plays unanswered-question:parent-session;\n" +
                                     "      fault sub entity,\n" +
                                     "          plays reported-fault:relevant-fault,\n" +
                                     "          plays fault-identification:identified-fault;\n" +
                                     "      question sub entity,\n" +
                                     "          plays fault-identification:identifying-question,\n" +
                                     "          plays unanswered-question:question-not-answered;\n" +
                                     "      reported-fault sub relation,\n" +
                                     "          relates relevant-fault,\n" +
                                     "          relates parent-session;\n" +
                                     "      unanswered-question sub relation,\n" +
                                     "          relates question-not-answered,\n" +
                                     "          relates parent-session;\n" +
                                     "      fault-identification sub relation,\n" +
                                     "          relates identifying-question,\n" +
                                     "          relates identified-fault;\n"
        );
        String query = "match (relevant-fault: $flt, parent-session: $ts) isa reported-fault;\n" +
                "          not {\n" +
                "              (question-not-answered: $ques, parent-session: $ts) isa unanswered-question;\n" +
                "              ($flt, $ques) isa fault-identification;" +
                "          };";
        Disjunction disjunction = createDisjunction(query);
        transaction.logic().typeInference().infer(disjunction);
        assertTrue(disjunction.conjunctions().get(0).negations().iterator().next().disjunction().conjunctions().get(0).isCoherent());
    }
}
