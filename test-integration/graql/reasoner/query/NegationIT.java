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
 */

package grakn.core.graql.reasoner.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Label;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.graql.exception.GraqlSemanticException;
import grakn.core.graql.reasoner.graph.ReachabilityGraph;
import grakn.core.graql.reasoner.utils.ReasonerUtils;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Negation;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static grakn.core.util.GraqlTestUtil.assertCollectionsEqual;
import static grakn.core.util.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static grakn.core.util.GraqlTestUtil.loadFromFileAndCommit;
import static graql.lang.Graql.and;
import static graql.lang.Graql.match;
import static graql.lang.Graql.not;
import static graql.lang.Graql.or;
import static graql.lang.Graql.var;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotEquals;

public class NegationIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    private static SessionImpl negationSession;
    private static SessionImpl recipeSession;
    private static SessionImpl reachabilitySession;

    @BeforeClass
    public static void loadContext(){
        negationSession = server.sessionWithNewKeyspace();
        String resourcePath = "test-integration/graql/reasoner/stubs/";
        loadFromFileAndCommit(resourcePath,"negation.gql", negationSession);
        recipeSession = server.sessionWithNewKeyspace();
        loadFromFileAndCommit(resourcePath,"recipeTest.gql", recipeSession);
        reachabilitySession = server.sessionWithNewKeyspace();
        ReachabilityGraph reachability = new ReachabilityGraph(reachabilitySession);
        reachability.load(3);
    }

    @AfterClass
    public static void closeSession(){
        negationSession.close();
    }

    @org.junit.Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test (expected = GraqlSemanticException.class)
    public void whenNegatingSinglePattern_exceptionIsThrown () {
        try(TransactionOLTP tx = negationSession.transaction().write()) {
            Negation<Pattern> pattern = not(var("x").has("attribute", "value"));
            ReasonerQueries.composite(Iterables.getOnlyElement(pattern.getNegationDNF().getPatterns()), tx);
        }
    }

    @Test (expected = GraqlSemanticException.class)
    public void whenExecutingUnboundNegationPattern_exceptionIsThrown () {
        try(TransactionOLTP tx = negationSession.transaction().write()) {
            Negation<Pattern> pattern = not(var("x").has("attribute", "value"));
            tx.execute(match(pattern));
        }
    }

    @Test (expected = GraqlSemanticException.class)
    public void whenIncorrectlyBoundNestedNegationBlock_exceptionIsThrown () {
        try(TransactionOLTP tx = negationSession.transaction().write()) {
            Conjunction<?> pattern = and(
                    var("r").isa("entity"),
                    not(
                            and(
                                    var().rel("r2").rel("i"),
                                    and(var("i").isa("entity"))
                            )
                    )
            );
            ReasonerQueries.composite(Iterables.getOnlyElement(pattern.getNegationDNF().getPatterns()), tx);
        }
    }

    @Test (expected = GraqlSemanticException.class)
    public void whenExecutingIncorrectlyBoundNestedNegationBlock_exceptionIsThrown () {
        try(TransactionOLTP tx = negationSession.transaction().write()) {
            Conjunction<?> pattern = and(
                    var("r").isa("entity"),
                    not(
                            and(
                                    var().rel("r2").rel("i"),
                                    and(var("i").isa("entity"))
                            )
                    )
            );
            tx.execute(match(pattern));
        }
    }

    @Test (expected = GraqlSemanticException.class)
    public void whenNegationBlockContainsDisjunction_exceptionIsThrown(){
        try(TransactionOLTP tx = negationSession.transaction().write()) {
            Conjunction<?> pattern = and(
                    var("x").isa("someType"),
                    not(
                            or(
                                    var("x").has("resource-string", "value"),
                                    var("x").has("resource-string", "someString")
                            )
                    )
            );
            ReasonerQueries.composite(Iterables.getOnlyElement(pattern.getNegationDNF().getPatterns()), tx);
        }
    }

    @Test (expected = GraqlSemanticException.class)
    public void whenExecutingNegationBlockContainingDisjunction_exceptionIsThrown () {
        try(TransactionOLTP tx = negationSession.transaction().write()) {
            Conjunction<?> pattern = and(
                    var("x").isa("someType"),
                    not(
                            or(
                                    var("x").has("resource-string", "value"),
                                    var("x").has("resource-string", "someString")
                            )
                    )
            );
            tx.execute(match(pattern));
        }
    }

    @Test (expected = GraqlSemanticException.class)
    public void whenExecutingNegationQueryWithReasoningOff_exceptionIsThrown () {
        try(TransactionOLTP tx = negationSession.transaction().write()) {
            Conjunction<?> pattern = and(
                    var("x").isa("entity"),
                    not(
                            var("x").has("attribute", "value")
                    )
            );
            tx.execute(match(pattern), false);
        }
    }

    @Test
    public void whenATypeInRuleNegationBlockIsAbsent_theRuleIsMatched() {
        try (TransactionOLTP tx = negationSession.transaction().write()) {
            assertFalse(tx.getAttributeType("absent-resource").instances().findFirst().isPresent());

            List<ConceptMap> answers = tx.execute(
                    match(
                            var("x").has("derived-resource-string", "no absent-resource attached")
                    ).get());

            List<ConceptMap> explicitAnswers = tx.execute(match(var("x").isa("someType")).get());
            assertCollectionsNonTriviallyEqual(explicitAnswers,answers);
        }
    }

    @Test
    public void conjunctionOfRelations_filteringSpecificRolePlayerType(){
        try(TransactionOLTP tx = negationSession.transaction().write()) {
            String unwantedLabel = "anotherType";
            EntityType unwantedType = tx.getEntityType(unwantedLabel);

            List<ConceptMap> answersWithoutSpecificRoleplayerType = tx.execute(Graql.<GraqlGet>parse(
                    "match " +
                            "(someRole: $x, otherRole: $y) isa binary;" +
                            "not {$q isa " + unwantedLabel + ";};" +
                            "(someRole: $y, otherRole: $q) isa binary;" +
                            "(someRole: $y, otherRole: $z) isa binary;" +
                            "get;"
            ));

            List<ConceptMap> fullAnswers = tx.execute(Graql.<GraqlGet>parse(
                    "match " +
                            "(someRole: $x, otherRole: $y) isa binary;" +
                            "(someRole: $y, otherRole: $q) isa binary;" +
                            "(someRole: $y, otherRole: $z) isa binary;" +
                            "get;"
            ));

            List<ConceptMap> expectedAnswers = fullAnswers.stream().filter(ans -> !ans.get("q").asThing().type().equals(unwantedType))
                    .collect(toList());

            assertCollectionsNonTriviallyEqual(
                    expectedAnswers,
                    answersWithoutSpecificRoleplayerType
            );
        }
    }

    @Test
    public void conjunctionOfRelations_filteringSpecificUnresolvableConnection(){
        try(TransactionOLTP tx = negationSession.transaction().write()) {
            String unwantedLabel = "anotherType";
            String connection = "binary";

            List<ConceptMap> answersWithoutSpecificConnection = tx.execute(Graql.<GraqlGet>parse(
                    "match " +
                            "(someRole: $x, otherRole: $y) isa binary;" +
                            "$q isa " + unwantedLabel + ";" +
                            "not {(someRole: $y, otherRole: $q) isa " + connection + ";};" +
                            "(someRole: $y, otherRole: $z) isa binary;" +
                            "get;"
            ));

            List<ConceptMap> fullAnswers = tx.execute(Graql.<GraqlGet>parse(
                    "match " +
                            "(someRole: $x, otherRole: $y) isa binary;" +
                            "$q isa " + unwantedLabel + ";" +
                            "(someRole: $y, otherRole: $z) isa binary;" +
                            "get;"
            ));

            List<ConceptMap> expectedAnswers = fullAnswers.stream()
                    .filter(ans -> !thingsRelated(
                            ImmutableMap.of(
                                    ans.get("y").asThing(), tx.getRole("someRole"),
                                    ans.get("q").asThing(), tx.getRole("otherRole")),
                            Label.of(connection),
                            tx)
                    ).collect(toList());

            assertCollectionsNonTriviallyEqual(
                    expectedAnswers,
                    answersWithoutSpecificConnection
            );
        }
    }

    @Test
    public void conjunctionOfRelations_filteringSpecificResolvableConnection(){
        try (TransactionOLTP tx = negationSession.transaction().write()) {
            String unwantedLabel = "anotherType";
            String connection = "derived-binary";

            List<ConceptMap> answersWithoutSpecificConnection = tx.execute(Graql.<GraqlGet>parse(
                    "match " +
                            "(someRole: $x, otherRole: $y) isa binary;" +
                            "$q isa " + unwantedLabel + ";" +
                            "not {(someRole: $y, otherRole: $q) isa " + connection + ";};" +
                            "(someRole: $y, otherRole: $z) isa binary;" +
                            "get;"
            ));

            List<ConceptMap> fullAnswers = tx.execute(Graql.<GraqlGet>parse(
                    "match " +
                            "$q isa " + unwantedLabel + ";" +
                            "(someRole: $x, otherRole: $y) isa binary;" +
                            "(someRole: $y, otherRole: $z) isa binary;" +
                            "get;"
            ));

            List<ConceptMap> expectedAnswers = fullAnswers.stream()
                    .filter(ans -> !thingsRelated(
                            ImmutableMap.of(
                                    ans.get("y").asThing(), tx.getRole("someRole"),
                                    ans.get("q").asThing(), tx.getRole("otherRole"))
                            ,
                            Label.of(connection),
                            tx)
                    ).collect(toList());

            assertCollectionsNonTriviallyEqual(
                    expectedAnswers,
                    answersWithoutSpecificConnection
            );
        }
    }

    @Test
    public void entitiesWithoutSpecificAttributeValue(){
        try(TransactionOLTP tx = negationSession.transaction().write()) {
            String specificStringValue = "value";

            List<ConceptMap> answersWithoutSpecificStringValue = tx.execute(
                    match(
                            var("x").isa("entity"),
                            not(var("x").has("attribute", specificStringValue))
                    ).get()
            );

            List<ConceptMap> expectedAnswers = tx.stream(match(var("x").isa("entity")).get())
                    .filter(ans -> ans.get("x").asThing().attributes().noneMatch(a -> a.value().equals(specificStringValue)))
                    .collect(toList());

            assertCollectionsEqual(
                    expectedAnswers,
                    answersWithoutSpecificStringValue
            );
        }
    }

    @Test
    public void entitiesWithAttributeNotEqualToSpecificValue(){
        try(TransactionOLTP tx = negationSession.transaction().write()) {
            String specificStringValue = "unattached";


            List<ConceptMap> answersWithoutSpecificStringValue = tx.execute(
                    match(
                            var("x").has("attribute", var("r")),
                            not(var("r").val(specificStringValue))
                    ).get()
            );

            List<ConceptMap> expectedAnswers = tx.stream(match(var("x").has("attribute", var("r"))).get())
                    .filter(ans -> !ans.get("r").asAttribute().value().equals(specificStringValue))
                    .collect(toList());

            assertCollectionsNonTriviallyEqual(
                    expectedAnswers,
                    answersWithoutSpecificStringValue
            );
        }
    }

    @Test
    public void entitiesHavingAttributesThatAreNotOfSpecificType(){
        try(TransactionOLTP tx = negationSession.transaction().write()) {
            String specificTypeLabel = "anotherType";
            EntityType specificType = tx.getEntityType(specificTypeLabel);

            List<ConceptMap> answersWithoutSpecificType = tx.execute(match(
                    var("x").has("attribute", var("r")),
                    not(var("x").isa(specificTypeLabel))
            ).get());

            List<ConceptMap> fullAnswers = tx.execute(match(var("x").has("attribute", var("r"))).get());
            List<ConceptMap> expectedAnswers = fullAnswers.stream()
                    .filter(ans -> !ans.get("x").asThing().type().equals(specificType)).collect(toList());
            assertCollectionsNonTriviallyEqual(expectedAnswers, answersWithoutSpecificType);
        }
    }

    @Test
    public void entitiesNotHavingRolePlayersInRelations(){
        try(TransactionOLTP tx = negationSession.transaction().write()) {
            String connection = "relation";
            List<ConceptMap> fullAnswers = tx.execute(match(var("x").has("attribute", var("r"))).get());

            List<ConceptMap> answersNotPlayingInRelation = tx.execute(
                    match(
                            var("x").has("attribute", var("r")),
                            not(var().rel("x").isa("relation"))
            ).get());

            List<ConceptMap> expectedAnswers = fullAnswers.stream()
                    .filter(ans -> !thingsRelated(
                            ImmutableMap.of(ans.get("x").asThing(), tx.getMetaRole()),
                            Label.of(connection),
                            tx))
                    .collect(toList());
            assertCollectionsEqual(
                    expectedAnswers,
                    answersNotPlayingInRelation
            );
        }
    }

    @Test
    public void excludingAConceptWithSpecificId(){
        try (TransactionOLTP tx = negationSession.transaction().write()) {
            Concept specificConcept = tx.stream(
                    match(var("x").isa("someType").has("resource-string", "value")).get()
            ).findFirst().orElse(null).get("x");

            GraqlGet query = match(
                    var().rel(var("rp")),
                    not(var("rp").id(specificConcept.id().getValue()))
                    ).get();

            List<ConceptMap> answers = tx.execute(query.asGet());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> assertNotEquals(ans.get("rp"), specificConcept));
        }
    }

    @Test
    public void excludingASpecificRole(){
        try (TransactionOLTP tx = negationSession.transaction().write()) {
            Relation relation = tx.getRelationType("binary").instances().findFirst().orElse(null);
            Role metaRole = tx.getMetaRole();

            GraqlGet query = match(
                    and(
                            var("r").id(relation.id().getValue()),
                            var("r").rel(var("rl"), var("rp")),
                            not(var("rl").type("role"))
                    )).get("rl", "rp");

            List<ConceptMap> answers = tx.execute(query.asGet());
            assertFalse(answers.isEmpty());
            answers.forEach(ans -> assertNotEquals(ans.get("rl"), metaRole));
        }
    }

    @Test
    public void negateMultiplePropertyStatement(){
        try(TransactionOLTP tx = negationSession.transaction().write()) {
            String specificValue = "value";
            String specificTypeLabel = "someType";
            String anotherSpecificValue = "attached";

            List<ConceptMap> answersWithoutSpecifcTypeAndValue = tx.execute(match(
                    var("x").has("attribute", var("r")),
                    not(and(
                            var("x")
                                    .isa(specificTypeLabel)
                                    .has("resource-string", specificValue)
                                    .has("derived-resource-string", anotherSpecificValue)
                            )
                    )
            ).get());

            List<ConceptMap> equivalentAnswers = tx.execute(match(
                    var("x").has("attribute", var("r")),
                    not(and(
                            var("x").isa(specificTypeLabel),
                            var("x").has("resource-string", specificValue),
                            var("x").has("derived-resource-string", anotherSpecificValue)
                            )
                    )
            ).get());

            assertCollectionsNonTriviallyEqual(equivalentAnswers, answersWithoutSpecifcTypeAndValue);
        }
    }

    @Test
    public void negateMultipleStatements(){
        try(TransactionOLTP tx = negationSession.transaction().write()) {
            String anotherSpecificValue = "value";
            String specificTypeLabel = "anotherType";
            EntityType specificType = tx.getEntityType(specificTypeLabel);

            List<ConceptMap> fullAnswers = tx.execute(Graql.parse("match $x has attribute $r;get;").asGet());

            List<ConceptMap> answersWithoutSpecificTypeAndValue = tx.execute(match(
                    var("x").has("attribute", var("r")),
                    not(var("x").isa(specificTypeLabel)),
                    not(var("r").val(anotherSpecificValue))
            ).get());

            List<ConceptMap> expectedAnswers = fullAnswers.stream()
                    .filter(ans -> !ans.get("r").asAttribute().value().equals(anotherSpecificValue))
                    .filter(ans -> !ans.get("x").asThing().type().equals(specificType))
                    .collect(toList());
            assertCollectionsNonTriviallyEqual(
                    expectedAnswers,
                    answersWithoutSpecificTypeAndValue
            );
        }
    }

    @Test
    public void whenNegatingGroundTransitiveRelation_queryTerminates(){
        try(TransactionOLTP tx = negationSession.transaction().write()) {
            Concept start = tx.execute(match(
                    var("x").isa("someType").has("resource-string", "value")).get()
            ).iterator().next().get("x");

            Concept end = tx.execute(match(
                    var("x").isa("someType").has("resource-string", "someString")).get()
            ).iterator().next().get("x");

            List<ConceptMap> answers = tx.execute(match(
                    not(var().rel("x").rel("y").isa("derived-binary")),
                    var("x").id(start.id().getValue()),
                    var("y").id(end.id().getValue())
            ).get());
            assertTrue(answers.isEmpty());
        }
    }

    @Test
    public void doubleNegation_recipesContainingAllergens(){
        try(TransactionOLTP tx = recipeSession.transaction().write()) {
            Conjunction<?> basePattern = and(
                    var("r").isa("recipe"),
                    var().rel("r").rel("i").isa("requires"),
                    var("i").isa("allergenic-ingredient")
            );
            List<ConceptMap> recipesContainingAllergens = tx.execute(match(basePattern).get("r"));

            List<ConceptMap> doubleNegationEquivalent = tx.execute(match(
                    var("r").isa("recipe"),
                    not(
                            not(basePattern)
                    )
            ).get("r"));

            assertEquals(recipesContainingAllergens, doubleNegationEquivalent);
        }
    }

    @Test
    public void negatedConjunction_allRecipesThatDoNotContainAllergens(){
        try(TransactionOLTP tx = recipeSession.transaction().write()) {
            Conjunction<?> basePattern = and(
                    var("r").isa("recipe"),
                    var().rel("r").rel("i").isa("requires"),
                    var("i").isa("allergenic-ingredient")
            );
            List<ConceptMap> allRecipes = tx.stream(match(var("r").isa("recipe")).get()).collect(Collectors.toList());
            List<ConceptMap> recipesContainingAllergens = tx.execute(match(basePattern).get("r"));

            List<ConceptMap> recipesWithoutAllergenIngredients = tx.execute(match(
                    var("r").isa("recipe"),
                    not(basePattern)
            ).get());
            assertEquals(ReasonerUtils.listDifference(allRecipes, recipesContainingAllergens), recipesWithoutAllergenIngredients);
        }
    }

    @Test
    public void allRecipesContainingAvailableIngredients(){
        try(TransactionOLTP tx = recipeSession.transaction().write()) {
            List<ConceptMap> allRecipes = tx.stream(match(var("r").isa("recipe")).get()).collect(Collectors.toList());
            List<ConceptMap> recipesWithUnavailableIngredientsExplicit = tx.execute(
                    match(
                            var("r").isa("recipe"),
                            var().rel("r").rel("i").isa("requires"),
                            not(var("i").isa("available-ingredient"))
                    ).get("r"));
            
            List<ConceptMap> recipesWithUnavailableIngredients = tx.execute(
                    match(var("r").isa("recipe-with-unavailable-ingredient")).get("r"));

            assertCollectionsNonTriviallyEqual(recipesWithUnavailableIngredientsExplicit, recipesWithUnavailableIngredients);
            List<ConceptMap> recipesWithAllIngredientsAvailableSimple = tx.execute(
                    match(
                            var("r").isa("recipe"),
                            not(var("r").isa("recipe-with-unavailable-ingredient"))
                    ).get("r"));

            List<ConceptMap> recipesWithAllIngredientsAvailableExplicit = tx.execute(match(
                    var("r").isa("recipe"),
                    not(and(
                            var().rel("r").rel("i").isa("requires"),
                            not(and(
                                    var("i").isa("ingredient"),
                                    var().rel("i").isa("containes")
                                    )
                            )
                            )
                    )
            ).get());

            List<ConceptMap> recipesWithAllIngredientsAvailable = tx.execute(match(
                    var("r").isa("recipe"),
                    not(and(
                            var().rel("r").rel("i").isa("requires"),
                            not(var("i").isa("available-ingredient"))
                    ))
            ).get());

            assertCollectionsNonTriviallyEqual(recipesWithAllIngredientsAvailableExplicit, recipesWithAllIngredientsAvailable);
            assertCollectionsNonTriviallyEqual(recipesWithAllIngredientsAvailableExplicit, recipesWithAllIngredientsAvailableSimple);
            assertCollectionsNonTriviallyEqual(recipesWithAllIngredientsAvailable, ReasonerUtils.listDifference(allRecipes, recipesWithUnavailableIngredients));
        }
    }

    @Test
    public void testSemiPositiveProgram(){
        try(TransactionOLTP tx = reachabilitySession.transaction().write()) {
            ConceptMap firstNeighbour = Iterables.getOnlyElement(tx.execute(Graql.parse("match $x has index 'a1';get;").asGet()));
            ConceptId firstNeighbourId = firstNeighbour.get("x").id();
            List<ConceptMap> indirectLinksWithOrigin = tx.execute(
                    Graql.<GraqlGet>parse("match " +
                            "(from: $x, to: $y) isa indirect-link;" +
                            "$x has index 'a';" +
                            "get;"
                    ));

            List<ConceptMap> alternativeRepresentation = tx.execute(
                    Graql.<GraqlGet>parse(
                            "match " +
                                    "(from: $x, to: $y) isa reachable;" +
                                    "$x has index 'a';" +
                                    "not {$y has index 'a1';};" +
                                    "get;"
                    ));

            List<ConceptMap> expected = tx.stream(
                    Graql.<GraqlGet>parse(
                            "match " +
                                    "(from: $x, to: $y) isa reachable;" +
                                    "$x has index 'a';" +
                                    "get;"
                    )).filter(ans -> !ans.get("y").id().equals(firstNeighbourId))
                    .collect(toList());
            assertCollectionsNonTriviallyEqual(alternativeRepresentation, indirectLinksWithOrigin);
            assertCollectionsNonTriviallyEqual(expected, indirectLinksWithOrigin);
        }
    }

    @Test
    public void testStratifiedProgram(){
        try (TransactionOLTP tx = reachabilitySession.transaction().write()) {
            List<ConceptMap> indirectLinksWithOrigin = tx.execute(
                    Graql.<GraqlGet>parse("match " +
                            "(from: $x, to: $y) isa unreachable;" +
                            "$x has index 'a';" +
                            "get;"
                    ));

            Set<ConceptMap> expected = tx.stream(
                    Graql.<GraqlGet>parse(
                            "match " +
                                    "$x has index 'a';" +
                                    "$y isa vertex;" +
                                    "{$y has index contains 'b';} or " +
                                    "{$y has index 'aa';} or " +
                                    "{$y has index 'a';} or " +
                                    "{$y has index 'cc';} or " +
                                    "{$y has index 'dd';};" +
                                    "get;"
                    )).collect(toSet());

            assertCollectionsNonTriviallyEqual(expected, indirectLinksWithOrigin);
        }
    }

    private boolean thingsRelated(Map<Thing, Role> thingMap, Label relation, TransactionOLTP tx){
        RelationType relationType = tx.getRelationType(relation.getValue());
        boolean inferrable = relationType.subs().flatMap(SchemaConcept::thenRules).findFirst().isPresent();

        if (!inferrable){
            return relationType
                    .instances()
                    .anyMatch(r -> thingMap.entrySet().stream().allMatch(e -> r.rolePlayers(e.getValue()).anyMatch(rp -> rp.equals(e.getKey()))));
        }

        Statement pattern = var();
        Set<Statement> patterns = new HashSet<>();
        for(Map.Entry<Thing, Role> entry : thingMap.entrySet()){
            Role role = entry.getValue();
            Thing thing = entry.getKey();
            Statement rpVar = var();
            patterns.add(rpVar.id(thing.id().getValue()));
            pattern = pattern.rel(role.label().getValue(), rpVar);
        }
        patterns.add(pattern.isa(relation.getValue()));
        return tx.stream(match(and(patterns)).get()).findFirst().isPresent();
    }
}
