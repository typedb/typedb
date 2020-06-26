package grakn.core.test.behaviour.resolution.framework.complete;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.behaviour.resolution.framework.common.NegationRemovalVisitor;
import grakn.core.test.behaviour.resolution.framework.common.StatementVisitor;
import grakn.core.test.behaviour.resolution.framework.resolve.QueryBuilder;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.property.IsaProperty;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.test.behaviour.resolution.framework.resolve.QueryBuilder.generateKeyStatements;


public class Completer {

    private static int numInferredConcepts;
    private final Session session;
    private Set<Rule> rules;

    public Completer(Session session) {
        this.session = session;
    }

    public void loadRules(Transaction tx, Set<grakn.core.kb.concept.api.Rule> graknRules) {
        Set<Rule> rules = new HashSet<>();
        for (grakn.core.kb.concept.api.Rule graknRule : graknRules) {
            rules.add(new Rule(Objects.requireNonNull(graknRule.when()), Objects.requireNonNull(graknRule.then()), graknRule.label().toString()));
        }
        this.rules = rules;
    }

    public int complete() {
        boolean allRulesRerun = true;

        while (allRulesRerun) {
            allRulesRerun = false;
            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {

                for (Rule rule : rules) {
                    allRulesRerun = allRulesRerun | completeRule(tx, rule);
                }
                tx.commit();
            }
        }
        return numInferredConcepts;
    }

    private static boolean completeRule(Transaction tx, Rule rule) {

        AtomicBoolean foundResult = new AtomicBoolean(false);
        // TODO When making match queries be careful that user-provided rules could trigger due to elements of the
        //  completion schema. These results should be filtered out.

        QueryBuilder qb = new QueryBuilder();

        // Use the DNF so that we can know each `when` if free of disjunctions. Disjunctions in the `when` will otherwise complicate things significantly
        Set<Conjunction<Pattern>> disjunctiveWhens = rule.when.getNegationDNF().getPatterns();
        for (Conjunction<Pattern> when : disjunctiveWhens) {
            // Get all the places that the `when` could be applied to
            Stream<ConceptMap> whenAnswers = tx.stream(Graql.match(when).get());
            Iterator<ConceptMap> whenIterator = whenAnswers.iterator();
            while (whenIterator.hasNext()) {

                // Get the variables of the rule that connect the `when` and the `then`, since this determines whether an inferred `then` should be duplicated
                Set<Variable> connectingVars = new HashSet<>(rule.when.variables());
                connectingVars.retainAll(rule.then.variables());

                Map<Variable, Concept> whenMap = whenIterator.next().map();

                // Get the concept map for those connected variables by filtering the answer we already have for the `when`
                Map<Variable, Concept> connectingAnswerMap = whenMap.entrySet().stream().filter(entry -> connectingVars.contains(entry.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                // We now have the answer but with only the connecting variables left

                // Get statements to match for the connecting concepts via their keys/uniqueness
                Set<Statement> connectingKeyStatements = generateKeyStatements(connectingAnswerMap);

                List<ConceptMap> thenAnswers = tx.execute(Graql.match(rule.then, Graql.and(connectingKeyStatements)).get());

                // We already know that the rule doesn't contain any disjunctions as we previously used negationDNF,
                // now we make sure negation blocks are removed, so that we know it must be a conjunct set of statements
                NegationRemovalVisitor negationRemover = new NegationRemovalVisitor();
                Pattern ruleResolutionConjunction = negationRemover.visitPattern(qb.ruleResolutionConjunction(rule.when, rule.then, rule.label));

                if (thenAnswers.size() == 0) {
                    // We've found somewhere the rule can be applied
                    HashSet<Statement> matchWhenPatterns = new HashSet<>();
                    matchWhenPatterns.addAll(generateKeyStatements(whenMap));
                    matchWhenPatterns.addAll(rule.when.statements());

                    Set<Statement> insertNewThenStatements = new HashSet<>();
                    insertNewThenStatements.addAll(rule.then.statements());
                    insertNewThenStatements.addAll(getThenKeyStatements(tx, rule.then));
                    insertNewThenStatements.addAll(ruleResolutionConjunction.statements());
                    HashSet<Variable> insertedVars = new HashSet<>(rule.then.variables());
                    insertedVars.removeAll(rule.when.variables());
                    numInferredConcepts += insertedVars.size();

                    // Apply the rule, with the records of how the inference was made
                    List<ConceptMap> inserted = tx.execute(Graql.match(Graql.and(matchWhenPatterns)).insert(insertNewThenStatements));
                    assert inserted.size() == 1;
                    foundResult.set(true);
                } else {
                    thenAnswers.forEach(thenAnswer -> {
                        // If it is *not* inferred, then do nothing, as rules shouldn't infer facts that are already present
//                    if (!isInferred(thenAnswer)) { // TODO check if the answer is inferred
                        // Check if it was this exact rule that previously inserted this `then` for these exact `when` instances

                        Set<Statement> checkStatements = new HashSet<>();
                        checkStatements.addAll(generateKeyStatements(whenMap));
                        checkStatements.addAll(generateKeyStatements(thenAnswer.map()));
                        checkStatements.addAll(ruleResolutionConjunction.statements());
                        checkStatements.addAll(rule.when.statements());
                        checkStatements.addAll(rule.then.statements());
                        List<ConceptMap> ans = tx.execute(Graql.match(checkStatements).get());

                        // Failure here means either a rule has been applied twice in the same place, or something else, perhaps the queries, has gone wrong
                        assert ans.size() <= 1;

                        if (ans.size() == 0) {
                            // This `then` has been previously inferred, but not in this exact scenario, so we add this resolution to the previously inserted inference
                            Set<Statement> matchStatements = new HashSet<>();
                            matchStatements.addAll(generateKeyStatements(whenMap));
                            matchStatements.addAll(generateKeyStatements(thenAnswer.map()));
                            matchStatements.addAll(rule.when.statements());
                            matchStatements.addAll(rule.then.statements());

                            List<ConceptMap> inserted = tx.execute(Graql.match(matchStatements).insert(ruleResolutionConjunction.statements()));
                            assert inserted.size() == 1;
                            foundResult.set(true);
                        }
//                    }
                    });
                }
            }
        }
        return foundResult.get();
    }

    /**
     * When inserting the `then` of a rule, each inserted concept needs to have a key. This function generates
     * those keys randomly given only the statements of the rule's `then`.
     * @param tx transaction
     * @return statements concerning only the keys of the `then`
     */
    // TODO Surprised that this worked, surely it should have inserted additional keys for the concepts in the
    //  `then` that are pre-existing?
    private static HashSet<Statement> getThenKeyStatements(Transaction tx, Pattern then) {
        HashSet<Statement> keyStatements = new HashSet<>();
        then.statements().forEach(s -> {
            s.properties().forEach(p -> {
                if (p instanceof IsaProperty) {
                    // Get the relevant type(s)
                    GraqlGet query = Graql.match(Graql.var("x").sub(((IsaProperty) p).type())).get();
                    List<ConceptMap> ans = tx.execute(query);
                    ans.forEach(a -> {
                        Set<? extends AttributeType> keys = a.get("x").asType().keys().collect(Collectors.toSet());
                        keys.forEach(k -> {
                            String keyTypeLabel = k.label().toString();
                            AttributeType.ValueType<?> v = k.valueType();
                            String randomKeyValue = UUID.randomUUID().toString();

                            assert v != null;
                            if (v.valueClass().equals(Long.class)) {

                                keyStatements.add(Graql.var(s.var()).has(keyTypeLabel, randomKeyValue.hashCode()));
                            } else if (v.valueClass().equals(String.class)) {

                                keyStatements.add(Graql.var(s.var()).has(keyTypeLabel, randomKeyValue));
                            }
                        });
                    });
                }
            });
        });
        return keyStatements;
    }

    private static class Rule {
        private final Pattern when;
        private final Pattern then;
        private String label;

        Rule(Pattern when, Pattern then, String label) {
            StatementVisitor visitor = new StatementVisitor(QueryBuilder::makeAnonVarsExplicit);
            this.when = visitor.visitPattern(when);
            this.then = visitor.visitPattern(then);
            this.label = label;
        }
    }
}
