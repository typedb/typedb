package grakn.core.test.behaviour.resolution.resolve;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Explanation;
import grakn.core.graql.reasoner.explanation.RuleExplanation;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.server.Transaction;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Disjunction;
import graql.lang.pattern.Negation;
import graql.lang.pattern.Pattern;
import graql.lang.property.HasAttributeProperty;
import graql.lang.property.IdProperty;
import graql.lang.property.IsaProperty;
import graql.lang.property.RelationProperty;
import graql.lang.property.TypeProperty;
import graql.lang.property.VarProperty;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import graql.lang.statement.StatementAttribute;
import graql.lang.statement.StatementInstance;
import graql.lang.statement.Variable;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;

public class QueryBuilder {

    private HashMap<String, Integer> nextVarIndex = new HashMap<>();

    public List<GraqlGet> buildMatchGet(Transaction tx, GraqlGet query) {
            List<ConceptMap> answers = tx.execute(query, true, true);

            ArrayList<GraqlGet> resolutionQueries = new ArrayList<>();
            for (ConceptMap answer : answers) {
                ConjunctionFlatteningVisitor flattener = new ConjunctionFlatteningVisitor();
                resolutionQueries.add(Graql.match(flattener.visitPattern(resolutionPattern(tx, answer, 0))).get());
            }
            System.out.print(resolutionQueries);
            return resolutionQueries;
    }

    private Conjunction<Pattern> resolutionPattern(Transaction tx, ConceptMap answer, Integer ruleResolutionIndex) {

        Pattern answerPattern = answer.getPattern();
        LinkedHashSet<Pattern> resolutionPatterns = new LinkedHashSet<>();

        if (answerPattern == null) {
            throw new RuntimeException("Answer is missing a pattern. Either patterns are broken or the initial query did not require inference.");
        }
        Integer finalRuleResolutionIndex1 = ruleResolutionIndex;

        StatementVisitor statementVisitor = new StatementVisitor(p -> {
            Statement withoutIds = removeIdProperties(makeAnonVarsExplicit(p));
            return withoutIds == null ? null : prefixVars(withoutIds, finalRuleResolutionIndex1);
        });

        resolutionPatterns.add(statementVisitor.visitPattern(answerPattern));

        generateKeyStatements(answer.map()).forEach(statement -> resolutionPatterns.add(prefixVars(statement, finalRuleResolutionIndex1)));

        if (answer.explanation() != null) {

            Explanation explanation = answer.explanation();

            if (explanation.isRuleExplanation()) {

                ConceptMap explAns = getOnlyElement(explanation.getAnswers());

                ruleResolutionIndex += 1;
                Integer finalRuleResolutionIndex0 = ruleResolutionIndex;

                StatementVisitor ruleStatementVisitor = new StatementVisitor(p -> prefixVars(makeAnonVarsExplicit(p), finalRuleResolutionIndex0));

                Pattern whenPattern = Objects.requireNonNull(((RuleExplanation) explanation).getRule().when());
                whenPattern = ruleStatementVisitor.visitPattern(whenPattern);
                resolutionPatterns.add(whenPattern);

                Pattern thenPattern = Objects.requireNonNull(((RuleExplanation) explanation).getRule().then());
                thenPattern = ruleStatementVisitor.visitPattern(thenPattern);
                resolutionPatterns.add(thenPattern);

                String ruleLabel = ((RuleExplanation)explanation).getRule().label().toString();
                resolutionPatterns.add(ruleResolutionConjunction(whenPattern, thenPattern, ruleLabel));
                resolutionPatterns.add(resolutionPattern(tx, explAns, ruleResolutionIndex));
            } else {
                for (ConceptMap explAns : explanation.getAnswers()) {
                    resolutionPatterns.addAll(resolutionPattern(tx, explAns, ruleResolutionIndex).getPatterns());
                }
            }
        }
        return Graql.and(resolutionPatterns);
    }

    public abstract class PatternVisitor {
        Pattern visitPattern(Pattern pattern) {
            if (pattern instanceof Statement) {
                return visitStatement((Statement) pattern);
            } else if (pattern instanceof Conjunction) {
                return visitConjunction((Conjunction<? extends Pattern>) pattern);
            } else if (pattern instanceof Negation) {
                return visitNegation((Negation<? extends Pattern>) pattern);
            } else if (pattern instanceof Disjunction) {
                return visitDisjunction((Disjunction<? extends Pattern>) pattern);
            }
            throw new UnsupportedOperationException();
        }

        Pattern visitStatement(Statement pattern) {
            return pattern;
        }

        Pattern visitConjunction(Conjunction<? extends Pattern> pattern) {
            Set<? extends Pattern> patterns = pattern.getPatterns();
            HashSet<Pattern> newPatterns = new HashSet<>();
            patterns.forEach(p -> {
                Pattern childPattern = visitPattern(p);
                if (childPattern != null) {
                    newPatterns.add(childPattern);
                }
            });
            return new Conjunction<>(newPatterns);
        }

        Pattern visitNegation(Negation<? extends Pattern> pattern) {
            return new Negation<>(visitPattern(pattern.getPattern()));
        }

        Pattern visitDisjunction(Disjunction<? extends Pattern> pattern) {
            Set<? extends Pattern> patterns = pattern.getPatterns();
            HashSet<Pattern> newPatterns = new HashSet<>();
            patterns.forEach(p -> {
                Pattern childPattern = visitPattern(p);
                if (childPattern != null) {
                    newPatterns.add(childPattern);
                }
            });
            return new Disjunction<>(newPatterns);
        }
    }

    public class StatementVisitor extends PatternVisitor {

        private Function<Statement, Pattern> function;

        StatementVisitor(Function<Statement, Pattern> function) {
            this.function = function;
        }

        @Override
        Pattern visitStatement(Statement pattern) {
            return function.apply(pattern);
        }
    }

    public class ConjunctionFlatteningVisitor extends PatternVisitor {
        Pattern visitConjunction(Conjunction<? extends Pattern> pattern) {
            Set<? extends Pattern> patterns = pattern.getPatterns();
            HashSet<Pattern> newPatterns = new HashSet<>();
            patterns.forEach(p -> {
                Pattern childPattern = visitPattern(p);
                if (childPattern instanceof Conjunction) {
                    newPatterns.addAll(((Conjunction<? extends Pattern>) childPattern).getPatterns());
                } else if (childPattern != null) {
                    newPatterns.add(visitPattern(childPattern));
                }
            });
            if (newPatterns.size() == 0) {
                return null;
            } else if (newPatterns.size() == 1) {
                return getOnlyElement(newPatterns);
            }
            return new Conjunction<>(newPatterns);
        }
    }

    public static Statement makeAnonVarsExplicit(Statement statement) {
        if (statement.var().isReturned()) {
            return statement;
        } else {
            return Statement.create(statement.var().asReturnedVar(), statement.properties());
        }
    }

    private Statement prefixVars(Statement statement, Integer ruleResolutionIndex) {
        String prefix = "r" + ruleResolutionIndex + "-";
        String newVarName = prefix + statement.var().name();

        LinkedHashSet<VarProperty> newProperties = new LinkedHashSet<>();
        for (VarProperty prop : statement.properties()) {

            // TODO implement the rest of these replacements
            if (prop instanceof RelationProperty) {

                List<RelationProperty.RolePlayer> roleplayers = ((RelationProperty) prop).relationPlayers();
                List<RelationProperty.RolePlayer> newRps = roleplayers.stream().map(rp -> {

                    String rpVarName = prefix + rp.getPlayer().var().name();
                    Statement newPlayerStatement = new Statement(new Variable(rpVarName));
                    return new RelationProperty.RolePlayer(rp.getRole().orElse(null), newPlayerStatement);
                }).collect(Collectors.toList());

                newProperties.add(new RelationProperty(newRps));
            } else if (prop instanceof HasAttributeProperty) {

                HasAttributeProperty hasProp = (HasAttributeProperty) prop;
                if (hasProp.attribute().var().isVisible()) {
                    // If the attribute has a variable, rather than a value
                    String newAttributeName = prefix + ((HasAttributeProperty) prop).attribute().var().name();
                    newProperties.add(new HasAttributeProperty(hasProp.type(), new Statement(new Variable(newAttributeName))));
                } else {
                    newProperties.add(hasProp);
                }
            } else {
                newProperties.add(prop);
            }
        }
        return Statement.create(new Variable(newVarName), newProperties);
    }

    public Conjunction<? extends Pattern> ruleResolutionConjunction(Pattern whenPattern, Pattern thenPattern, String ruleLabel) {
        String inferenceType = "resolution";
        String inferenceRuleLabelType = "rule-label";
        Variable ruleVar = new Variable(getNextVar("rule"));
        Statement relation = Graql.var(ruleVar).isa(inferenceType).has(inferenceRuleLabelType, ruleLabel);
        StatementVisitor bodyVisitor = new StatementVisitor(p -> statementToResolutionConjunction(p, ruleVar, "body"));
        StatementVisitor headVisitor = new StatementVisitor(p -> statementToResolutionConjunction(p, ruleVar, "head"));
        Pattern body = bodyVisitor.visitPattern(whenPattern);
        Pattern head = headVisitor.visitPattern(thenPattern);
        return Graql.and(body, head, relation);
    }

    private Conjunction<Statement> statementToResolutionConjunction(Statement statement, Variable ruleVar, String ruleRole) {
        LinkedHashMap<String, Statement> stringStatementMap = statementToResolutionProperties(statement);
        LinkedHashSet<Statement> s = new LinkedHashSet<>();
        for (String var : stringStatementMap.keySet()) {
            s.add(Graql.var(ruleVar).rel(ruleRole, Graql.var(var)));
        }
        s.addAll(stringStatementMap.values());
        return Graql.and(s);
    }

    public LinkedHashMap<String, Statement> statementToResolutionProperties(Statement statement) {
        LinkedHashMap<String, Statement> props = new LinkedHashMap<>();

        String statementVar = statement.var().name();

        for (VarProperty varProp : statement.properties()) {

            if (varProp instanceof HasAttributeProperty) {
                String nextVar = getNextVar("x");
                StatementInstance propStatement = Graql.var(nextVar).isa("has-attribute-property").has((HasAttributeProperty) varProp).rel("owner", statementVar);
                props.put(nextVar, propStatement);

            } else if (varProp instanceof RelationProperty){
                for (RelationProperty.RolePlayer rolePlayer : ((RelationProperty)varProp).relationPlayers()) {
                    Optional<Statement> role = rolePlayer.getRole();

                    String nextVar = getNextVar("x");

                    StatementInstance propStatement = Graql.var(nextVar).isa("relation-property").rel("rel", statementVar).rel("roleplayer", Graql.var(rolePlayer.getPlayer().var()));
                    if(role.isPresent()) {
                        String roleLabel = ((TypeProperty) getOnlyElement(role.get().properties())).name();
                        propStatement = propStatement.has("role-label", roleLabel);
                    }
                    props.put(nextVar, propStatement);
                }
            } else if (varProp instanceof IsaProperty){
                String nextVar = getNextVar("x");
                StatementInstance propStatement = Graql.var(nextVar).isa("isa-property").rel("instance", statementVar).has("type-label", varProp.property());
                props.put(nextVar, propStatement);
            }
        }
        return props;
    }

    private String getNextVar(String prefix){
        nextVarIndex.putIfAbsent(prefix, 0);
        int currentIndex = nextVarIndex.get(prefix);
        String nextVar = "x" + currentIndex;
        nextVarIndex.put(prefix, currentIndex + 1);
        return nextVar;
    }

    /**
     * Remove properties that stipulate ConceptIds from a given statement
     * @param statement statement to remove from
     * @return statement without any ID properties, null if an ID property was the only property
     */
    public static Statement removeIdProperties(Statement statement) {
        LinkedHashSet<VarProperty> propertiesWithoutIds = new LinkedHashSet<>();
        statement.properties().forEach(varProperty -> {
            if (!(varProperty instanceof IdProperty)) {
                propertiesWithoutIds.add(varProperty);
            }
        });
        if (propertiesWithoutIds.isEmpty()) {
            return null;
        }
        return Statement.create(statement.var(), propertiesWithoutIds);
    }

    /**
     * Create a set of statements that will query for the keys of the concepts given in the map. Attributes given in
     * the map are simply queried for by their own type and value.
     * @param varMap variable map of concepts
     * @return Statements that check for the keys of the given concepts
     */
    public static Set<Statement> generateKeyStatements(Map<Variable, Concept> varMap) {
        LinkedHashSet<Statement> statements = new LinkedHashSet<>();

        for (Map.Entry<Variable, Concept> entry : varMap.entrySet()) {
            Variable var = entry.getKey();
            Concept concept = entry.getValue();

            if (concept.isAttribute()) {

                String typeLabel = concept.asAttribute().type().label().toString();
                Statement statement = Graql.var(var).isa(typeLabel);
                StatementAttribute s = null;

                Object attrValue = concept.asAttribute().value();
                if (attrValue instanceof String) {
                    s = statement.val((String) attrValue);
                } else if (attrValue instanceof Double) {
                    s = statement.val((Double) attrValue);
                } else if (attrValue instanceof Long) {
                    s = statement.val((Long) attrValue);
                } else if (attrValue instanceof LocalDateTime) {
                    s = statement.val((LocalDateTime) attrValue);
                } else if (attrValue instanceof Boolean) {
                    s = statement.val((Boolean) attrValue);
                }
                statements.add(s);

            } else if (concept.isEntity() | concept.isRelation()){

                concept.asThing().keys().forEach(attribute -> {

                    String typeLabel = attribute.type().label().toString();
                    Statement statement = Graql.var(var);
                    Object attrValue = attribute.value();

                    StatementInstance s = null;
                    if (attrValue instanceof String) {
                        s = statement.has(typeLabel, (String) attrValue);
                    } else if (attrValue instanceof Double) {
                        s = statement.has(typeLabel, (Double) attrValue);
                    } else if (attrValue instanceof Long) {
                        s = statement.has(typeLabel, (Long) attrValue);
                    } else if (attrValue instanceof LocalDateTime) {
                        s = statement.has(typeLabel, (LocalDateTime) attrValue);
                    } else if (attrValue instanceof Boolean) {
                        s = statement.has(typeLabel, (Boolean) attrValue);
                    }
                    statements.add(s);
                });

            } else {
                throw new RuntimeException("Presently we only handle queries concerning Things, not Types");
            }
        }
        return statements;
    }

}
