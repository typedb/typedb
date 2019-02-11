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

package grakn.core.graql.query.parser;

import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.exception.GraqlQueryException;
import graql.lang.exception.GraqlException;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.property.HasAttributeProperty;
import grakn.core.graql.query.property.IsaProperty;
import grakn.core.graql.query.property.RelationProperty;
import grakn.core.graql.query.property.ValueProperty;
import grakn.core.graql.query.query.GraqlAggregate;
import grakn.core.graql.query.query.GraqlCompute;
import grakn.core.graql.query.query.GraqlDefine;
import grakn.core.graql.query.query.GraqlDelete;
import grakn.core.graql.query.query.GraqlGet;
import grakn.core.graql.query.query.GraqlGroup;
import grakn.core.graql.query.query.GraqlInsert;
import grakn.core.graql.query.query.GraqlQuery;
import grakn.core.graql.query.query.GraqlUndefine;
import grakn.core.graql.query.statement.Statement;
import grakn.core.graql.query.statement.Variable;
import graql.grammar.GraqlBaseVisitor;
import graql.grammar.GraqlLexer;
import graql.grammar.GraqlParser;
import graql.lang.parser.ErrorListener;
import graql.lang.util.StringUtil;
import graql.lang.util.Token;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.graql.query.Graql.and;
import static grakn.core.graql.query.Graql.not;
import static grakn.core.graql.query.Graql.type;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Graql query string parser to produce Graql Java objects
 */
public class Parser extends GraqlBaseVisitor {

    private GraqlParser parse(String queryString, ErrorListener errorListener) {
        CharStream charStream = CharStreams.fromString(queryString);

        GraqlLexer lexer = new GraqlLexer(charStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);
        CommonTokenStream tokens = new CommonTokenStream(lexer);

        GraqlParser parser = new GraqlParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);

        return parser;
    }

    private <CONTEXT extends ParserRuleContext, RETURN> RETURN parseQuery(
            String queryString, Function<GraqlParser, CONTEXT> parserMethod, Function<CONTEXT, RETURN> visitor
    ) {
        ErrorListener errorListener = ErrorListener.of(queryString);
        GraqlParser parser = parse(queryString, errorListener);

        // BailErrorStrategy + SLL is a very fast parsing strategy for queries
        // that are expected to be correct. However, it may not be able to
        // provide detailed/useful error message, if at all.
        parser.setErrorHandler(new BailErrorStrategy());
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

        CONTEXT queryContext;
        try {
            queryContext = parserMethod.apply(parser);
        } catch (ParseCancellationException e) {
            // We parse the query one more time, with "strict strategy" :
            // DefaultErrorStrategy + LL_EXACT_AMBIG_DETECTION
            // This was not set to default parsing strategy, but it is useful
            // to produce detailed/useful error message
            parser.setErrorHandler(new DefaultErrorStrategy());
            parser.getInterpreter().setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION);
            parserMethod.apply(parser);

            throw GraqlException.create(errorListener.toString());
        }

        return visitor.apply(queryContext);
    }

    @SuppressWarnings("unchecked")
    public <T extends GraqlQuery> T parseQueryEOF(String queryString) {
        return (T) parseQuery(queryString, GraqlParser::eof_query, this::visitEof_query);
    }

    @SuppressWarnings("unchecked")
    public <T extends GraqlQuery> Stream<T> parseQueryListEOF(String queryString) {
        return (Stream<T>) parseQuery(queryString, GraqlParser::eof_query_list, this::visitEof_query_list);
    }

    public Pattern parsePatternEOF(String patternString) {
        return parseQuery(patternString, GraqlParser::eof_pattern, this::visitEof_pattern);
    }

    public Stream<? extends Pattern> parsePatternListEOF(String patternsString) {
        return parseQuery(patternsString, GraqlParser::eof_pattern_list, this::visitEof_pattern_list);
    }

    // GLOBAL HELPER METHODS ===================================================

    private Variable getVar(TerminalNode variable) {
        // Remove '$' prefix
        String name = variable.getSymbol().getText().substring(1);

        if (name.equals(Token.Char.UNDERSCORE.toString())) {
            return new Variable();
        } else {
            return new Variable(name);
        }
    }

    // PARSER VISITORS =========================================================

    @Override
    public GraqlQuery visitEof_query(GraqlParser.Eof_queryContext ctx) {
        return visitQuery(ctx.query());
    }

    @Override
    public Stream<? extends GraqlQuery> visitEof_query_list(GraqlParser.Eof_query_listContext ctx) {
        return ctx.query().stream().map(this::visitQuery);
    }

    @Override
    public Pattern visitEof_pattern(GraqlParser.Eof_patternContext ctx) {
        return visitPattern(ctx.pattern());
    }

    @Override
    public Stream<? extends Pattern> visitEof_pattern_list(GraqlParser.Eof_pattern_listContext ctx) {
        return ctx.pattern().stream().map(this::visitPattern);
    }

    // GRAQL QUERIES ===========================================================

    @Override
    public GraqlQuery visitQuery(GraqlParser.QueryContext ctx) {
        if (ctx.query_define() != null) {
            return visitQuery_define(ctx.query_define());

        } else if (ctx.query_undefine() != null) {
            return visitQuery_undefine(ctx.query_undefine());

        } else if (ctx.query_insert() != null) {
            return visitQuery_insert(ctx.query_insert());

        } else if (ctx.query_delete() != null) {
            return visitQuery_delete(ctx.query_delete());

        } else if (ctx.query_get() != null) {
            return visitQuery_get(ctx.query_get());

        } else if (ctx.query_aggregate() != null) {
            return visitQuery_aggregate(ctx.query_aggregate());

        } else if (ctx.query_group() != null) {
            return visitQuery_group(ctx.query_group());

        } else if (ctx.query_compute() != null) {
            return visitQuery_compute(ctx.query_compute());

        } else {
            throw new IllegalArgumentException("Unrecognised Graql Query: " + ctx.getText());
        }
    }

    @Override
    public GraqlDefine visitQuery_define(GraqlParser.Query_defineContext ctx) {
        List<Statement> vars = ctx.statement_type().stream()
                .map(this::visitStatement_type)
                .collect(toList());
        return Graql.define(vars);
    }

    @Override
    public GraqlUndefine visitQuery_undefine(GraqlParser.Query_undefineContext ctx) {
        List<Statement> vars = ctx.statement_type().stream()
                .map(this::visitStatement_type)
                .collect(toList());
        return Graql.undefine(vars);
    }

    @Override
    public GraqlInsert visitQuery_insert(GraqlParser.Query_insertContext ctx) {
        List<Statement> statements = ctx.statement_instance().stream()
                .map(this::visitStatement_instance)
                .collect(toList());

        if (ctx.pattern() != null && !ctx.pattern().isEmpty()) {
            LinkedHashSet<Pattern> patterns = ctx.pattern().stream().map(this::visitPattern)
                    .collect(Collectors.toCollection(LinkedHashSet::new));

            return Graql.match(patterns).insert(statements);
        } else {
            return Graql.insert(statements);
        }
    }

    @Override
    public GraqlDelete visitQuery_delete(GraqlParser.Query_deleteContext ctx) {
        LinkedHashSet<Pattern> patterns = ctx.pattern().stream().map(this::visitPattern)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return Graql.match(patterns).delete(visitVariables(ctx.variables()));

        // TODO: implement parsing of OFFSET, LIMIT and ORDER
    }

    @Override
    public GraqlGet visitQuery_get(GraqlParser.Query_getContext ctx) {
        LinkedHashSet<Pattern> patterns = ctx.pattern().stream().map(this::visitPattern)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return Graql.match(patterns).get(visitVariables(ctx.variables()));

        // TODO: implement parsing of OFFSET, LIMIT and ORDER
    }

    /**
     * Visits the aggregate query node in the parsed syntax tree and builds the
     * appropriate aggregate query object
     *
     * @param ctx reference to the parsed aggregate query string
     * @return An AggregateQuery object
     */
    @Override
    public GraqlAggregate visitQuery_aggregate(GraqlParser.Query_aggregateContext ctx) {
        GraqlParser.Function_aggregateContext function = ctx.function_aggregate();
        GraqlAggregate.Method method = GraqlAggregate.Method.of(function.function_method().getText());
        Variable variable = function.VAR_() != null ? getVar(function.VAR_()) : null;

        return new GraqlAggregate(visitQuery_get(ctx.query_get()), method, variable);
    }

    @Override
    public GraqlGroup visitQuery_group(GraqlParser.Query_groupContext ctx) {
        Variable var = getVar(ctx.function_group().VAR_());
        GraqlParser.Function_aggregateContext function = ctx.function_aggregate();

        if (function == null) {
            return visitQuery_get(ctx.query_get()).group(var);
        } else {
            GraqlAggregate.Method aggregateMethod = GraqlAggregate.Method.of(function.function_method().getText());
            Variable aggregateVar = function.VAR_() != null ? getVar(function.VAR_()) : null;

            return new GraqlGroup.Aggregate(visitQuery_get(ctx.query_get()).group(var), aggregateMethod, aggregateVar);
        }
    }

    // DELETE AND GET QUERY MODIFIERS ==========================================

    @Override
    public LinkedHashSet<Variable> visitVariables(GraqlParser.VariablesContext ctx) {
        return ctx.VAR_().stream()
                .map(this::getVar)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    // COMPUTE QUERY ===========================================================

    public GraqlCompute visitQuery_compute(GraqlParser.Query_computeContext ctx) {
        GraqlParser.Compute_methodContext method = ctx.compute_method();
        GraqlParser.Compute_conditionsContext conditions = ctx.compute_conditions();

        GraqlCompute<?> query = Graql.compute(GraqlCompute.Method.of(method.getText()));
        if (conditions == null) return query;

        for (GraqlParser.Compute_conditionContext conditionCtx : conditions.compute_condition()) {
            if (conditionCtx.FROM() != null) {
                query.from(ConceptId.of(visitId(conditionCtx.id())));

            } else if (conditionCtx.TO() != null) {
                query.to(ConceptId.of(visitId(conditionCtx.id())));

            } else if (conditionCtx.OF() != null) {
                query.of(visitLabels(conditionCtx.labels()));

            } else if (conditionCtx.IN() != null) {
                query.in(visitLabels(conditionCtx.labels()));

            } else if (conditionCtx.USING() != null) {
                query.using(GraqlCompute.Algorithm.of(conditionCtx.compute_algorithm().getText()));

            } else if (conditionCtx.WHERE() != null) {
                query.where(visitCompute_args(conditionCtx.compute_args()));

            } else {
                throw GraqlQueryException.invalidComputeQuery_invalidCondition(query.method());
            }
        }

        Optional<GraqlQueryException> exception = query.getException();
        if (exception.isPresent()) throw exception.get();

        return query;
    }

    @Override
    public List<GraqlCompute.Argument> visitCompute_args(GraqlParser.Compute_argsContext ctx) {

        List<GraqlParser.Compute_argContext> argContextList = new ArrayList<>();
        List<GraqlCompute.Argument> argList = new ArrayList<>();

        if (ctx.compute_arg() != null) {
            argContextList.add(ctx.compute_arg());
        } else if (ctx.compute_args_array() != null) {
            argContextList.addAll(ctx.compute_args_array().compute_arg());
        }

        for (GraqlParser.Compute_argContext argContext : argContextList) {
            if (argContext.MIN_K() != null) {
                argList.add(GraqlCompute.Argument.min_k(getInteger(argContext.INTEGER_())));

            } else if (argContext.K() != null) {
                argList.add(GraqlCompute.Argument.k(getInteger(argContext.INTEGER_())));

            } else if (argContext.SIZE() != null) {
                argList.add(GraqlCompute.Argument.size(getInteger(argContext.INTEGER_())));

            } else if (argContext.CONTAINS() != null) {
                argList.add(GraqlCompute.Argument.contains(ConceptId.of(visitId(argContext.id()))));
            }
        }

        return argList;
    }

    // QUERY PATTERNS ==========================================================

    @Override
    public Set<Pattern> visitPatterns(GraqlParser.PatternsContext ctx) {
        return ctx.pattern().stream()
                .map(this::visitPattern)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public Pattern visitPattern(GraqlParser.PatternContext ctx) {

        if (ctx.pattern_statement() != null) {
            return visitPattern_statement(ctx.pattern_statement());

        } else if (ctx.pattern_disjunction() != null) {
            return visitPattern_disjunction(ctx.pattern_disjunction());

        } else if (ctx.pattern_conjunction() != null) {
            return visitPattern_conjunction(ctx.pattern_conjunction());

        } else if (ctx.pattern_negation() != null) {
            return visitPattern_negation(ctx.pattern_negation());

        } else {
            throw new IllegalArgumentException("Unrecognised Pattern: " + ctx.getText());
        }
    }

    @Override
    public Pattern visitPattern_disjunction(GraqlParser.Pattern_disjunctionContext ctx) {
        Set<Pattern> patterns = ctx.patterns().stream()
                .map(patternsCtx -> {
                    Set<Pattern> patternSet = visitPatterns(patternsCtx);
                    if (patternSet.size() > 1) return and(patternSet);
                    else return patternSet.iterator().next();
                })
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return Graql.or(patterns);
    }

    @Override
    public Pattern visitPattern_conjunction(GraqlParser.Pattern_conjunctionContext ctx) {
        return and(visitPatterns(ctx.patterns()));
    }

    @Override
    public Pattern visitPattern_negation(GraqlParser.Pattern_negationContext ctx) {
        Set<Pattern> patterns = visitPatterns(ctx.patterns());

        if (patterns.size() == 1) {
            return not(patterns.iterator().next());

        } else {
            return not(and(patterns));
        }
    }

    // PATTERN STATEMENTS ======================================================

    @Override
    public Statement visitPattern_statement(GraqlParser.Pattern_statementContext ctx) {
        // TODO: restrict for Match VS Define VS Insert

        if (ctx.statement_instance() != null) {
            return visitStatement_instance(ctx.statement_instance());

        } else if (ctx.statement_type() != null) {
            return visitStatement_type(ctx.statement_type());

        } else {
            throw new IllegalArgumentException("Unrecognised Statement class: " + ctx.getText());
        }
    }

    // TYPE STATEMENTS =========================================================

    @Override
    public Statement visitStatement_type(GraqlParser.Statement_typeContext ctx) {
        // TODO: restrict for Define VS Match for all usage of visitType(...)

        Statement type = visitType(ctx.type());

        for (GraqlParser.Type_propertyContext property : ctx.type_property()) {

            if (property.ABSTRACT() != null) {
                type = type.isAbstract();

            } else if (property.SUB_() != null) {
                Token.Property sub = Token.Property.of(property.SUB_().getText());

                if (sub != null && sub.equals(Token.Property.SUB)) {
                    type = type.sub(visitType(property.type(0)));

                } else if (sub != null && sub.equals(Token.Property.SUBX)) {
                    type = type.subX(visitType(property.type(0)));

                } else {
                    throw new IllegalArgumentException("Unrecognised SUB Property: " + property.type(0).getText());
                }

            } else if (property.KEY() != null) {
                type = type.key(visitType(property.type(0)));

            } else if (property.HAS() != null) {
                type = type.has(visitType(property.type(0)));

            } else if (property.PLAYS() != null) {
                type = type.plays(visitType(property.type(0)));

            } else if (property.RELATES() != null) {
                if (property.AS() != null) {
                    type = type.relates(visitType(property.type(0)),
                                        visitType(property.type(1)));
                } else {
                    type = type.relates(visitType(property.type(0)));
                }
            } else if (property.DATATYPE() != null) {
                type = type.datatype(Token.DataType.of(property.datatype().getText()));

            } else if (property.REGEX() != null) {
                type = type.regex(visitRegex(property.regex()));

            } else if (property.WHEN() != null) {
                type = type.when(and(
                        property.pattern().stream()
                                .map(this::visitPattern)
                                .collect(Collectors.toList())
                ));
            } else if (property.THEN() != null) {
                type = type.then(and(
                        property.statement_instance().stream()
                                .map(this::visitStatement_instance)
                                .collect(Collectors.toList())
                ));
            } else if (property.TYPE() != null) {
                type = type.type(visitLabel(property.label()));

            } else {
                throw new IllegalArgumentException("Unrecognised Type Statement: " + property.getText());
            }
        }

        return type;
    }

    // INSTANCE STATEMENTS =====================================================

    @Override
    public Statement visitStatement_instance(GraqlParser.Statement_instanceContext ctx) {
        // TODO: restrict for Insert VS Match

        if (ctx.statement_thing() != null) {
            return visitStatement_thing(ctx.statement_thing());

        } else if (ctx.statement_relation() != null) {
            return visitStatement_relation(ctx.statement_relation());

        } else if (ctx.statement_attribute() != null) {
            return visitStatement_attribute(ctx.statement_attribute());

        } else {
            throw new IllegalArgumentException("Unrecognised Instance Statement: " + ctx.getText());
        }
    }

    @Override @SuppressWarnings("Duplicates")
    public Statement visitStatement_thing(GraqlParser.Statement_thingContext ctx) {
        // TODO: restrict for Insert VS Match

        Statement instance = Graql.var(getVar(ctx.VAR_(0)));

        if (ctx.ISA_() != null) {
            instance = instance.isa(getIsaProperty(ctx.ISA_(), ctx.type()));

        } else if (ctx.ID() != null) {
            instance = instance.id(visitId(ctx.id()));

        } else if (ctx.NEQ() != null) {
            instance = instance.not(getVar(ctx.VAR_(1)));
        }

        if (ctx.attributes() != null) {
            for (HasAttributeProperty attribute : visitAttributes(ctx.attributes())) {
                instance = instance.has(attribute);
            }
        }

        return instance;
    }

    @Override @SuppressWarnings("Duplicates")
    public Statement visitStatement_relation(GraqlParser.Statement_relationContext ctx) {
        // TODO: restrict for Insert VS Match

        Statement instance;
        if (ctx.VAR_() != null) {
            instance = Graql.var(getVar(ctx.VAR_()));
        } else {
            instance = Graql.var(new Variable(false));
        }

        instance = instance.rel(visitRelation(ctx.relation()));
        if (ctx.ISA_() != null) {
            instance = instance.isa(getIsaProperty(ctx.ISA_(), ctx.type()));
        }

        if (ctx.attributes() != null) {
            for (HasAttributeProperty attribute : visitAttributes(ctx.attributes())) {
                instance = instance.has(attribute);
            }
        }

        return instance;
    }

    @Override @SuppressWarnings("Duplicates")
    public Statement visitStatement_attribute(GraqlParser.Statement_attributeContext ctx) {
        // TODO: restrict for Insert VS Match

        Statement instance;
        if (ctx.VAR_() != null) {
            instance = Graql.var(getVar(ctx.VAR_()));
        } else {
            instance = Graql.var();
        }

        instance = instance.attribute(new ValueProperty<>(visitOperation(ctx.operation())));
        if (ctx.ISA_() != null) {
            instance = instance.isa(getIsaProperty(ctx.ISA_(), ctx.type()));
        }

        if (ctx.attributes() != null) {
            for (HasAttributeProperty attribute : visitAttributes(ctx.attributes())) {
                instance = instance.has(attribute);
            }
        }

        return instance;
    }

    private IsaProperty getIsaProperty(TerminalNode isaToken, GraqlParser.TypeContext ctx) {
        Token.Property isa = Token.Property.of(isaToken.getText());

        if (isa != null && isa.equals(Token.Property.ISA)) {
            return new IsaProperty(visitType(ctx));

        } else if (isa != null && isa.equals(Token.Property.ISAX)) {
            return new IsaProperty(visitType(ctx), true);

        } else {
            throw new IllegalArgumentException("Unrecognised ISA property: " + ctx.getText());
        }
    }

    // ATTRIBUTE STATEMENT CONSTRUCT ===============================================

    @Override
    public List<HasAttributeProperty> visitAttributes(GraqlParser.AttributesContext ctx) {
        return ctx.attribute().stream().map(this::visitAttribute).collect(toList());
    }

    @Override
    public HasAttributeProperty visitAttribute(GraqlParser.AttributeContext ctx) {
        String type = ctx.label().getText();

        if (ctx.VAR_() != null) {
            Statement variable = Graql.var(getVar(ctx.VAR_()));
            if (ctx.via() != null) {
                return new HasAttributeProperty(type, variable, Graql.var(getVar(ctx.via().VAR_())));
            } else {
                return new HasAttributeProperty(type, variable);
            }
        } else if (ctx.operation() != null) {
            Statement value = Graql.var().attribute(new ValueProperty<>(visitOperation(ctx.operation())));
            if (ctx.via() != null) {
                return new HasAttributeProperty(type, value, Graql.var(getVar(ctx.via().VAR_())));
            } else {
                return new HasAttributeProperty(type, value);
            }
        } else {
            throw new IllegalArgumentException("Unrecognised MATCH HAS statement: " + ctx.getText());
        }
    }

    // RELATION STATEMENT CONSTRUCT ============================================

    public RelationProperty visitRelation(GraqlParser.RelationContext ctx) {
        List<RelationProperty.RolePlayer> rolePlayers = new ArrayList<>();

        for (GraqlParser.Role_playerContext rolePlayerCtx : ctx.role_player()) {
            Statement player = new Statement(getVar(rolePlayerCtx.player().VAR_()));
            if (rolePlayerCtx.type() != null) {
                Statement role = visitType(rolePlayerCtx.type());
                rolePlayers.add(new RelationProperty.RolePlayer(role, player));
            } else {
                rolePlayers.add(new RelationProperty.RolePlayer(null, player));
            }
        }
        return new RelationProperty(rolePlayers);
    }

    // TYPE, LABEL, AND IDENTIFIER CONSTRUCTS ==================================

    @Override
    public Statement visitType(GraqlParser.TypeContext ctx) {
        if (ctx.label() != null) {
            return type(visitLabel(ctx.label()));
        } else {
            return new Statement(getVar(ctx.VAR_()));
        }
    }

    @Override
    public Set<String> visitLabels(GraqlParser.LabelsContext labels) {
        List<GraqlParser.LabelContext> labelsList = new ArrayList<>();

        if (labels.label() != null) {
            labelsList.add(labels.label());
        } else if (labels.label_array() != null) {
            labelsList.addAll(labels.label_array().label());
        }

        return labelsList.stream().map(this::visitLabel).collect(toSet());
    }

    @Override
    public String visitLabel(GraqlParser.LabelContext ctx) {
        if (ctx.identifier() != null) {
            return visitIdentifier(ctx.identifier());
        } else {
            return ctx.ID_IMPLICIT_().getText();
        }
    }

    @Override
    public String visitId(GraqlParser.IdContext ctx) {
        return visitIdentifier(ctx.identifier());
    }

    @Override
    public String visitIdentifier(GraqlParser.IdentifierContext ctx) {
        if (ctx.STRING_() != null) {
            return getString(ctx.STRING_());
        } else {
            return ctx.getText();
        }
    }

    // ATTRIBUTE OPERATION CONSTRUCTS ==========================================

    @Override // TODO: this visitor method should not return a Predicate if we have the right data structure
    public ValueProperty.Operation<?> visitOperation(GraqlParser.OperationContext ctx) {
        if (ctx.assignment() != null) {
            return visitAssignment(ctx.assignment());
        } else if (ctx.comparison() != null) {
            return visitComparison(ctx.comparison());
        } else {
            throw new IllegalArgumentException("Unreconigsed Attribute Operation: " + ctx.getText());
        }
    }

    @Override
    public ValueProperty.Operation<?> visitAssignment(GraqlParser.AssignmentContext ctx) {
        Object value = visitLiteral(ctx.literal());

        if (value instanceof Integer) {
            return new ValueProperty.Operation.Assignment.Number<>(((Integer) value));
        } else if (value instanceof Long) {
            return new ValueProperty.Operation.Assignment.Number<>((Long) value);
        } else if (value instanceof Float) {
            return new ValueProperty.Operation.Assignment.Number<>((Float) value);
        } else if (value instanceof Double) {
            return new ValueProperty.Operation.Assignment.Number<>((Double) value);
        } else if (value instanceof Boolean) {
            return new ValueProperty.Operation.Assignment.Boolean((Boolean) value);
        } else if (value instanceof String) {
            return new ValueProperty.Operation.Assignment.String((String) value);
        } else if (value instanceof LocalDateTime) {
            return new ValueProperty.Operation.Assignment.DateTime((LocalDateTime) value);
        } else {
            throw new IllegalArgumentException("Unrecognised Value Assignment: " + ctx.getText());
        }
    }

    @Override
    public ValueProperty.Operation<?> visitComparison(GraqlParser.ComparisonContext ctx) {
        String comparatorStr;
        Object value;

        if (ctx.comparator() != null) {
            comparatorStr = ctx.comparator().getText();
        } else if (ctx.CONTAINS() != null) {
            comparatorStr = ctx.CONTAINS().getText();
        } else if (ctx.LIKE() != null) {
            comparatorStr = ctx.LIKE().getText();
        } else {
            throw new IllegalArgumentException("Unrecognised Value Comparison: " + ctx.getText());
        }

        Token.Comparator comparator = Token.Comparator.of(comparatorStr);
        if (comparator == null) {
            throw new IllegalArgumentException("Unrecognised Value Comparator: " + comparatorStr);
        }

        if (ctx.comparable() != null) {
            if (ctx.comparable().literal() != null) {
                value = visitLiteral(ctx.comparable().literal());
            } else if (ctx.comparable().VAR_() != null) {
                value = new Statement(getVar(ctx.comparable().VAR_()));
            } else {
                throw new IllegalArgumentException("Unrecognised Comparable value: " + ctx.comparable().getText());
            }
        } else if (ctx.containable() != null) {
            if (ctx.containable().STRING_() != null) {
                value = getString(ctx.containable().STRING_());
            } else if (ctx.containable().VAR_() != null) {
                value = new Statement(getVar(ctx.containable().VAR_()));
            } else {
                throw new IllegalArgumentException("Unrecognised Containable value: " + ctx.containable().getText());
            }
        } else if (ctx.regex() != null) {
            value = visitRegex(ctx.regex());
        } else {
            throw new IllegalArgumentException("Unrecognised Value Comparison: " + ctx.getText());
        }

        if (value instanceof Integer) {
            return new ValueProperty.Operation.Comparison.Number<>(comparator, ((Integer) value));
        } else if (value instanceof Long) {
            return new ValueProperty.Operation.Comparison.Number<>(comparator, (Long) value);
        } else if (value instanceof Float) {
            return new ValueProperty.Operation.Comparison.Number<>(comparator, (Float) value);
        } else if (value instanceof Double) {
            return new ValueProperty.Operation.Comparison.Number<>(comparator, (Double) value);
        } else if (value instanceof Boolean) {
            return new ValueProperty.Operation.Comparison.Boolean(comparator, (Boolean) value);
        } else if (value instanceof String) {
            return new ValueProperty.Operation.Comparison.String(comparator, (String) value);
        } else if (value instanceof LocalDateTime) {
            return new ValueProperty.Operation.Comparison.DateTime(comparator, (LocalDateTime) value);
        } else if (value instanceof Statement) {
            return new ValueProperty.Operation.Comparison.Variable(comparator, (Statement) value);
        } else {
            throw new IllegalArgumentException("Unrecognised Value Assignment: " + ctx.getText());
        }
    }

    // LITERAL INPUT VALUES ====================================================

    @Override
    public String visitRegex(GraqlParser.RegexContext ctx) {
        // Remove surrounding /.../
        String unquoted = unquoteString(ctx.STRING_());
        return unquoted.replaceAll("\\\\/", "/");
    }

    @Override
    public Token.DataType visitDatatype(GraqlParser.DatatypeContext datatype) {
        if (datatype.BOOLEAN() != null) {
            return Token.DataType.BOOLEAN;
        } else if (datatype.DATE() != null) {
            return Token.DataType.DATE;
        } else if (datatype.DOUBLE() != null) {
            return Token.DataType.DOUBLE;
        } else if (datatype.LONG() != null) {
            return Token.DataType.LONG;
        } else if (datatype.STRING() != null) {
            return Token.DataType.STRING;
        } else {
            throw new IllegalArgumentException("Unrecognised DataType: " + datatype);
        }
    }

    @Override
    public Object visitLiteral(GraqlParser.LiteralContext ctx) {
        if (ctx.STRING_() != null) {
            return getString(ctx.STRING_());

        } else if (ctx.INTEGER_() != null) {
            return getInteger(ctx.INTEGER_());

        } else if (ctx.REAL_() != null) {
            return getReal(ctx.REAL_());

        } else if (ctx.BOOLEAN_() != null) {
            return getBoolean(ctx.BOOLEAN_());

        } else if (ctx.DATE_() != null) {
            return getDate(ctx.DATE_());

        } else if (ctx.DATETIME_() != null) {
            return getDateTime(ctx.DATETIME_());

        } else {
            throw new IllegalArgumentException("Unrecognised Literal token: " + ctx.getText());
        }
    }

    private String getString(TerminalNode string) {
        // Remove surrounding quotes
        String unquoted = unquoteString(string);
        return StringUtil.unescapeString(unquoted);
    }

    private String unquoteString(TerminalNode string) {
        return string.getText().substring(1, string.getText().length() - 1);
    }

    private long getInteger(TerminalNode number) {
        return Long.parseLong(number.getText());
    }

    private double getReal(TerminalNode real) {
        return Double.valueOf(real.getText());
    }

    private boolean getBoolean(TerminalNode bool) {
        Token.Literal literal = Token.Literal.of(bool.getText());

        if (literal != null && literal.equals(Token.Literal.TRUE)) {
            return true;

        } else if (literal != null && literal.equals(Token.Literal.FALSE)) {
            return false;

        } else {
            throw new IllegalArgumentException("Unrecognised Boolean token: " + bool.getText());
        }
    }

    private LocalDateTime getDate(TerminalNode date) {
        return LocalDate.parse(date.getText(),
                               DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay();
    }

    private LocalDateTime getDateTime(TerminalNode dateTime) {
        return LocalDateTime.parse(dateTime.getText(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }
}
