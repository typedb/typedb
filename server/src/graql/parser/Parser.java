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

package grakn.core.graql.parser;

import grakn.core.common.util.CommonUtil;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.exception.GraqlSyntaxException;
import grakn.core.graql.query.AggregateQuery;
import grakn.core.graql.query.ComputeQuery;
import grakn.core.graql.query.DefineQuery;
import grakn.core.graql.query.DeleteQuery;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.GroupAggregateQuery;
import grakn.core.graql.query.GroupQuery;
import grakn.core.graql.query.InsertQuery;
import grakn.core.graql.query.Query;
import grakn.core.graql.query.UndefineQuery;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Disjunction;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.HasAttributeProperty;
import grakn.core.graql.query.pattern.property.IsaExplicitProperty;
import grakn.core.graql.query.pattern.property.IsaProperty;
import grakn.core.graql.query.pattern.property.RelationProperty;
import grakn.core.graql.query.pattern.property.SubExplicitProperty;
import grakn.core.graql.query.pattern.property.SubProperty;
import grakn.core.graql.query.pattern.property.ValueProperty;
import grakn.core.graql.query.predicate.ValuePredicate;
import grakn.core.graql.util.StringUtil;
import graql.grammar.GraqlBaseVisitor;
import graql.grammar.GraqlLexer;
import graql.grammar.GraqlParser;
import graql.parser.ErrorListener;
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
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.graql.query.Graql.eq;
import static grakn.core.graql.query.pattern.Pattern.label;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Graql query string parser to produce Graql Java objects
 */
// This class performs a lot of unchecked casts, because ANTLR's visit methods only return 'object'
@SuppressWarnings({"unchecked", "Duplicates"})
public class Parser extends GraqlBaseVisitor {

    public Parser() {}

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

    @SuppressWarnings("unchecked")
    public <T extends Query> T parseQueryEOF(String queryString) {
        ErrorListener errorListener = ErrorListener.of(queryString);
        GraqlParser parser = parse(queryString, errorListener);

        // BailErrorStrategy + SLL is a very fast parsing strategy for queries
        // that are expected to be correct. However, it may not be able to
        // provide detailed/useful error message, if at all.
        parser.setErrorHandler(new BailErrorStrategy());
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

        GraqlParser.Eof_queryContext eofQueryContext;
        try {
            eofQueryContext = parser.eof_query();
        } catch (ParseCancellationException e) {
            // We parse the query one more time, with "strict strategy" :
            // DefaultErrorStrategy + LL_EXACT_AMBIG_DETECTION
            // This was not set to default parsing strategy, but it is useful
            // to produce detailed/useful error message
            parser.setErrorHandler(new DefaultErrorStrategy());
            parser.getInterpreter().setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION);
            parser.eof_query();

            throw GraqlSyntaxException.create(errorListener.toString());
        }

        return (T) visitEof_query(eofQueryContext);
    }

    @SuppressWarnings("unchecked")
    public <T extends Query> Stream<T> parseQueryListEOF(String queryString) {
        ErrorListener errorListener = ErrorListener.of(queryString);
        GraqlParser parser = parse(queryString, errorListener);

        // BailErrorStrategy is also optimal for parsing very large files
        // as it will terminate upon the first error encountered
        parser.setErrorHandler(new BailErrorStrategy());

        GraqlParser.Eof_query_listContext eofQueryListContext;
        try {
            eofQueryListContext = parser.eof_query_list();
        } catch (ParseCancellationException e) {
            throw GraqlSyntaxException.create(errorListener.toString());
        }

        return (Stream<T>) this.visitEof_query_list(eofQueryListContext);
    }

    public Pattern parsePatternEOF(String patternString) {
        ErrorListener errorListener = ErrorListener.of(patternString);
        GraqlParser parser = parse(patternString, errorListener);

        GraqlParser.Eof_patternContext eofPatternContext = parser.eof_pattern();
        if (errorListener.hasErrors()) {
            throw GraqlSyntaxException.create(errorListener.toString());
        }

        return visitEof_pattern(eofPatternContext);
    }

    public Stream<? extends Pattern> parsePatternListEOF(String patternsString) {
        ErrorListener errorListener = ErrorListener.of(patternsString);
        GraqlParser parser = parse(patternsString, errorListener);

        // If we're using the BailErrorStrategy, we will throw here
        // This strategy is designed for parsing very large files
        // and terminate upon the first error encountered
        parser.setErrorHandler(new BailErrorStrategy());

        GraqlParser.Eof_pattern_listContext eofPatternListContext;
        try {
            eofPatternListContext = parser.eof_pattern_list();
        } catch (ParseCancellationException e) {
            throw GraqlSyntaxException.create(errorListener.toString());
        }

        return visitEof_pattern_list(eofPatternListContext);
    }

    // GLOBAL HELPER METHODS ===================================================

    private Variable getVar(TerminalNode variable) {
        // Remove '$' prefix
        String name = variable.getSymbol().getText().substring(1);

        if (name.equals(Query.Char.UNDERSCORE.toString())) {
            return new Variable();
        } else {
            return new Variable(name);
        }
    }

    // PARSER VISITORS =========================================================

    @Override
    public Query visitEof_query(GraqlParser.Eof_queryContext ctx) {
        return visitQuery(ctx.query());
    }

    @Override
    public Stream<? extends Query> visitEof_query_list(GraqlParser.Eof_query_listContext ctx) {
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
    public Query visitQuery(GraqlParser.QueryContext ctx) {
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
    public DefineQuery visitQuery_define(GraqlParser.Query_defineContext ctx) {
        List<Statement> vars = ctx.statement_type().stream()
                .map(this::visitStatement_type)
                .collect(toList());
        return Graql.define(vars);
    }

    @Override
    public UndefineQuery visitQuery_undefine(GraqlParser.Query_undefineContext ctx) {
        List<Statement> vars = ctx.statement_type().stream()
                .map(this::visitStatement_type)
                .collect(toList());
        return Graql.undefine(vars);
    }

    @Override
    public InsertQuery visitQuery_insert(GraqlParser.Query_insertContext ctx) {
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
    public DeleteQuery visitQuery_delete(GraqlParser.Query_deleteContext ctx) {
        LinkedHashSet<Pattern> patterns = ctx.pattern().stream().map(this::visitPattern)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return Graql.match(patterns).delete(visitVariables(ctx.variables()));

        // TODO: implement parsing of OFFSET, LIMIT and ORDER
    }

    @Override
    public GetQuery visitQuery_get(GraqlParser.Query_getContext ctx) {
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
    public AggregateQuery visitQuery_aggregate(GraqlParser.Query_aggregateContext ctx) {
        GraqlParser.Function_aggregateContext function = ctx.function_aggregate();
        AggregateQuery.Method method = AggregateQuery.Method.of(function.function_method().getText());
        Variable variable = function.VAR_() != null ? getVar(function.VAR_()) : null;

        return new AggregateQuery(visitQuery_get(ctx.query_get()), method, variable);
    }

    @Override
    public GroupQuery visitQuery_group(GraqlParser.Query_groupContext ctx) {
        Variable var = getVar(ctx.function_group().VAR_());
        GraqlParser.Function_aggregateContext function = ctx.function_aggregate();

        if (function == null) {
            return visitQuery_get(ctx.query_get()).group(var);
        } else {
            AggregateQuery.Method aggregateMethod = AggregateQuery.Method.of(function.function_method().getText());
            Variable aggregateVar = function.VAR_() != null ? getVar(function.VAR_()) : null;

            return new GroupAggregateQuery(visitQuery_get(ctx.query_get()).group(var), aggregateMethod, aggregateVar);
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

    public ComputeQuery visitQuery_compute(GraqlParser.Query_computeContext ctx) {
        GraqlParser.Compute_methodContext method = ctx.compute_method();
        GraqlParser.Compute_conditionsContext conditions = ctx.compute_conditions();

        ComputeQuery query = Graql.compute(ComputeQuery.Method.of(method.getText()));
        if (conditions == null) return query;

        for (GraqlParser.Compute_conditionContext conditionCtx : conditions.compute_condition()) {
            if (conditionCtx.FROM() != null) {
                query.from(ConceptId.of(visitId((conditionCtx.id()))));

            } else if (conditionCtx.TO() != null) {
                query.to(ConceptId.of(visitId(conditionCtx.id())));

            } else if (conditionCtx.OF() != null) {
                query.of(visitLabels(conditionCtx.labels()));

            } else if (conditionCtx.IN() != null) {
                query.in(visitLabels(conditionCtx.labels()));

            } else if (conditionCtx.USING() != null) {
                query.using(ComputeQuery.Algorithm.of(conditionCtx.compute_algorithm().getText()));

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
    public List<ComputeQuery.Argument> visitCompute_args(GraqlParser.Compute_argsContext ctx) {

        List<GraqlParser.Compute_argContext> argContextList = new ArrayList<>();
        List<ComputeQuery.Argument> argList = new ArrayList<>();

        if (ctx.compute_arg() != null) {
            argContextList.add(ctx.compute_arg());
        } else if (ctx.compute_args_array() != null) {
            argContextList.addAll(ctx.compute_args_array().compute_arg());
        }

        for (GraqlParser.Compute_argContext argContext : argContextList) {
            if (argContext.MIN_K() != null) {
                argList.add(ComputeQuery.Argument.min_k(getInteger(argContext.INTEGER_())));

            } else if (argContext.K() != null) {
                argList.add(ComputeQuery.Argument.k(getInteger(argContext.INTEGER_())));

            } else if (argContext.SIZE() != null) {
                argList.add(ComputeQuery.Argument.size(getInteger(argContext.INTEGER_())));

            } else if (argContext.CONTAINS() != null) {
                argList.add(ComputeQuery.Argument.contains(ConceptId.of(visitId(argContext.id()))));
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

        if (ctx.statement() != null) {
            return visitStatement(ctx.statement());

        } else if (ctx.disjunction() != null) {
            return visitDisjunction(ctx.disjunction());

        } else {
            return visitConjunction(ctx.conjunction());
        }
    }

    @Override
    public Conjunction<?> visitConjunction(GraqlParser.ConjunctionContext ctx) {
        return Pattern.and(visitPatterns(ctx.patterns()));
    }

    @Override
    public Disjunction<?> visitDisjunction(GraqlParser.DisjunctionContext ctx) {
        Set<Pattern> patterns = ctx.patterns().stream()
                .map(patternsCtx -> {
                    Set<Pattern> patternSet = visitPatterns(patternsCtx);
                    if (patternSet.size() > 1) return Pattern.and(patternSet);
                    else return patternSet.iterator().next();
                })
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return Pattern.or(patterns);
    }

    @Override
    public Statement visitStatement(GraqlParser.StatementContext ctx) {
        // TODO: restrict for Match VS Define VS Insert

        if (ctx.statement_instance() != null) {
            return visitStatement_instance(ctx.statement_instance());
        } else if (ctx.statement_type() != null){
            return visitStatement_type(ctx.statement_type());
        } else {
            throw new IllegalArgumentException("Uncrecognised Statement class: " + ctx.getText());
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
                type = type.addProperty(getSubProperty(property.SUB_(), property.type(0)));

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
                type = type.datatype(Query.DataType.of(property.datatype().getText()));

            } else if (property.REGEX() != null) {
                type = type.like(visitRegex(property.regex()));

            } else if (property.WHEN() != null) {
                type = type.when(Pattern.and(
                        property.pattern().stream()
                                .map(this::visitPattern)
                                .collect(Collectors.toList())
                ));
            } else if (property.THEN() != null) {
                type = type.then(Pattern.and(
                        property.statement_instance().stream()
                                .map(this::visitStatement_instance)
                                .collect(Collectors.toList())
                ));
            } else if (property.LABEL() != null) {
                type = type.label(property.label().getText());

            } else {
                throw new IllegalArgumentException("Unrecognised INSERT statement: " + property.getText());
            }
        }

        return type;
    }

    private SubProperty getSubProperty(TerminalNode subToken, GraqlParser.TypeContext ctx) {
        Query.Property sub = Query.Property.of(subToken.getText());

        if (sub != null && sub.equals(Query.Property.SUB)){
            return new SubProperty(visitType(ctx));

        } else if (sub != null && sub.equals(Query.Property.SUBX)) {
            return new SubExplicitProperty(visitType(ctx));

        } else {
            throw new IllegalArgumentException("Unrecognised SUB property: " + ctx.getText());
        }
    }

    // INSTANCE STATEMENTS =====================================================

    @Override
    public Statement visitStatement_instance(GraqlParser.Statement_instanceContext ctx) {
        // TODO: restrict for Insert VS Match

        if (ctx.instance_thing() != null) {
            return visitInstance_thing(ctx.instance_thing());

        } else if (ctx.instance_relation() != null) {
            return visitInstance_relation(ctx.instance_relation());

        } else if (ctx.instance_attribute() != null) {
            return visitInstance_attribute(ctx.instance_attribute());

        } else {
            throw new IllegalArgumentException("Unrecognised INSERT statement: " + ctx.getText());
        }
    }

    @Override
    public Statement visitInstance_thing(GraqlParser.Instance_thingContext ctx) {
        // TODO: restrict for Insert VS Match

        Statement instance = Pattern.var(getVar(ctx.VAR_()));

        if (ctx.ISA_() != null) {
            instance = instance.addProperty(getIsaProperty(ctx.ISA_(), ctx.type()));

        } else if (ctx.ID() != null) {
            instance = instance.id(ctx.id().getText());

        }

        if (ctx.attributes() != null) {
            for (HasAttributeProperty attribute : visitAttributes(ctx.attributes())) {
                instance = instance.addProperty(attribute);
            }
        }

        return instance;
    }

    @Override
    public Statement visitInstance_relation(GraqlParser.Instance_relationContext ctx) {
        // TODO: restrict for Insert VS Match

        Statement instance;
        if (ctx.VAR_() != null) {
            instance = Pattern.var(getVar(ctx.VAR_()));
        } else {
            instance = Pattern.var();
        }

        instance = instance.addProperty(visitRelation(ctx.relation()));
        if (ctx.ISA_() != null) {
            instance = instance.addProperty(getIsaProperty(ctx.ISA_(), ctx.type()));
        }

        if (ctx.attributes() != null) {
            for (HasAttributeProperty attribute : visitAttributes(ctx.attributes())) {
                instance = instance.addProperty(attribute);
            }
        }

        return instance;
    }

    @Override
    public Statement visitInstance_attribute(GraqlParser.Instance_attributeContext ctx) {
        // TODO: restrict for Insert VS Match

        Statement instance;
        if (ctx.VAR_() != null) {
            instance = Pattern.var(getVar(ctx.VAR_()));
        } else {
            instance = Pattern.var();
        }

        instance = instance.addProperty(new ValueProperty(visitPredicate(ctx.predicate())));
        if (ctx.ISA_() != null) {
            instance = instance.addProperty(getIsaProperty(ctx.ISA_(), ctx.type()));
        }

        if (ctx.attributes() != null) {
            for (HasAttributeProperty attribute : visitAttributes(ctx.attributes())) {
                instance = instance.addProperty(attribute);
            }
        }

        return instance;
    }

    private IsaProperty getIsaProperty(TerminalNode isaToken, GraqlParser.TypeContext ctx) {
        Query.Property isa = Query.Property.of(isaToken.getText());

        if (isa != null && isa.equals(Query.Property.ISA)){
            return new IsaProperty(visitType(ctx));

        } else if (isa != null && isa.equals(Query.Property.ISAX)) {
            return new IsaExplicitProperty(visitType(ctx));

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
            Statement variable = Pattern.var(getVar(ctx.VAR_()));
            if (ctx.via() != null) {
                return new HasAttributeProperty(type, variable, Pattern.var(getVar(ctx.via().VAR_())));
            } else {
                return new HasAttributeProperty(type, variable);
            }
        } else if (ctx.predicate() != null) {
            Statement value = Pattern.var().val(visitPredicate(ctx.predicate()));
            if (ctx.via() != null) {
                return new HasAttributeProperty(type, value, Pattern.var(getVar(ctx.via().VAR_())));
            } else {
                return new HasAttributeProperty(type, value);
            }
        } else {
            throw new IllegalArgumentException("Unrecognised MATCH HAS statement: " + ctx.getText());
        }
    }

    // RELATION STATEMENT CONSTRUCT ============================================

    public RelationProperty visitRelation(GraqlParser.RelationContext ctx) {
        LinkedHashSet<RelationProperty.RolePlayer> rolePlayers = new LinkedHashSet<>();

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

    // TYPE STATEMENT CONSTRUCT ================================================

    @Override
    public Statement visitType(GraqlParser.TypeContext ctx) {
        if (ctx.label() != null) {
            return label(visitLabel(ctx.label()));
        } else {
            return new Statement(getVar(ctx.VAR_()));
        }
    }

    // DATA TYPE AND VALUE STATEMENT CONSTRUCTS ====================================

    @Override
    public String visitRegex(GraqlParser.RegexContext ctx) {
        // Remove surrounding /.../
        String unquoted = unquoteString(ctx.STRING_());
        return unquoted.replaceAll("\\\\/", "/");
        // TODO: Why replace \\/ with / in the regex?
    }


    // ============================================================================================================== //
    // ============================================================================================================== //
    // ============================================================================================================== //


    @Override
    public Label visitLabel(GraqlParser.LabelContext ctx) {
        if (ctx.identifier() != null) {
            return Label.of(visitIdentifier(ctx.identifier()));
        } else {
            return Label.of(ctx.ID_IMPLICIT_().getText());
        }
    }

    @Override
    public Set<Label> visitLabels(GraqlParser.LabelsContext labels) {
        List<GraqlParser.LabelContext> labelsList = new ArrayList<>();

        if (labels.label() != null) {
            labelsList.add(labels.label());
        } else if (labels.labelsArray() != null) {
            labelsList.addAll(labels.labelsArray().label());
        }

        return labelsList.stream().map(this::visitLabel).collect(toSet());
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

    @Override
    public ValuePredicate visitPredicateEq(GraqlParser.PredicateEqContext ctx) {
        return eq(visitValue(ctx.value()));
    }

    @Override
    public ValuePredicate visitPredicateVariable(GraqlParser.PredicateVariableContext ctx) {
        return eq(new Statement(getVar(ctx.VAR_())));
    }

    @Override
    public ValuePredicate visitPredicateNeq(GraqlParser.PredicateNeqContext ctx) {
        return applyPredicate(Graql::neq, Graql::neq, visitValueOrVar(ctx.valueOrVar()));
    }

    @Override
    public ValuePredicate visitPredicateGt(GraqlParser.PredicateGtContext ctx) {
        return applyPredicate((Function<Comparable<?>, ValuePredicate>) Graql::gt, Graql::gt, visitValueOrVar(ctx.valueOrVar()));
    }

    @Override
    public ValuePredicate visitPredicateGte(GraqlParser.PredicateGteContext ctx) {
        return applyPredicate((Function<Comparable<?>, ValuePredicate>) Graql::gte, Graql::gte, visitValueOrVar(ctx.valueOrVar()));
    }

    @Override
    public ValuePredicate visitPredicateLt(GraqlParser.PredicateLtContext ctx) {
        return applyPredicate((Function<Comparable<?>, ValuePredicate>) Graql::lt, Graql::lt, visitValueOrVar(ctx.valueOrVar()));
    }

    @Override
    public ValuePredicate visitPredicateLte(GraqlParser.PredicateLteContext ctx) {
        return applyPredicate((Function<Comparable<?>, ValuePredicate>) Graql::lte, Graql::lte, visitValueOrVar(ctx.valueOrVar()));
    }

    @Override
    public ValuePredicate visitPredicateContains(GraqlParser.PredicateContainsContext ctx) {
        Object stringOrStatement = ctx.STRING_() != null ? getString(ctx.STRING_()) : new Statement(getVar(ctx.VAR_()));

        return applyPredicate((Function<String, ValuePredicate>) Graql::contains, Graql::contains, stringOrStatement);
    }

    @Override
    public ValuePredicate visitPredicateRegex(GraqlParser.PredicateRegexContext ctx) {
        return Graql.like(visitRegex(ctx.regex()));
    }

    @Override
    public Statement visitValueVariable(GraqlParser.ValueVariableContext ctx) {
        return new Statement(getVar(ctx.VAR_()));
    }

    @Override
    public String visitValueString(GraqlParser.ValueStringContext ctx) {
        return getString(ctx.STRING_());
    }

    @Override
    public Long visitValueInteger(GraqlParser.ValueIntegerContext ctx) {
        return getInteger(ctx.INTEGER_());
    }

    @Override
    public Double visitValueReal(GraqlParser.ValueRealContext ctx) {
        return Double.valueOf(ctx.REAL_().getText());
    }

    @Override
    public Boolean visitValueBoolean(GraqlParser.ValueBooleanContext ctx) {
        return visitBool(ctx.bool());
    }

    @Override
    public LocalDateTime visitValueDate(GraqlParser.ValueDateContext ctx) {
        return LocalDate.parse(ctx.DATE_().getText(), DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay();
    }

    @Override
    public LocalDateTime visitValueDateTime(GraqlParser.ValueDateTimeContext ctx) {
        return LocalDateTime.parse(ctx.DATETIME_().getText(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    @Override
    public Boolean visitBool(GraqlParser.BoolContext ctx) {
        return ctx.TRUE() != null;
    }

    @Override
    public Query.DataType visitDatatype(GraqlParser.DatatypeContext datatype) {
        if (datatype.BOOLEAN() != null) {
            return Query.DataType.BOOLEAN;
        } else if (datatype.DATE() != null) {
            return Query.DataType.DATE;
        } else if (datatype.DOUBLE() != null) {
            return Query.DataType.DOUBLE;
        } else if (datatype.LONG() != null) {
            return Query.DataType.LONG;
        } else if (datatype.STRING() != null) {
            return Query.DataType.STRING;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised " + datatype);
        }
    }

    private ValuePredicate visitPredicate(GraqlParser.PredicateContext ctx) {
        return (ValuePredicate) visit(ctx);
    }

    private Object visitValueOrVar(GraqlParser.ValueOrVarContext ctx) {
        return visit(ctx);
    }

    private Object visitValue(GraqlParser.ValueContext ctx) {
        return visit(ctx);
    }

    private String getString(TerminalNode string) {
        // Remove surrounding quotes
        String unquoted = unquoteString(string);
        return StringUtil.unescapeString(unquoted);
    }

    private String unquoteString(TerminalNode string) {
        return string.getText().substring(1, string.getText().length() - 1);
    }

    /**
     * Compose two functions together into a single function
     */
    private <T> UnaryOperator<T> compose(UnaryOperator<T> before, UnaryOperator<T> after) {
        return x -> after.apply(before.apply(x));
    }

    /**
     * Chain a stream of functions into a single function, which applies each one after the other
     */
    private <T> UnaryOperator<T> chainOperators(Stream<UnaryOperator<T>> operators) {
        return operators.reduce(UnaryOperator.identity(), this::compose);
    }

    private UnaryOperator<Statement> getVarProperties(List<? extends ParserRuleContext> contexts) {
        return chainOperators(contexts.stream().map(ctx -> (UnaryOperator<Statement>) visit(ctx)));
    }

    protected long getInteger(TerminalNode integer) {
        return Long.parseLong(integer.getText());
    }

    private <T> ValuePredicate applyPredicate(
            Function<T, ValuePredicate> valPred, Function<Statement, ValuePredicate> varPred, Object obj
    ) {
        if (obj instanceof Statement) {
            return varPred.apply((Statement) obj);
        } else {
            return valPred.apply((T) obj);
        }
    }
}
