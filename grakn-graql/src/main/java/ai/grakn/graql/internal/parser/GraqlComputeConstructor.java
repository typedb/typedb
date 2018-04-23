/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.parser;

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.analytics.ComputeQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.analytics.StatisticsQuery;
import ai.grakn.graql.analytics.ConnectedComponentQuery;
import ai.grakn.graql.analytics.CorenessQuery;
import ai.grakn.graql.analytics.CountQuery;
import ai.grakn.graql.analytics.DegreeQuery;
import ai.grakn.graql.analytics.KCoreQuery;
import ai.grakn.graql.analytics.PathQuery;
import ai.grakn.graql.analytics.PathsQuery;
import ai.grakn.graql.internal.antlr.GraqlParser;
import ai.grakn.util.GraqlSyntax;
import org.antlr.v4.runtime.ParserRuleContext;

/**
 * Graql Compute Query constructor class. This class helps the GraqlConstructor class to construct Graql Compute
 * Queries given context information provided by GraqlParser
 *
 * @author Haikal Pribadi
 */
public class GraqlComputeConstructor {
    private final QueryBuilder queryBuilder;
    private final GraqlConstructor graqlConstructor;

    GraqlComputeConstructor(GraqlConstructor graqlConstructor, QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
        this.graqlConstructor = graqlConstructor;
    }

    /**
     *
     * @param computeQuery
     * @return
     */
    public ComputeQuery<?> constructComputeQuery(GraqlParser.ComputeQueryContext computeQuery) {
        if (computeQuery.computeMethod().COUNT() != null) {
            return constructComputeCountQuery(computeQuery.computeConditions());

        } else if (computeQuery.computeMethod().MIN() != null) {
            return constructComputeStatisticsQuery(computeQuery.computeConditions(), GraqlParser.MIN);

        } else if (computeQuery.computeMethod().MAX() != null) {
            return constructComputeStatisticsQuery(computeQuery.computeConditions(), GraqlParser.MAX);

        } else if (computeQuery.computeMethod().MEDIAN() != null) {
            return constructComputeStatisticsQuery(computeQuery.computeConditions(), GraqlParser.MEDIAN);

        } else if (computeQuery.computeMethod().MEAN() != null) {
            return constructComputeStatisticsQuery(computeQuery.computeConditions(), GraqlParser.MEAN);

        } else if (computeQuery.computeMethod().STD() != null) {
            return constructComputeStatisticsQuery(computeQuery.computeConditions(), GraqlParser.STD);

        } else if (computeQuery.computeMethod().SUM() != null) {
            return constructComputeStatisticsQuery(computeQuery.computeConditions(), GraqlParser.SUM);

        } else if (computeQuery.computeMethod().PATH() != null) {
            return constructComputePathQuery(computeQuery.computeConditions());

        } else if (computeQuery.computeMethod().PATHS() != null) {
            return constructComputePathsQuery(computeQuery.computeConditions());

        } else if (computeQuery.computeMethod().CENTRALITY() != null) {
            return constructComputeCentralityQuery(computeQuery.computeConditions());

        } else if (computeQuery.computeMethod().CLUSTER() != null) {
            return constructComputeClusterQuery(computeQuery.computeConditions());

        }
        throw GraqlQueryException.invalidComputeMethod();
    }

    /**
     * Constructs a graql compute count query
     *
     * @param conditions
     * @return CountQuery object
     */
    private CountQuery constructComputeCountQuery(GraqlParser.ComputeConditionsContext conditions) {

        CountQuery computeCount = queryBuilder.compute().count();

        if(conditions != null) {
            for (GraqlParser.ComputeConditionContext condition : conditions.computeCondition()) {
                switch (((ParserRuleContext) condition.getChild(1)).getRuleIndex()) {
                    // The 'compute count' query may be given 'in <types>' condition
                    case GraqlParser.RULE_computeInLabels:
                        computeCount = computeCount.in(graqlConstructor.visitLabels(condition.computeInLabels().labels()));
                        break;
                    // The 'compute count query does not accept any other condition
                    default:
                        throw GraqlQueryException.invalidComputeCountCondition();
                }
            }
        }

        return computeCount;
    }

    /**
     * Constructs a graql compute statistics queries: min, max, median, mean, std, sum
     *
     * @param conditions
     * @param methodIndex
     * @return A subtype of StatisticsQuery object
     */
    private StatisticsQuery<?> constructComputeStatisticsQuery(GraqlParser.ComputeConditionsContext conditions, int methodIndex) {

        StatisticsQuery<?> computeStatistics = initialiseComputeStatisticsQuery(methodIndex);

        boolean computeOfConditionExists = false;
        boolean invalidComputeConditionExists = false;

        if(conditions != null) {
            for (GraqlParser.ComputeConditionContext condition : conditions.computeCondition()) {
                switch (((ParserRuleContext) condition.getChild(1)).getRuleIndex()) {
                    // The 'compute <statistics>' query requires a 'of <types>' condition
                    case GraqlParser.RULE_computeOfLabels:
                        computeStatistics = computeStatistics.of(graqlConstructor.visitLabels(condition.computeOfLabels().labels()));
                        computeOfConditionExists = true;
                        break;
                    // The 'compute <statistics>' query may be given 'in <types>' condition
                    case GraqlParser.RULE_computeInLabels:
                        computeStatistics = computeStatistics.in(graqlConstructor.visitLabels(condition.computeInLabels().labels()));
                        break;
                    // The 'compute <statistics>' query does not accept any other condition
                    default:
                        invalidComputeConditionExists = true;
                        break;
                }
            }
        }

        if (!computeOfConditionExists || invalidComputeConditionExists) {
            throwComputeStatisticsException(methodIndex, computeOfConditionExists, invalidComputeConditionExists);
        }

        return computeStatistics;
    }

    /**
     * Helper method to construct specific subtype of StatisticsQuery depending on a given compute statistics method
     * index.
     *
     * @param methodIndex
     * @return A subtype of StatisticsQuery object
     */
    private StatisticsQuery<?> initialiseComputeStatisticsQuery(int methodIndex) {

        switch (methodIndex) {
            case GraqlParser.MIN:
                return queryBuilder.compute().min();
            case GraqlParser.MAX:
                return queryBuilder.compute().max();
            case GraqlParser.MEDIAN:
                return queryBuilder.compute().median();
            case GraqlParser.MEAN:
                return queryBuilder.compute().mean();
            case GraqlParser.STD:
                return queryBuilder.compute().std();
            case GraqlParser.SUM:
                return queryBuilder.compute().sum();
            default:
                throw GraqlQueryException.invalidComputeMethod();
        }
    }

    /**
     * Helper method to throw a specific GraqlQueryException depending on a given compute statistics method index and
     * compute condition characteristics.
     *
     * @param methodIndex
     * @param computeOfConditionExists
     * @param invalidComputeConditionExists
     */
    private void throwComputeStatisticsException
    (int methodIndex, boolean computeOfConditionExists, boolean invalidComputeConditionExists) {

        switch (methodIndex) {
            case GraqlParser.MIN:
                if (!computeOfConditionExists) throw GraqlQueryException.invalidComputeMinMissingCondition();
                if (invalidComputeConditionExists) throw GraqlQueryException.invalidComputeMinCondition();
            case GraqlParser.MAX:
                if (!computeOfConditionExists) throw GraqlQueryException.invalidComputeMaxMissingCondition();
                if (invalidComputeConditionExists) throw GraqlQueryException.invalidComputeMaxCondition();
            case GraqlParser.MEDIAN:
                if (!computeOfConditionExists) throw GraqlQueryException.invalidComputeMedianMissingCondition();
                if (invalidComputeConditionExists) throw GraqlQueryException.invalidComputeMedianCondition();
            case GraqlParser.MEAN:
                if (!computeOfConditionExists) throw GraqlQueryException.invalidComputeMeanMissingCondition();
                if (invalidComputeConditionExists) throw GraqlQueryException.invalidComputeMeanCondition();
            case GraqlParser.STD:
                if (!computeOfConditionExists) throw GraqlQueryException.invalidComputeStdMissingCondition();
                if (invalidComputeConditionExists) throw GraqlQueryException.invalidComputeStdCondition();
            case GraqlParser.SUM:
                if (!computeOfConditionExists) throw GraqlQueryException.invalidComputeSumMissingCondition();
                if (invalidComputeConditionExists) throw GraqlQueryException.invalidComputeSumCondition();
            default:
                break;
        }
    }

    /**
     * Constructs graql compute path query
     *
     * @param conditions
     * @return PathQuery object
     */
    private PathQuery constructComputePathQuery(GraqlParser.ComputeConditionsContext conditions) {

        PathQuery computePath = queryBuilder.compute().path();
        boolean computeFromIDExists = false;
        boolean computeToIDExists = false;

        if (conditions != null) {
            for (GraqlParser.ComputeConditionContext condition : conditions.computeCondition()) {
                switch (((ParserRuleContext) condition.getChild(1)).getRuleIndex()) {
                    // The 'compute path' query requires a 'from <id>' condition
                    case GraqlParser.RULE_computeFromID:
                        computePath = computePath.from(graqlConstructor.visitId(condition.computeFromID().id()));
                        computeFromIDExists = true;
                        break;
                    // The 'compute path' query requires a 'to <id>' condition
                    case GraqlParser.RULE_computeToID:
                        computePath = computePath.to(graqlConstructor.visitId(condition.computeToID().id()));
                        computeToIDExists = true;
                        break;
                    // The 'compute path' query may be given 'in <types>' condition
                    case GraqlParser.RULE_computeInLabels:
                        computePath = computePath.in(graqlConstructor.visitLabels(condition.computeInLabels().labels()));
                        break;
                    // The 'compute path' query does not accept any other condition
                    default:
                        throw GraqlQueryException.invalidComputePathCondition();
                }
            }
        }

        if (!computeFromIDExists || !computeToIDExists) {
            throw GraqlQueryException.invalidComputePathMissingCondition();
        }

        return computePath;
    }

    /**
     * Constructs a graql compute path query
     *
     * @param conditions
     * @return PathQuery object
     */
    // TODO this function should be merged with the [singular] constructComputePathQuery() once they have an abstraction
    private PathsQuery constructComputePathsQuery(GraqlParser.ComputeConditionsContext conditions) {

        PathsQuery computePaths = queryBuilder.compute().paths();

        boolean computeFromIDExists = false;
        boolean computeToIDExists = false;

        if (conditions != null) {
            for (GraqlParser.ComputeConditionContext condition : conditions.computeCondition()) {
                switch (((ParserRuleContext) condition.getChild(1)).getRuleIndex()) {
                    // The 'compute paths' query requires a 'from <id>' condition
                    case GraqlParser.RULE_computeFromID:
                        computePaths = computePaths.from(graqlConstructor.visitId(condition.computeFromID().id()));
                        computeFromIDExists = true;
                        break;
                    // The 'compute paths' query requires a 'to <id>' condition
                    case GraqlParser.RULE_computeToID:
                        computePaths = computePaths.to(graqlConstructor.visitId(condition.computeToID().id()));
                        computeToIDExists = true;
                        break;
                    // The 'compute paths' query may be given 'in <types>' condition
                    case GraqlParser.RULE_computeInLabels:
                        computePaths = computePaths.in(graqlConstructor.visitLabels(condition.computeInLabels().labels()));
                        break;
                    // The 'compute paths' query does not accept any other condition
                    default:
                        throw GraqlQueryException.invalidComputePathsCondition();
                }
            }
        }

        if (!computeFromIDExists || !computeToIDExists) {
            throw GraqlQueryException.invalidComputePathsMissingCondition();
        }

        return computePaths;
    }

    /**
     * Constructs graql 'compute centrality' query
     *
     * @param conditions
     * @return A subtype of ComputeQuery object: CorenessQuery or DegreeQuery object
     */
    private ComputeQuery<?> constructComputeCentralityQuery(GraqlParser.ComputeConditionsContext conditions) {

        GraqlParser.LabelsContext computeOfTypes = null;
        GraqlParser.LabelsContext computeInTypes = null;
        GraqlParser.ComputeAlgorithmContext computeAlgorithm = null;
        GraqlParser.ComputeArgsContext computeArgs = null;

        if (conditions != null) {
            for (GraqlParser.ComputeConditionContext condition : conditions.computeCondition()) {
                switch (((ParserRuleContext) condition.getChild(1)).getRuleIndex()) {
                    // The 'compute centrality' query requires 'using <algorithm>' condition
                    case GraqlParser.RULE_computeAlgorithm:
                        computeAlgorithm = condition.computeAlgorithm();
                        break;
                    // The 'compute centrality' query may be given 'of <types>' condition
                    case GraqlParser.RULE_computeOfLabels:
                        computeOfTypes = condition.computeOfLabels().labels();
                        break;
                    // The 'compute centrality' query may be given 'in <types>' condition
                    case GraqlParser.RULE_computeInLabels:
                        computeInTypes = condition.computeInLabels().labels();
                        break;
                    // The 'compute centrality' query may be given 'where <args>' condition
                    case GraqlParser.RULE_computeArgs:
                        computeArgs = condition.computeArgs();
                        break;
                    // The 'compute centrality' query does not accept any other condition
                    default:
                        throw GraqlQueryException.invalidComputeCentralityCondition();
                }
            }
        }

        // The 'compute centrality' query requires 'using <algorithm>' condition
        if (computeAlgorithm == null) {
            throw GraqlQueryException.invalidComputeCentralityMissingCondition();

        } else if (computeAlgorithm.getText().equals(GraqlSyntax.Compute.Algorithm.DEGREE)) {
            return constructComputeCentralityUsingDegreeQuery(computeOfTypes, computeInTypes, computeArgs);

        } else if (computeAlgorithm.getText().equals(GraqlSyntax.Compute.Algorithm.K_CORE)) {
            return constructComputeCentralityUsingKCoreQuery(computeOfTypes, computeInTypes, computeArgs);

        }
        //TODO: The if checks above compares Strings because our Grammar definition inconsistently declares strings.
        //TODO: We should make the grammar definition more consistent and clean up these String comparisons

        throw GraqlQueryException.invalidComputeCentralityAlgorithm();
    }

    /**
     * Constructs graql 'compute centrality using degree' query
     *
     * @param computeOfTypes
     * @param computeInTypes
     * @param computeArgs
     * @return A DegreeQuery object
     */
    private DegreeQuery constructComputeCentralityUsingDegreeQuery
    (GraqlParser.LabelsContext computeOfTypes, GraqlParser.LabelsContext computeInTypes,
     GraqlParser.ComputeArgsContext computeArgs) {

        // The 'compute centrality using degree' query does not accept 'where <arguments>' condition
        if (computeArgs != null) throw GraqlQueryException.invalidComputeCentralityUsingDegreeCondition();

        DegreeQuery computeCentrality = queryBuilder.compute().centrality().usingDegree();

        // The 'compute centrality using degree' query can be given 'of <types>' or 'in <types>' condition
        if (computeOfTypes != null) computeCentrality.of(graqlConstructor.visitLabels(computeOfTypes));
        if (computeInTypes != null) computeCentrality.in(graqlConstructor.visitLabels(computeInTypes));

        return computeCentrality;
    }

    /**
     * Constructs graql 'compute centrality using k-core' query
     *
     * @param computeOfTypes
     * @param computeInTypes
     * @param computeArgs
     * @return A CorenessQuery object
     */
    private CorenessQuery constructComputeCentralityUsingKCoreQuery
    (GraqlParser.LabelsContext computeOfTypes, GraqlParser.LabelsContext computeInTypes,
     GraqlParser.ComputeArgsContext computeArgs) {

        CorenessQuery computeCentrality = queryBuilder.compute().centrality().usingKCore();

        // The 'compute centrality using k-core' query can be given 'of <types>' or 'in <types>' condition
        if (computeOfTypes != null) computeCentrality.of(graqlConstructor.visitLabels(computeOfTypes));
        if (computeInTypes != null) computeCentrality.in(graqlConstructor.visitLabels(computeInTypes));

        // The 'compute centrality using k-core' query only looks for 'min-k = <value>' argument in 'where <arguments>'
        if (computeArgs != null) return constructComputeCentralityUsingKCoreQueryWithMinK(computeCentrality, computeArgs);

        return computeCentrality;
    }

    /**
     * Constructs graql 'compute centrality using k-core, where min-k = <value>' query
     *
     * @param computeCentralityUsingKCore
     * @param computeArgs
     * @return A CorenessQuery object
     */
    private CorenessQuery constructComputeCentralityUsingKCoreQueryWithMinK
    (CorenessQuery computeCentralityUsingKCore, GraqlParser.ComputeArgsContext computeArgs) {

        // If an argument is provided, it can only be the 'min-k = <value>' argument
        for (GraqlParser.ComputeArgContext arg : graqlConstructor.visitComputeArgs(computeArgs)) {
            if (arg instanceof GraqlParser.ComputeArgMinKContext) {
                computeCentralityUsingKCore.minK(graqlConstructor.getInteger(((GraqlParser.ComputeArgMinKContext) arg).INTEGER()));
            } else {
                throw GraqlQueryException.invalidComputeCentralityUsingKCoreArgs();
            }
        }

        return computeCentralityUsingKCore;
    }

    /**
     *
     * @param conditions
     * @return
     */
    private ComputeQuery<?> constructComputeClusterQuery(GraqlParser.ComputeConditionsContext conditions) {

        GraqlParser.LabelsContext computeInTypes = null;
        GraqlParser.ComputeAlgorithmContext computeAlgorithm = null;
        GraqlParser.ComputeArgsContext computeArgs = null;

        if (conditions != null) {
            for (GraqlParser.ComputeConditionContext condition : conditions.computeCondition()) {
                switch (((ParserRuleContext) condition.getChild(1)).getRuleIndex()) {
                    // The 'compute cluster' query requires 'using <algorithm>' condition
                    case GraqlParser.RULE_computeAlgorithm:
                        computeAlgorithm = condition.computeAlgorithm();
                        break;
                    // The 'compute cluster' query may be given 'in <types>' condition
                    case GraqlParser.RULE_computeInLabels:
                        computeInTypes = condition.computeInLabels().labels();
                        break;
                    // The 'compute cluster' query may be given 'where <args>' condition
                    case GraqlParser.RULE_computeArgs:
                        computeArgs = condition.computeArgs();
                        break;
                    // The 'compute cluster' query does not accept any other condition
                    default:
                        throw GraqlQueryException.invalidComputeClusterCondition();
                }
            }
        }

        // The 'compute cluster' query requires 'using <algorithm>' condition
        if (computeAlgorithm == null) {
            throw GraqlQueryException.invalidComputeClusterMissingCondition();

        } else if (computeAlgorithm.getText().equals(GraqlSyntax.Compute.Algorithm.CONNECTED_COMPONENT)) {
            return constructComputeClusterUsingConnectedComponentQuery(computeInTypes, computeArgs);

        } else if (computeAlgorithm.getText().equals(GraqlSyntax.Compute.Algorithm.K_CORE)) {
            return constructComputeClusterUsingKCoreQuery(computeInTypes, computeArgs);

        }
        //TODO: The if checks above compares Strings because our Grammar definition inconsistently declares strings.
        //TODO: We should make the grammar definition more consistent and clean up these String comparisons

        throw GraqlQueryException.invalidComputeClusterAlgorithm();
    }

    /**
     *
     * @param computeInTypes
     * @param computeArgs
     * @return
     */
    private ConnectedComponentQuery constructComputeClusterUsingConnectedComponentQuery
            (GraqlParser.LabelsContext computeInTypes, GraqlParser.ComputeArgsContext computeArgs) {

        ConnectedComponentQuery computeCluster = queryBuilder.compute().cluster().usingConnectedComponent();

        // The 'compute cluster using connected-component' query can be given 'in <types>' condition
        if (computeInTypes != null) computeCluster.in(graqlConstructor.visitLabels(computeInTypes));

        // The 'compute cluster using connected-component' query can be given 'where <args>' condition:
        // The 'start = <id>', 'members = <bool>', 'size = <int>' arguments are accepted
        if (computeArgs != null) {
            for (GraqlParser.ComputeArgContext arg : graqlConstructor.visitComputeArgs(computeArgs)) {
                if (arg instanceof GraqlParser.ComputeArgStartContext) {
                    computeCluster.start(graqlConstructor.visitId(((GraqlParser.ComputeArgStartContext) arg).id()));
                } else if (arg instanceof GraqlParser.ComputeArgMembersContext) {
                    if (graqlConstructor.visitBool(((GraqlParser.ComputeArgMembersContext) arg).bool())) {
                        computeCluster.membersOn();
                    } else {
                        computeCluster.membersOff();
                    }
                } else if (arg instanceof GraqlParser.ComputeArgSizeContext) {
                    computeCluster.size(graqlConstructor.getInteger(((GraqlParser.ComputeArgSizeContext) arg).INTEGER()));
                } else {
                    throw GraqlQueryException.invalidComputeClusterUsingConnectedComponentArgument();
                }
            }
        }

        return computeCluster;
    }

    /**
     *
     * @param computeInTypes
     * @param computeArgs
     * @return
     */
    private KCoreQuery constructComputeClusterUsingKCoreQuery
    (GraqlParser.LabelsContext computeInTypes, GraqlParser.ComputeArgsContext computeArgs) {
        KCoreQuery computeCluster = queryBuilder.compute().cluster().usingKCore();

        // The 'compute cluster using connected-component' query can be given 'in <types>' condition
        if (computeInTypes != null) computeCluster.in(graqlConstructor.visitLabels(computeInTypes));

        // The 'compute cluster using connected-component' query can be given 'where <args>' condition:
        // The 'start = <id>', 'members = <bool>', 'size = <int>' arguments are accepted
        if (computeArgs != null) {
            for (GraqlParser.ComputeArgContext arg : graqlConstructor.visitComputeArgs(computeArgs)) {
                if (arg instanceof GraqlParser.ComputeArgKContext) {
                    computeCluster.k(graqlConstructor.getInteger(((GraqlParser.ComputeArgKContext) arg).INTEGER()));

                } else {
                    throw GraqlQueryException.invalidComputeClusterUsingKCoreArgument();
                }
            }
        }

        return computeCluster;
    }
}
