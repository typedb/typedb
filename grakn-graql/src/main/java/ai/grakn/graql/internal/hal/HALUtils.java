/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.hal;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.util.Schema;
import com.theoryinpractise.halbuilder.api.Representation;
import javafx.util.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Utils class used by HALBuilders
 *
 * @author Marco Scoppetta
 */
public class HALUtils {

    final static String EXPLORE_CONCEPT_LINK = "explore";

    // - Edges names

    final static String ISA_EDGE = "isa";
    final static String SUB_EDGE = "sub";
    final static String OUTBOUND_EDGE = "OUT";
    final static String INBOUND_EDGE = "IN";
    final static String HAS_ROLE_EDGE = "has-role";
    final static String HAS_RESOURCE_EDGE = "has-resource";
    final static String PLAYS_ROLE_EDGE = "plays-role";
    final static String HAS_EMPTY_ROLE_EDGE = "EMPTY-GRAKN-ROLE";


    // - State properties

    final static String ID_PROPERTY = "_id";
    final static String TYPE_PROPERTY = "_type";
    final static String BASETYPE_PROPERTY = "_baseType";
    final static String DIRECTION_PROPERTY = "_direction";
    final static String VALUE_PROPERTY = "_value";
    final static String NAME_PROPERTY = "_name";


    static Schema.BaseType getBaseType(Instance instance) {
        if (instance.isEntity()) {
            return Schema.BaseType.ENTITY;
        } else if (instance.isRelation()) {
            return Schema.BaseType.RELATION;
        } else if (instance.isResource()) {
            return Schema.BaseType.RESOURCE;
        } else if (instance.isRule()) {
            return Schema.BaseType.RULE;
        } else {
            throw new RuntimeException("Unrecognized base type of " + instance);
        }
    }

    static Schema.BaseType getBaseType(Type type) {
        if (type.isEntityType()) {
            return Schema.BaseType.ENTITY_TYPE;
        } else if (type.isRelationType()) {
            return Schema.BaseType.RELATION_TYPE;
        } else if (type.isResourceType()) {
            return Schema.BaseType.RESOURCE_TYPE;
        } else if (type.isRuleType()) {
            return Schema.BaseType.RULE_TYPE;
        } else if (type.isRoleType()) {
            return Schema.BaseType.ROLE_TYPE;
        } else if (type.getLabel().equals(Schema.MetaSchema.CONCEPT.getLabel())) {
            return Schema.BaseType.TYPE;
        } else {
            throw new RuntimeException("Unrecognized base type of " + type);
        }
    }

    static void generateConceptState(Representation resource, Concept concept) {

        resource.withProperty(ID_PROPERTY, concept.getId().getValue());

        if (concept.isInstance()) {
            Instance instance = concept.asInstance();
            resource.withProperty(TYPE_PROPERTY, instance.type().getLabel().getValue())
                    .withProperty(BASETYPE_PROPERTY, getBaseType(instance).name());
        } else {
            resource.withProperty(BASETYPE_PROPERTY, getBaseType(concept.asType()).name());
        }

        if (concept.isResource()) {
            resource.withProperty(VALUE_PROPERTY, concept.asResource().getValue());
        }
        if (concept.isType()) {
            resource.withProperty(NAME_PROPERTY, concept.asType().getLabel().getValue());
        }
    }

    static Map<VarAdmin, Pair<Map<VarName, String>, String>> computeRoleTypesFromQuery(MatchQuery matchQuery, Answer firstAnswer) {
        final Map<VarAdmin, Pair<Map<VarName, String>, String>> roleTypes = new HashMap<>();
        AnswerExplanation firstExplanation = firstAnswer.getExplanation();
        if (firstExplanation.isEmpty()) {
            return computeRoleTypesFromQueryNoReasoner(matchQuery);
        } else {
            if (firstExplanation.isRuleExplanation() || firstExplanation.isLookupExplanation()) {
                Atom atom = ((ReasonerAtomicQuery) firstAnswer.getExplanation().getQuery()).getAtom();
                if (atom.isRelation()) {
                    VarAdmin varAdmin = atom.getPattern().asVar();
                    roleTypes.put(varAdmin, pairVarNamesRelationType(atom));
                }
            } else {
                firstAnswer.getExplanation().getAnswers().forEach(answer -> {
                    Atom atom = ((ReasonerAtomicQuery) answer.getExplanation().getQuery()).getAtom();
                    if (atom.isRelation()) {
                        VarAdmin varAdmin = atom.getPattern().asVar();
                        roleTypes.put(varAdmin, pairVarNamesRelationType(atom));
                    }
                });
            }

            return roleTypes;
        }
    }

    private static Map<VarAdmin, Pair<Map<VarName, String>, String>> computeRoleTypesFromQueryNoReasoner(MatchQuery matchQuery) {
        final Map<VarAdmin, Pair<Map<VarName, String>, String>> roleTypes = new HashMap<>();
        matchQuery.admin().getPattern().getVars().forEach(var -> {
            if (var.getProperty(RelationProperty.class).isPresent() && !var.isUserDefinedName()) {
                Map<VarName, String> tempMap = new HashMap<>();
                var.getProperty(RelationProperty.class).get()
                        .getRelationPlayers().forEach(x -> {
                            tempMap.put(x.getRolePlayer().getVarName(),
                                    (x.getRoleType().isPresent()) ? x.getRoleType().get().getPrintableName() : HAS_EMPTY_ROLE_EDGE);
                        }
                );
                String relationType = null;
                if(var.getProperty(IsaProperty.class).isPresent()) {
                    Optional<TypeLabel> relOptional = var.getProperty(IsaProperty.class).get().getType().getTypeLabel();
                    relationType = (relOptional.isPresent()) ? relOptional.get().getValue() : "";
                }else{
                    relationType = "";
                }

                roleTypes.put(var, new Pair<>(tempMap, relationType));
            }
        });
        return roleTypes;
    }

    private static Pair<Map<VarName, String>, String> pairVarNamesRelationType(Atom atom) {
        Relation reasonerRel = ((Relation) atom);
        Map<VarName, String> varNamesToRole = new HashMap<>();
        // Put all the varNames in the map with EMPTY-ROLE role
        reasonerRel.getRolePlayers().forEach(varName -> varNamesToRole.put(varName, HAS_EMPTY_ROLE_EDGE));
        // Overrides the varNames that have roles in the previous map
        reasonerRel.getRoleVarTypeMap().entrySet().forEach(entry -> varNamesToRole.put(entry.getValue().getKey(), entry.getKey().getLabel().getValue()));

        String relationType = (reasonerRel.getType() != null) ? reasonerRel.getType().getLabel().getValue() : "";
        return new Pair<>(varNamesToRole, relationType);
    }

    static Map<VarAdmin, Boolean> buildInferredRelationsMap(Answer firstAnswer) {
        final Map<VarAdmin, Boolean> inferredRelations = new HashMap<>();
        AnswerExplanation firstExplanation = firstAnswer.getExplanation();
        if (firstExplanation.isRuleExplanation() || firstExplanation.isLookupExplanation()) {
            Atom atom = ((ReasonerAtomicQuery) firstAnswer.getExplanation().getQuery()).getAtom();
            if (atom.isRelation()) {
                VarAdmin varAdmin = atom.getPattern().asVar();
                inferredRelations.put(varAdmin, firstAnswer.getExplanation().isRuleExplanation());
            }
        } else {
            firstAnswer.getExplanation().getAnswers().forEach(answer -> {
                Atom atom = ((ReasonerAtomicQuery) answer.getExplanation().getQuery()).getAtom();
                if (atom.isRelation()) {
                    VarAdmin varAdmin = atom.getPattern().asVar();
                    inferredRelations.put(varAdmin, answer.getExplanation().isRuleExplanation());
                }
            });
        }

        return inferredRelations;
    }

}
