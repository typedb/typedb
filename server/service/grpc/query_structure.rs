/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::{variable::Variable, Type};
use compiler::query_structure::{
    FunctionReturnStructure, FunctionStructure, PipelineStructure, QueryStructure, QueryStructureConjunction,
    QueryStructureNestedPattern, QueryStructureStage, StructureVariableId,
};
use concept::{error::ConceptReadError, type_::type_manager::TypeManager};
use encoding::value::{label::Label, value::Value};
use error::todo_must_implement;
use ir::pattern::{
    constraint::{Comparator, Constraint, IsaKind, SubKind},
    ParameterID, Vertex,
};
use query::analyse::AnalysedQuery;
use storage::snapshot::ReadableSnapshot;
use typedb_protocol::conjunction_structure::{structure_constraint, structure_vertex};

use crate::service::grpc::{
    concept::{
        encode_attribute_type, encode_entity_type, encode_relation_type, encode_role_type, encode_value,
        encode_value_type,
    },
    document::encode_kind,
};

pub(crate) struct PipelineStructureContext<'a, Snapshot: ReadableSnapshot> {
    pub(crate) pipeline_structure: &'a PipelineStructure,
    pub(crate) snapshot: &'a Snapshot,
    pub(crate) type_manager: &'a TypeManager,
    pub(crate) role_names: HashMap<Variable, String>,
}
impl<'a, Snapshot: ReadableSnapshot> PipelineStructureContext<'a, Snapshot> {
    pub(crate) fn get_parameter_value(&self, param: &ParameterID) -> Option<Value<'static>> {
        debug_assert!(matches!(param, ParameterID::Value(_, _)));
        self.pipeline_structure.parameters.value(*param).cloned()
    }

    pub fn get_parameter_iid(&self, param: &ParameterID) -> Option<&[u8]> {
        self.pipeline_structure.parameters.iid(*param).map(|iid| iid.as_ref())
    }

    pub(crate) fn get_type(&self, label: &Label) -> Option<answer::Type> {
        self.pipeline_structure.parametrised_structure.resolved_labels.get(label).cloned()
    }

    pub(crate) fn get_call_syntax(&self, constraint: &Constraint<Variable>) -> Option<&String> {
        self.pipeline_structure.parametrised_structure.calls_syntax.get(constraint)
    }

    pub(crate) fn get_role_type(&self, variable: &Variable) -> Option<&str> {
        self.role_names.get(variable).map(|name| name.as_str())
    }
}

pub fn encode_analyzed_query(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    analyzed_query: &AnalysedQuery,
) -> Result<typedb_protocol::analyze::Res, Box<ConceptReadError>> {
    let structure = encode_query_structure(snapshot, type_manager, &analyzed_query.structure)?;
    let annotations = typedb_protocol::analyze::res::QueryAnnotations {};
    Ok(typedb_protocol::analyze::Res { structure: Some(structure), annotations: Some(annotations) })
}

pub fn encode_query_structure(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    query_structure: &QueryStructure,
) -> Result<typedb_protocol::analyze::res::QueryStructure, Box<ConceptReadError>> {
    let query = encode_pipeline_structure(snapshot, type_manager, query_structure.query.as_ref().unwrap())?;
    let preamble = query_structure
        .preamble
        .iter()
        .map(|func| encode_function_structure(snapshot, type_manager, func))
        .collect::<Result<_, _>>()?;

    todo_must_implement!("We removed the optionality of all this somewhere.");
    Ok(typedb_protocol::analyze::res::QueryStructure { query: Some(query), preamble })
}

fn encode_function_structure(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    function: &FunctionStructure,
) -> Result<typedb_protocol::analyze::res::query_structure::FunctionStructure, Box<ConceptReadError>> {
    use typedb_protocol::analyze::res::query_structure::function_structure;
    let body = encode_pipeline_structure(snapshot, type_manager, function.pipeline.as_ref().unwrap())?;
    let arguments = function.arguments.iter().map(|v| encode_structure_variable(*v)).collect();
    let returns = match &function.return_ {
        FunctionReturnStructure::Stream { variables } => {
            let variables = variables.iter().map(|v| encode_structure_variable(*v)).collect();
            function_structure::Returns::Stream(function_structure::ReturnOpStream { variables })
        }
        FunctionReturnStructure::Single { variables, selector } => {
            let selector = selector.clone();
            let variables = variables.iter().map(|v| encode_structure_variable(*v)).collect();
            function_structure::Returns::Single(function_structure::ReturnOpSingle { selector, variables })
        }
        FunctionReturnStructure::Check {} => function_structure::Returns::Check(function_structure::ReturnOpCheck {}),
        FunctionReturnStructure::Reduce { reducers } => {
            let reducers = reducers
                .iter()
                .map(|reducer| {
                    let variables = reducer.arguments.iter().map(|v| encode_structure_variable(*v)).collect();
                    typedb_protocol::analyze::res::query_structure::Reducer {
                        reducer: reducer.reducer.clone(),
                        variables,
                    }
                })
                .collect();
            function_structure::Returns::Reduce(function_structure::ReturnOpReduce { reducers })
        }
    };
    Ok(typedb_protocol::analyze::res::query_structure::FunctionStructure {
        body: Some(body),
        arguments,
        returns: Some(returns),
    })
}

fn encode_pipeline_structure(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    pipeline_structure: &PipelineStructure,
) -> Result<typedb_protocol::analyze::res::query_structure::PipelineStructure, Box<ConceptReadError>> {
    let conjunctions = pipeline_structure
        .parametrised_structure
        .conjunctions
        .iter()
        .map(|conj| encode_query_conjunction(snapshot, type_manager, &pipeline_structure, conj))
        .collect::<Result<Vec<_>, _>>()?;
    let variable_info = pipeline_structure
        .variable_names
        .iter()
        .map(|(var, name)| {
            (
                var.as_u32(),
                typedb_protocol::analyze::res::query_structure::pipeline_structure::VariableInfo { name: name.clone() },
            )
        })
        .collect();
    let stages =
        pipeline_structure.parametrised_structure.stages.iter().map(|stage| encode_query_stage(stage)).collect();
    let outputs = pipeline_structure.available_variables.iter().map(|var| encode_structure_variable(*var)).collect();
    Ok(typedb_protocol::analyze::res::query_structure::PipelineStructure {
        conjunctions,
        variable_info,
        stages,
        outputs,
    })
}

fn encode_query_stage(
    stage: &QueryStructureStage,
) -> typedb_protocol::analyze::res::query_structure::pipeline_structure::PipelineStage {
    use typedb_protocol::analyze::res::query_structure::pipeline_structure::{pipeline_stage, pipeline_stage::Stage};
    let variant = match stage {
        QueryStructureStage::Match { block } => Stage::Match(pipeline_stage::Match { block: block.as_u32() }),
        QueryStructureStage::Insert { block } => Stage::Insert(pipeline_stage::Insert { block: block.as_u32() }),
        QueryStructureStage::Delete { block, deleted_variables } => {
            let deleted_variables = deleted_variables.iter().map(|v| encode_structure_variable(*v)).collect();
            Stage::Delete(pipeline_stage::Delete { block: block.as_u32(), deleted_variables })
        }
        QueryStructureStage::Put { block } => Stage::Put(pipeline_stage::Put { block: block.as_u32() }),
        QueryStructureStage::Update { block } => Stage::Update(pipeline_stage::Update { block: block.as_u32() }),
        QueryStructureStage::Select { variables } => {
            let variables = variables.iter().map(|v| encode_structure_variable(*v)).collect();
            Stage::Select(pipeline_stage::Select { variables })
        }
        QueryStructureStage::Sort { variables } => {
            use typedb_protocol::analyze::res::query_structure::pipeline_structure::pipeline_stage::{
                sort, sort::sort_variable::SortDirection,
            };
            let sort_variables = variables
                .iter()
                .map(|v| {
                    let direction = if v.ascending { SortDirection::Asc } else { SortDirection::Desc };
                    sort::SortVariable {
                        variable: Some(encode_structure_variable(v.variable)),
                        direction: direction as i32,
                    }
                })
                .collect();
            Stage::Sort(pipeline_stage::Sort { sort_variables })
        }
        QueryStructureStage::Offset { offset } => Stage::Offset(pipeline_stage::Offset { offset: *offset }),
        QueryStructureStage::Limit { limit } => Stage::Limit(pipeline_stage::Limit { limit: *limit }),
        QueryStructureStage::Require { variables } => {
            let variables = variables.iter().map(|v| encode_structure_variable(*v)).collect();
            Stage::Require(pipeline_stage::Require { variables })
        }
        QueryStructureStage::Distinct => Stage::Distinct(pipeline_stage::Distinct {}),
        QueryStructureStage::Reduce { reducers, groupby } => {
            let groupby = groupby.iter().map(|v| encode_structure_variable(*v)).collect();
            let reducers = reducers
                .iter()
                .map(|reducer| {
                    let assigned = Some(encode_structure_variable(reducer.assigned));
                    let variables = reducer.reducer.arguments.iter().map(|v| encode_structure_variable(*v)).collect();
                    let reducer_name = reducer.reducer.reducer.clone();
                    let reducer = Some(typedb_protocol::analyze::res::query_structure::Reducer {
                        reducer: reducer_name,
                        variables,
                    });
                    pipeline_stage::reduce::ReduceAssign { assigned, reducer }
                })
                .collect();
            Stage::Reduce(pipeline_stage::Reduce { reducers, groupby })
        }
    };
    typedb_protocol::analyze::res::query_structure::pipeline_structure::PipelineStage { stage: Some(variant) }
}

fn encode_query_conjunction(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    pipeline_structure: &PipelineStructure,
    conjunction: &QueryStructureConjunction,
) -> Result<typedb_protocol::ConjunctionStructure, Box<ConceptReadError>> {
    let mut constraints = Vec::new();
    let role_names = conjunction
        .constraints
        .iter()
        .filter_map(|constraint| constraint.as_role_name())
        .map(|rolename| (rolename.type_().as_variable().unwrap(), rolename.name().to_owned()))
        .collect();
    let context = PipelineStructureContext { pipeline_structure, snapshot, type_manager, role_names };
    conjunction
        .constraints
        .iter()
        .try_for_each(|constraint| query_structure_constraint(&context, constraint, &mut constraints))?;
    conjunction.nested.iter().for_each(|nested| conjunction_structure_nested(nested, &mut constraints));
    Ok(typedb_protocol::ConjunctionStructure { constraints })
}

fn query_structure_constraint(
    context: &PipelineStructureContext<'_, impl ReadableSnapshot>,
    constraint: &Constraint<Variable>,
    constraints: &mut Vec<typedb_protocol::conjunction_structure::StructureConstraint>,
) -> Result<(), Box<ConceptReadError>> {
    let span = constraint.source_span().map(|span| structure_constraint::ConstraintSpan {
        begin: span.begin_offset as u64,
        end: span.end_offset as u64,
    });
    match constraint {
        Constraint::Links(links) => {
            let relation = encode_structure_vertex_variable(links.relation())?;
            let player = encode_structure_vertex_variable(links.player())?;
            let role = encode_structure_vertex_label_or_variable(context, links.role_type())?;
            constraints.push(typedb_protocol::conjunction_structure::StructureConstraint {
                span,
                constraint: Some(structure_constraint::Constraint::Links(structure_constraint::Links {
                    relation: Some(relation),
                    player: Some(player),
                    role: Some(role),
                    exactness: encode_exactness(false) as i32,
                })),
            });
        }
        Constraint::Has(has) => {
            constraints.push(typedb_protocol::conjunction_structure::StructureConstraint {
                span,
                constraint: Some(structure_constraint::Constraint::Has(structure_constraint::Has {
                    owner: Some(encode_structure_vertex_variable(has.owner())?),
                    attribute: Some(encode_structure_vertex_variable(has.attribute())?),
                    exactness: encode_exactness(false) as i32,
                })),
            });
        }

        Constraint::Isa(isa) => {
            constraints.push(typedb_protocol::conjunction_structure::StructureConstraint {
                span,
                constraint: Some(structure_constraint::Constraint::Isa(structure_constraint::Isa {
                    thing: Some(encode_structure_vertex_variable(isa.thing())?),
                    r#type: Some(encode_structure_vertex_label_or_variable(context, isa.type_())?),
                    exactness: encode_exactness(isa.isa_kind() == IsaKind::Exact) as i32,
                })),
            });
        }
        Constraint::Sub(sub) => {
            constraints.push(typedb_protocol::conjunction_structure::StructureConstraint {
                span,
                constraint: Some(structure_constraint::Constraint::Sub(structure_constraint::Sub {
                    subtype: Some(encode_structure_vertex_label_or_variable(context, sub.subtype())?),
                    supertype: Some(encode_structure_vertex_label_or_variable(context, sub.supertype())?),
                    exactness: encode_exactness(sub.sub_kind() == SubKind::Exact) as i32,
                })),
            });
        }
        Constraint::Owns(owns) => {
            constraints.push(typedb_protocol::conjunction_structure::StructureConstraint {
                span,
                constraint: Some(structure_constraint::Constraint::Owns(structure_constraint::Owns {
                    owner: Some(encode_structure_vertex_label_or_variable(context, owns.owner())?),
                    attribute: Some(encode_structure_vertex_label_or_variable(context, owns.attribute())?),
                    exactness: encode_exactness(false) as i32,
                })),
            });
        }
        Constraint::Relates(relates) => {
            constraints.push(typedb_protocol::conjunction_structure::StructureConstraint {
                span,
                constraint: Some(structure_constraint::Constraint::Relates(structure_constraint::Relates {
                    relation: Some(encode_structure_vertex_label_or_variable(context, relates.relation())?),
                    role: Some(encode_structure_vertex_label_or_variable(context, relates.role_type())?),
                    exactness: encode_exactness(false) as i32,
                })),
            });
        }
        Constraint::Plays(plays) => {
            constraints.push(typedb_protocol::conjunction_structure::StructureConstraint {
                span,
                constraint: Some(structure_constraint::Constraint::Plays(structure_constraint::Plays {
                    player: Some(encode_structure_vertex_label_or_variable(context, plays.player())?),
                    role: Some(encode_structure_vertex_label_or_variable(context, plays.role_type())?),
                    exactness: encode_exactness(false) as i32,
                })),
            });
        }
        Constraint::IndexedRelation(indexed) => {
            let span_1 = indexed.source_span_1().map(|span| structure_constraint::ConstraintSpan {
                begin: span.begin_offset as u64,
                end: span.end_offset as u64,
            });
            let span_2 = indexed.source_span_2().map(|span| structure_constraint::ConstraintSpan {
                begin: span.begin_offset as u64,
                end: span.end_offset as u64,
            });
            constraints.push(typedb_protocol::conjunction_structure::StructureConstraint {
                span: span_1,
                constraint: Some(structure_constraint::Constraint::Links(structure_constraint::Links {
                    relation: Some(encode_structure_vertex_variable(indexed.relation())?),
                    player: Some(encode_structure_vertex_variable(indexed.player_1())?),
                    role: Some(encode_structure_vertex_label_or_variable(context, indexed.role_type_1())?),
                    exactness: encode_exactness(false) as i32,
                })),
            });
            constraints.push(typedb_protocol::conjunction_structure::StructureConstraint {
                span: span_2,
                constraint: Some(structure_constraint::Constraint::Links(structure_constraint::Links {
                    relation: Some(encode_structure_vertex_variable(indexed.relation())?),
                    player: Some(encode_structure_vertex_variable(indexed.player_2())?),
                    role: Some(encode_structure_vertex_label_or_variable(context, indexed.role_type_2())?),
                    exactness: encode_exactness(false) as i32,
                })),
            });
        }
        Constraint::ExpressionBinding(expr) => {
            let text = context
                .get_call_syntax(constraint)
                .map_or_else(|| format!("Expression#{}", constraints.len()), |text| text.clone());
            let assigned = expr
                .ids_assigned()
                .map(|variable| encode_structure_vertex_variable(&Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            let arguments = expr
                .expression_ids()
                .map(|variable| encode_structure_vertex_variable(&Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            constraints.push(typedb_protocol::conjunction_structure::StructureConstraint {
                span,
                constraint: Some(structure_constraint::Constraint::Expression(structure_constraint::Expression {
                    text,
                    assigned,
                    arguments,
                })),
            });
        }
        Constraint::FunctionCallBinding(function_call) => {
            let text = context
                .get_call_syntax(constraint)
                .map_or_else(|| format!("Function#{}", constraints.len()), |text| text.clone());
            let assigned = function_call
                .ids_assigned()
                .map(|variable| encode_structure_vertex_variable(&Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            let arguments = function_call
                .function_call()
                .argument_ids()
                .map(|variable| encode_structure_vertex_value_or_variable(context, &Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            constraints.push(typedb_protocol::conjunction_structure::StructureConstraint {
                span,
                constraint: Some(structure_constraint::Constraint::FunctionCall(structure_constraint::FunctionCall {
                    name: text,
                    assigned,
                    arguments,
                })),
            });
        }
        Constraint::Is(is) => {
            constraints.push(typedb_protocol::conjunction_structure::StructureConstraint {
                span,
                constraint: Some(structure_constraint::Constraint::Is(structure_constraint::Is {
                    lhs: Some(encode_structure_vertex_variable(is.lhs())?),
                    rhs: Some(encode_structure_vertex_variable(is.rhs())?),
                })),
            });
        }
        Constraint::Iid(iid) => {
            let iid_bytes = Vec::from(context.get_parameter_iid(iid.iid().as_parameter().as_ref().unwrap()).unwrap());
            constraints.push(typedb_protocol::conjunction_structure::StructureConstraint {
                span,
                constraint: Some(structure_constraint::Constraint::Iid(structure_constraint::Iid {
                    concept: Some(encode_structure_vertex_variable(iid.var())?),
                    iid: iid_bytes,
                })),
            });
        }
        Constraint::Comparison(comparison) => {
            constraints.push(typedb_protocol::conjunction_structure::StructureConstraint {
                span,
                constraint: Some(structure_constraint::Constraint::Comparison(structure_constraint::Comparison {
                    lhs: Some(encode_structure_vertex_value_or_variable(context, comparison.lhs())?),
                    rhs: Some(encode_structure_vertex_value_or_variable(context, comparison.rhs())?),
                    comparator: encode_comparator(comparison.comparator()) as i32,
                })),
            })
        }
        Constraint::Kind(kind) => constraints.push(typedb_protocol::conjunction_structure::StructureConstraint {
            span,
            constraint: Some(structure_constraint::Constraint::Kind(structure_constraint::Kind {
                kind: encode_kind(kind.kind()) as i32,
                r#type: Some(encode_structure_vertex_label_or_variable(context, kind.type_())?),
            })),
        }),
        Constraint::Label(label) => constraints.push(typedb_protocol::conjunction_structure::StructureConstraint {
            span,
            constraint: Some(structure_constraint::Constraint::Label(structure_constraint::Label {
                r#type: Some(encode_structure_vertex_label_or_variable(context, label.type_())?),
                label: label
                    .type_label()
                    .as_label()
                    .expect("Expected constant label in label constraint")
                    .scoped_name()
                    .as_str()
                    .to_owned(),
            })),
        }),
        Constraint::Value(value) => constraints.push(typedb_protocol::conjunction_structure::StructureConstraint {
            span,
            constraint: Some(structure_constraint::Constraint::Value(structure_constraint::Value {
                attribute_type: Some(encode_structure_vertex_label_or_variable(context, value.attribute_type())?),
                value_type: Some(encode_ir_value_type(
                    value.value_type().clone(),
                    context.snapshot,
                    context.type_manager,
                )?),
            })),
        }),
        // Constraints that probably don't need to be handled
        Constraint::RoleName(_) => {} // Handled separately via resolved_role_names
        // Optimisations don't represent the structure
        Constraint::LinksDeduplication(_) | Constraint::Unsatisfiable(_) => {}
    };
    error::todo_must_implement!("This whole file may need an update. I def need to update kind");
    Ok(())
}

fn conjunction_structure_nested(
    nested: &QueryStructureNestedPattern,
    constraints: &mut Vec<typedb_protocol::conjunction_structure::StructureConstraint>,
) {
    let constraint = match nested {
        QueryStructureNestedPattern::Or { branches } => {
            let branches = branches.iter().map(|b| b.as_u32()).collect();
            structure_constraint::Constraint::Or(structure_constraint::Or { branches })
        }
        QueryStructureNestedPattern::Not { conjunction } => {
            structure_constraint::Constraint::Not(structure_constraint::Not { conjunction: conjunction.as_u32() })
        }
        QueryStructureNestedPattern::Try { conjunction } => {
            structure_constraint::Constraint::Try(structure_constraint::Try { conjunction: conjunction.as_u32() })
        }
    };
    constraints
        .push(typedb_protocol::conjunction_structure::StructureConstraint { span: None, constraint: Some(constraint) });
}

fn encode_exactness(is_exact: bool) -> structure_constraint::ConstraintExactness {
    match is_exact {
        true => structure_constraint::ConstraintExactness::Exact,
        false => structure_constraint::ConstraintExactness::Subtypes,
    }
}

fn encode_structure_variable(id: StructureVariableId) -> typedb_protocol::conjunction_structure::Variable {
    typedb_protocol::conjunction_structure::Variable { id: id.as_u32() }
}

fn encode_structure_vertex_variable(
    vertex: &Vertex<Variable>,
) -> Result<typedb_protocol::conjunction_structure::StructureVertex, Box<ConceptReadError>> {
    let variable = vertex.as_variable().expect("Expected variable");
    let encoded_variable = encode_structure_variable(variable.into());
    Ok(typedb_protocol::conjunction_structure::StructureVertex {
        vertex: Some(structure_vertex::Vertex::Variable(encoded_variable)),
    })
}

fn encode_structure_vertex_label_or_variable(
    context: &PipelineStructureContext<'_, impl ReadableSnapshot>,
    vertex: &Vertex<Variable>,
) -> Result<typedb_protocol::conjunction_structure::StructureVertex, Box<ConceptReadError>> {
    match vertex {
        Vertex::Variable(_) => encode_structure_vertex_variable(vertex),
        Vertex::Label(label) => {
            let encoded_type = match context.get_type(label) {
                Some(type_) => {
                    let resolved = encode_structure_vertex_label(context.snapshot, context.type_manager, &type_)?;
                    structure_vertex::label::Label::Resolved(resolved)
                }
                None => {
                    let unresolved = label.scoped_name.as_str().to_owned();
                    structure_vertex::label::Label::FailedInference(unresolved)
                }
            };
            let label = structure_vertex::Label { label: Some(encoded_type) };
            Ok(typedb_protocol::conjunction_structure::StructureVertex {
                vertex: Some(structure_vertex::Vertex::Label(label)),
            })
        }
        Vertex::Parameter(_) => unreachable!("Expected variable or label"),
    }
}

fn encode_structure_vertex_value_or_variable(
    context: &PipelineStructureContext<'_, impl ReadableSnapshot>,
    vertex: &Vertex<Variable>,
) -> Result<typedb_protocol::conjunction_structure::StructureVertex, Box<ConceptReadError>> {
    match vertex {
        Vertex::Variable(_) => encode_structure_vertex_variable(vertex),
        Vertex::Parameter(parameter) => {
            let value = context.get_parameter_value(&parameter).expect("Expected values to be present");
            Ok(typedb_protocol::conjunction_structure::StructureVertex {
                vertex: Some(structure_vertex::Vertex::Value(encode_value(value))),
            })
        }
        Vertex::Label(_) => unreachable!("Expected variable or value"),
    }
}

fn encode_structure_vertex_label(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    type_: &Type,
) -> Result<typedb_protocol::Type, Box<ConceptReadError>> {
    let encoded = match type_ {
        Type::Entity(entity) => {
            typedb_protocol::r#type::Type::EntityType(encode_entity_type(entity, snapshot, type_manager).unwrap())
        }
        Type::Relation(relation) => {
            typedb_protocol::r#type::Type::RelationType(encode_relation_type(relation, snapshot, type_manager)?)
        }
        Type::Attribute(attribute) => {
            typedb_protocol::r#type::Type::AttributeType(encode_attribute_type(attribute, snapshot, type_manager)?)
        }
        Type::RoleType(role) => {
            typedb_protocol::r#type::Type::RoleType(encode_role_type(role, snapshot, type_manager)?)
        }
    };
    Ok(typedb_protocol::Type { r#type: Some(encoded) })
}

fn encode_ir_value_type(
    value_type: ir::pattern::ValueType,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<typedb_protocol::ValueType, Box<ConceptReadError>> {
    use ir::pattern::ValueType;
    // TODO: Structs being just a string is a bit sus
    match value_type {
        ValueType::Builtin(value_type) => encode_value_type(value_type, snapshot, type_manager),
        ValueType::Struct(name) => {
            Ok(typedb_protocol::ValueType { value_type: Some(
                typedb_protocol::value_type::ValueType::Struct(typedb_protocol::value_type::Struct { name })
            )})
        }
    }
}

fn encode_comparator(comparator: Comparator) -> structure_constraint::comparison::Comparator {
    use structure_constraint::comparison::Comparator as Cmp;
    match comparator {
        Comparator::Equal => Cmp::Equal,
        Comparator::NotEqual => Cmp::NotEqual,
        Comparator::Less => Cmp::Less,
        Comparator::Greater => Cmp::Greater,
        Comparator::LessOrEqual => Cmp::LessOrEqual,
        Comparator::GreaterOrEqual => Cmp::GreaterOrEqual,
        Comparator::Like => Cmp::Like,
        Comparator::Contains => Cmp::Contains,
    }
}
