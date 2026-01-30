/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, HashMap};

use answer::{variable::Variable, Type};
use compiler::{
    annotation::{function::FunctionParameterAnnotation, type_inference::get_type_annotation_from_label},
    query_structure::{
        ConjunctionAnnotations, FunctionReturnStructure, FunctionStructure, PipelineStructure,
        PipelineStructureAnnotations, PipelineVariableAnnotation, PipelineVariableAnnotationAndModifier,
        QueryStructureConjunction, QueryStructureNestedPattern, QueryStructureStage, StructureSortVariable,
        StructureVariableId,
    },
};
use concept::{error::ConceptReadError, type_::type_manager::TypeManager};
use encoding::value::{label::Label, value::Value};
use ir::pattern::{
    constraint::{Comparator, Constraint, IsaKind, SubKind},
    ParameterID, Vertex,
};
use query::analyse::{
    AnalysedQuery, FetchStructureAnnotations, FetchStructureAnnotationsFields, FunctionStructureAnnotations,
};
use storage::snapshot::ReadableSnapshot;
use typedb_protocol::{
    analyze::res::{analyzed_query as analyze_proto, analyzed_query::pipeline as structure_proto},
    analyzed_conjunction as conjunction_proto,
    analyzed_conjunction::{constraint as structure_constraint, constraint_vertex},
};

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
        debug_assert!(matches!(param, ParameterID::Value { .. }));
        self.pipeline_structure.parameters.value(param).cloned()
    }

    pub fn get_parameter_iid(&self, param: &ParameterID) -> Option<&[u8]> {
        self.pipeline_structure.parameters.iid(param).map(|iid| iid.as_ref())
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
    analyzed_query: AnalysedQuery,
) -> Result<typedb_protocol::analyze::res::AnalyzedQuery, Box<ConceptReadError>> {
    let AnalysedQuery { structure, annotations, source } = analyzed_query;
    let query = encode_analyzed_pipeline(snapshot, type_manager, &structure.query, &annotations.query)?;
    let preamble = std::iter::zip(structure.preamble.iter(), annotations.preamble.iter())
        .map(|(structure, annotations)| encode_function(snapshot, type_manager, structure, annotations))
        .collect::<Result<_, _>>()?;
    let fetch = annotations
        .fetch
        .as_ref()
        .map(|fetch| {
            encode_fetch_fields(snapshot, type_manager, &fetch)
                .map(|object| analyze_proto::Fetch { node: Some(analyze_proto::fetch::Node::Object(object)) })
        })
        .transpose()?;
    Ok(typedb_protocol::analyze::res::AnalyzedQuery { source, query: Some(query), preamble, fetch })
}

fn encode_function(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    structure: &FunctionStructure,
    annotations: &FunctionStructureAnnotations,
) -> Result<analyze_proto::Function, Box<ConceptReadError>> {
    use analyze_proto::function as function_proto;
    let body = encode_analyzed_pipeline(snapshot, type_manager, &structure.body, &annotations.body)?;
    let arguments = structure.arguments.iter().map(|v| encode_structure_variable(*v)).collect();
    let return_operation_variant = match &structure.return_ {
        FunctionReturnStructure::Stream { variables } => {
            let variables = variables.iter().map(|v| encode_structure_variable(*v)).collect();
            function_proto::return_operation::ReturnOperation::Stream(
                function_proto::return_operation::ReturnOpStream { variables },
            )
        }
        FunctionReturnStructure::Single { variables, selector } => {
            let selector = selector.clone();
            let variables = variables.iter().map(|v| encode_structure_variable(*v)).collect();
            function_proto::return_operation::ReturnOperation::Single(
                function_proto::return_operation::ReturnOpSingle { selector, variables },
            )
        }
        FunctionReturnStructure::Check {} => {
            function_proto::return_operation::ReturnOperation::Check(function_proto::return_operation::ReturnOpCheck {})
        }
        FunctionReturnStructure::Reduce { reducers } => {
            let reducers = reducers
                .iter()
                .map(|reducer| {
                    let variables = reducer.arguments.iter().map(|v| encode_structure_variable(*v)).collect();
                    analyze_proto::Reducer { reducer: reducer.reducer.clone(), variables }
                })
                .collect();
            function_proto::return_operation::ReturnOperation::Reduce(
                function_proto::return_operation::ReturnOpReduce { reducers },
            )
        }
    };
    let return_operation = function_proto::ReturnOperation { return_operation: Some(return_operation_variant) };
    let arguments_annotations = annotations
        .signature
        .arguments
        .iter()
        .map(|arg| encode_function_parameter_annotations(snapshot, type_manager, arg))
        .collect::<Result<Vec<_>, Box<ConceptReadError>>>()?;
    let return_annotations = annotations
        .signature
        .returns
        .iter()
        .map(|arg| encode_function_parameter_annotations(snapshot, type_manager, arg))
        .collect::<Result<Vec<_>, Box<ConceptReadError>>>()?;
    Ok(analyze_proto::Function {
        body: Some(body),
        arguments,
        return_operation: Some(return_operation),
        arguments_annotations,
        return_annotations,
    })
}

pub(crate) fn encode_analyzed_pipeline_for_query(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    structure: &PipelineStructure,
) -> Result<analyze_proto::Pipeline, Box<ConceptReadError>> {
    let dummy_annotations = &vec![BTreeMap::new(); structure.parametrised_structure.conjunctions.len()];
    encode_analyzed_pipeline(snapshot, type_manager, structure, &dummy_annotations)
}

fn encode_analyzed_pipeline(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    structure: &PipelineStructure,
    annotations: &PipelineStructureAnnotations,
) -> Result<analyze_proto::Pipeline, Box<ConceptReadError>> {
    let conjunctions = std::iter::zip(structure.parametrised_structure.conjunctions.iter(), annotations.iter())
        .map(|(conj, annos)| encode_query_conjunction(snapshot, type_manager, structure, conj, annos))
        .collect::<Result<Vec<_>, _>>()?;
    let variable_info = structure
        .variable_names
        .iter()
        .map(|(var, name)| (var.as_u32(), structure_proto::VariableInfo { name: name.clone() }))
        .collect();
    let stages = structure.parametrised_structure.stages.iter().map(|stage| encode_query_stage(stage)).collect();
    let outputs =
        structure.parametrised_structure.output_variables.iter().map(|var| encode_structure_variable(*var)).collect();
    Ok(analyze_proto::Pipeline { conjunctions, variable_info, stages, outputs })
}

fn encode_query_stage(stage: &QueryStructureStage) -> structure_proto::PipelineStage {
    use structure_proto::{pipeline_stage, pipeline_stage::Stage};
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
            use pipeline_stage::{sort, sort::sort_variable::SortDirection};
            let sort_variables = variables
                .iter()
                .map(|v| {
                    let (direction, variable) = match v {
                        StructureSortVariable::Ascending { variable } => (SortDirection::Asc, variable),
                        StructureSortVariable::Descending { variable } => (SortDirection::Desc, variable),
                    };
                    sort::SortVariable {
                        variable: Some(encode_structure_variable(*variable)),
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
                    let reducer = Some(analyze_proto::Reducer { reducer: reducer_name, variables });
                    pipeline_stage::reduce::ReduceAssign { assigned, reducer }
                })
                .collect();
            Stage::Reduce(pipeline_stage::Reduce { reducers, groupby })
        }
    };
    structure_proto::PipelineStage { stage: Some(variant) }
}

fn encode_query_conjunction(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    pipeline_structure: &PipelineStructure,
    structure: &QueryStructureConjunction,
    annotations: &ConjunctionAnnotations,
) -> Result<typedb_protocol::AnalyzedConjunction, Box<ConceptReadError>> {
    let mut constraints = Vec::new();
    let role_names = structure
        .constraints
        .iter()
        .filter_map(|constraint| constraint.as_role_name())
        .map(|rolename| (rolename.type_().as_variable().unwrap(), rolename.name().to_owned()))
        .collect();
    let context = PipelineStructureContext { pipeline_structure, snapshot, type_manager, role_names };
    structure
        .constraints
        .iter()
        .try_for_each(|constraint| query_structure_constraint(&context, constraint, &mut constraints))?;
    structure.nested.iter().for_each(|nested| conjunction_structure_nested(nested, &mut constraints));
    let variable_annotations = encode_conjunction_annotations(snapshot, type_manager, annotations)?;
    Ok(typedb_protocol::AnalyzedConjunction { constraints, variable_annotations })
}

fn query_structure_constraint(
    context: &PipelineStructureContext<'_, impl ReadableSnapshot>,
    constraint: &Constraint<Variable>,
    constraints: &mut Vec<conjunction_proto::Constraint>,
) -> Result<(), Box<ConceptReadError>> {
    let span = constraint.source_span().map(|span| structure_constraint::ConstraintSpan {
        begin: span.begin_offset as u64,
        end: span.end_offset as u64,
    });
    match constraint {
        Constraint::Links(links) => {
            let relation = encode_structure_vertex_variable(links.relation())?;
            let player = encode_structure_vertex_variable(links.player())?;
            let role = encode_named_role_as_vertex(context, links.role_type())?;
            constraints.push(conjunction_proto::Constraint {
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
            constraints.push(conjunction_proto::Constraint {
                span,
                constraint: Some(structure_constraint::Constraint::Has(structure_constraint::Has {
                    owner: Some(encode_structure_vertex_variable(has.owner())?),
                    attribute: Some(encode_structure_vertex_variable(has.attribute())?),
                    exactness: encode_exactness(false) as i32,
                })),
            });
        }

        Constraint::Isa(isa) => {
            constraints.push(conjunction_proto::Constraint {
                span,
                constraint: Some(structure_constraint::Constraint::Isa(structure_constraint::Isa {
                    instance: Some(encode_structure_vertex_variable(isa.thing())?),
                    r#type: Some(encode_structure_vertex_label_or_variable(context, isa.type_())?),
                    exactness: encode_exactness(isa.isa_kind() == IsaKind::Exact) as i32,
                })),
            });
        }
        Constraint::Sub(sub) => {
            constraints.push(conjunction_proto::Constraint {
                span,
                constraint: Some(structure_constraint::Constraint::Sub(structure_constraint::Sub {
                    subtype: Some(encode_structure_vertex_label_or_variable(context, sub.subtype())?),
                    supertype: Some(encode_structure_vertex_label_or_variable(context, sub.supertype())?),
                    exactness: encode_exactness(sub.sub_kind() == SubKind::Exact) as i32,
                })),
            });
        }
        Constraint::Owns(owns) => {
            constraints.push(conjunction_proto::Constraint {
                span,
                constraint: Some(structure_constraint::Constraint::Owns(structure_constraint::Owns {
                    owner: Some(encode_structure_vertex_label_or_variable(context, owns.owner())?),
                    attribute: Some(encode_structure_vertex_label_or_variable(context, owns.attribute())?),
                    exactness: encode_exactness(false) as i32,
                })),
            });
        }
        Constraint::Relates(relates) => {
            constraints.push(conjunction_proto::Constraint {
                span,
                constraint: Some(structure_constraint::Constraint::Relates(structure_constraint::Relates {
                    relation: Some(encode_structure_vertex_label_or_variable(context, relates.relation())?),
                    role: Some(encode_named_role_as_vertex(context, relates.role_type())?),
                    exactness: encode_exactness(false) as i32,
                })),
            });
        }
        Constraint::Plays(plays) => {
            constraints.push(conjunction_proto::Constraint {
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
            constraints.push(conjunction_proto::Constraint {
                span: span_1,
                constraint: Some(structure_constraint::Constraint::Links(structure_constraint::Links {
                    relation: Some(encode_structure_vertex_variable(indexed.relation())?),
                    player: Some(encode_structure_vertex_variable(indexed.player_1())?),
                    role: Some(encode_named_role_as_vertex(context, indexed.role_type_1())?),
                    exactness: encode_exactness(false) as i32,
                })),
            });
            constraints.push(conjunction_proto::Constraint {
                span: span_2,
                constraint: Some(structure_constraint::Constraint::Links(structure_constraint::Links {
                    relation: Some(encode_structure_vertex_variable(indexed.relation())?),
                    player: Some(encode_structure_vertex_variable(indexed.player_2())?),
                    role: Some(encode_named_role_as_vertex(context, indexed.role_type_2())?),
                    exactness: encode_exactness(false) as i32,
                })),
            });
        }
        Constraint::ExpressionBinding(expr) => {
            let text = context
                .get_call_syntax(constraint)
                .map_or_else(|| format!("Expression#{}", constraints.len()), |text| text.clone());
            let assigned = Some(encode_structure_vertex_variable(expr.left())?);
            let arguments = expr
                .expression_ids()
                .map(|variable| encode_structure_vertex_variable(&Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            constraints.push(conjunction_proto::Constraint {
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
            constraints.push(conjunction_proto::Constraint {
                span,
                constraint: Some(structure_constraint::Constraint::FunctionCall(structure_constraint::FunctionCall {
                    name: text,
                    assigned,
                    arguments,
                })),
            });
        }
        Constraint::Is(is) => {
            constraints.push(conjunction_proto::Constraint {
                span,
                constraint: Some(structure_constraint::Constraint::Is(structure_constraint::Is {
                    lhs: Some(encode_structure_vertex_variable(is.lhs())?),
                    rhs: Some(encode_structure_vertex_variable(is.rhs())?),
                })),
            });
        }
        Constraint::Iid(iid) => {
            let iid_bytes = Vec::from(context.get_parameter_iid(iid.iid().as_parameter().as_ref().unwrap()).unwrap());
            constraints.push(conjunction_proto::Constraint {
                span,
                constraint: Some(structure_constraint::Constraint::Iid(structure_constraint::Iid {
                    concept: Some(encode_structure_vertex_variable(iid.var())?),
                    iid: iid_bytes,
                })),
            });
        }
        Constraint::Comparison(comparison) => constraints.push(conjunction_proto::Constraint {
            span,
            constraint: Some(structure_constraint::Constraint::Comparison(structure_constraint::Comparison {
                lhs: Some(encode_structure_vertex_value_or_variable(context, comparison.lhs())?),
                rhs: Some(encode_structure_vertex_value_or_variable(context, comparison.rhs())?),
                comparator: encode_comparator(comparison.comparator()) as i32,
            })),
        }),
        Constraint::Kind(kind) => constraints.push(conjunction_proto::Constraint {
            span,
            constraint: Some(structure_constraint::Constraint::Kind(structure_constraint::Kind {
                kind: encode_kind(kind.kind()) as i32,
                r#type: Some(encode_structure_vertex_label_or_variable(context, kind.type_())?),
            })),
        }),
        Constraint::Label(label) => constraints.push(conjunction_proto::Constraint {
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
        Constraint::Value(value) => constraints.push(conjunction_proto::Constraint {
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
    Ok(())
}

fn conjunction_structure_nested(
    nested: &QueryStructureNestedPattern,
    constraints: &mut Vec<conjunction_proto::Constraint>,
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
    constraints.push(conjunction_proto::Constraint { span: None, constraint: Some(constraint) });
}

fn encode_exactness(is_exact: bool) -> structure_constraint::ConstraintExactness {
    match is_exact {
        true => structure_constraint::ConstraintExactness::Exact,
        false => structure_constraint::ConstraintExactness::Subtypes,
    }
}

fn encode_structure_variable(id: StructureVariableId) -> conjunction_proto::Variable {
    conjunction_proto::Variable { id: id.as_u32() }
}

fn encode_named_role_as_vertex(
    context: &PipelineStructureContext<'_, impl ReadableSnapshot>,
    role_type: &Vertex<Variable>,
) -> Result<conjunction_proto::ConstraintVertex, Box<ConceptReadError>> {
    if let Some(name) = role_type.as_variable().and_then(|v| context.get_role_type(&v)) {
        let role_var = role_type.as_variable().unwrap();
        let named_role_vertex = constraint_vertex::NamedRole {
            name: name.to_owned(),
            variable: Some(encode_structure_variable(StructureVariableId::from(role_var))),
        };
        let vertex = constraint_vertex::Vertex::NamedRole(named_role_vertex);
        Ok(conjunction_proto::ConstraintVertex { vertex: Some(vertex) })
    } else {
        encode_structure_vertex_label_or_variable(context, role_type)
    }
}

fn encode_structure_vertex_variable(
    vertex: &Vertex<Variable>,
) -> Result<conjunction_proto::ConstraintVertex, Box<ConceptReadError>> {
    let variable = vertex.as_variable().expect("Expected variable");
    let encoded_variable = encode_structure_variable(variable.into());
    Ok(conjunction_proto::ConstraintVertex { vertex: Some(constraint_vertex::Vertex::Variable(encoded_variable)) })
}

fn encode_structure_vertex_label_or_variable(
    context: &PipelineStructureContext<'_, impl ReadableSnapshot>,
    vertex: &Vertex<Variable>,
) -> Result<conjunction_proto::ConstraintVertex, Box<ConceptReadError>> {
    match vertex {
        Vertex::Variable(_) => encode_structure_vertex_variable(vertex),
        Vertex::Label(label) => {
            let encoded_type = if let Some(type_) = context.get_type(label) {
                let resolved = encode_type(&type_, context.snapshot, context.type_manager)?;
                constraint_vertex::Vertex::Label(resolved)
            } else if let Some(type_) = get_type_annotation_from_label(context.snapshot, context.type_manager, label)? {
                let resolved = encode_type(&type_, context.snapshot, context.type_manager)?;
                constraint_vertex::Vertex::Label(resolved)
            } else {
                debug_assert!(false, "This should be unreachable, but we don't want crashes");
                let label = format!("ERROR_UNRESOLVED:{}", label.scoped_name.as_str());
                constraint_vertex::Vertex::Label(typedb_protocol::Type {
                    r#type: Some(typedb_protocol::r#type::Type::EntityType(typedb_protocol::EntityType { label })),
                })
            };
            Ok(conjunction_proto::ConstraintVertex { vertex: Some(encoded_type) })
        }
        Vertex::Parameter(_) => unreachable!("Expected variable or label"),
    }
}

fn encode_structure_vertex_value_or_variable(
    context: &PipelineStructureContext<'_, impl ReadableSnapshot>,
    vertex: &Vertex<Variable>,
) -> Result<conjunction_proto::ConstraintVertex, Box<ConceptReadError>> {
    match vertex {
        Vertex::Variable(_) => encode_structure_vertex_variable(vertex),
        Vertex::Parameter(parameter) => {
            let value = context.get_parameter_value(&parameter).expect("Expected values to be present");
            Ok(conjunction_proto::ConstraintVertex {
                vertex: Some(constraint_vertex::Vertex::Value(encode_value(value))),
            })
        }
        Vertex::Label(_) => unreachable!("Expected variable or value"),
    }
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
        ValueType::Struct(name) => Ok(typedb_protocol::ValueType {
            value_type: Some(typedb_protocol::value_type::ValueType::Struct(typedb_protocol::value_type::Struct {
                name,
            })),
        }),
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

fn encode_conjunction_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    conjunction_annotations: &BTreeMap<StructureVariableId, PipelineVariableAnnotationAndModifier>,
) -> Result<HashMap<u32, conjunction_proto::VariableAnnotations>, Box<ConceptReadError>> {
    let variable_annotations = conjunction_annotations
        .iter()
        .map(|(variable_id, annotation)| {
            let encoded = match &annotation.annotations {
                PipelineVariableAnnotation::Instance(types) => {
                    conjunction_proto::variable_annotations::Annotations::Instance(
                        encode_types_to_concept_variable_annotations(snapshot, type_manager, types.iter())?,
                    )
                }
                PipelineVariableAnnotation::Type(types) => conjunction_proto::variable_annotations::Annotations::Type(
                    encode_types_to_concept_variable_annotations(snapshot, type_manager, types.iter())?,
                ),
                PipelineVariableAnnotation::Value(value_type) => {
                    conjunction_proto::variable_annotations::Annotations::ValueAnnotations(encode_value_type(
                        value_type.clone(),
                        snapshot,
                        type_manager,
                    )?)
                }
            };
            let is_optional = annotation.is_optional;
            Ok((
                variable_id.as_u32(),
                conjunction_proto::VariableAnnotations { is_optional, annotations: Some(encoded) },
            ))
        })
        .collect::<Result<HashMap<_, _>, Box<ConceptReadError>>>()?;
    Ok(variable_annotations)
}

fn encode_fetch_fields(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    fetch_object: &FetchStructureAnnotationsFields,
) -> Result<analyze_proto::fetch::Object, Box<ConceptReadError>> {
    let fetch = fetch_object
        .iter()
        .map(|(key, value)| Ok((key.clone(), encode_fetch(snapshot, type_manager, value)?)))
        .collect::<Result<HashMap<_, _>, Box<ConceptReadError>>>()?;
    Ok(analyze_proto::fetch::Object { fetch })
}

fn encode_fetch(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    annotations: &FetchStructureAnnotations,
) -> Result<analyze_proto::Fetch, Box<ConceptReadError>> {
    use analyze_proto::fetch as fetch_proto;
    let node = match annotations {
        FetchStructureAnnotations::Leaf(value_types) => {
            let annotations = value_types
                .iter()
                .map(|v| encode_value_type(v.clone(), snapshot, type_manager))
                .collect::<Result<Vec<_>, Box<ConceptReadError>>>()?;
            let inner = fetch_proto::Leaf { annotations };
            fetch_proto::Node::Leaf(inner)
        }
        FetchStructureAnnotations::Object(object) => {
            let inner = encode_fetch_fields(snapshot, type_manager, object)?;
            fetch_proto::Node::Object(inner)
        }
        FetchStructureAnnotations::List(list) => {
            let inner = encode_fetch(snapshot, type_manager, list.as_ref())?;
            fetch_proto::Node::List(Box::new(inner))
        }
    };
    Ok(analyze_proto::Fetch { node: Some(node) })
}

fn encode_function_parameter_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    parameter: &FunctionParameterAnnotation,
) -> Result<conjunction_proto::VariableAnnotations, Box<ConceptReadError>> {
    let annotations = match parameter {
        FunctionParameterAnnotation::AnyConcept => {
            let annotations = encode_all_types_to_concept_variable_annotations(snapshot, type_manager)?;
            conjunction_proto::variable_annotations::Annotations::Instance(annotations)
        }
        FunctionParameterAnnotation::Concept(types) => {
            let annotations = encode_types_to_concept_variable_annotations(snapshot, type_manager, types.iter())?;
            conjunction_proto::variable_annotations::Annotations::Instance(annotations)
        }
        FunctionParameterAnnotation::Value(value) => {
            conjunction_proto::variable_annotations::Annotations::ValueAnnotations(encode_value_type(
                value.clone(),
                snapshot,
                type_manager,
            )?)
        }
    };
    // TODO: If we ever have optional parameters
    Ok(conjunction_proto::VariableAnnotations { is_optional: false, annotations: Some(annotations) })
}

fn encode_all_types_to_concept_variable_annotations(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<conjunction_proto::variable_annotations::ConceptVariableAnnotations, Box<ConceptReadError>> {
    let object_types = type_manager.get_object_types(snapshot)?;
    let attribute_types = type_manager.get_attribute_types(snapshot)?;
    let concept_types =
        Iterator::chain(object_types.into_iter().map(Type::from), attribute_types.into_iter().map(Type::from));
    concept_types
        .map(|type_| encode_type(&type_, snapshot, type_manager))
        .collect::<Result<Vec<_>, _>>()
        .map(|types| conjunction_proto::variable_annotations::ConceptVariableAnnotations { types })
}

fn encode_types_to_concept_variable_annotations<'a>(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    types: impl Iterator<Item = &'a answer::Type>,
) -> Result<conjunction_proto::variable_annotations::ConceptVariableAnnotations, Box<ConceptReadError>> {
    types
        .map(|type_| encode_type(type_, snapshot, type_manager))
        .collect::<Result<Vec<_>, _>>()
        .map(|types| conjunction_proto::variable_annotations::ConceptVariableAnnotations { types })
}

fn encode_type(
    type_: &answer::Type,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
) -> Result<typedb_protocol::Type, Box<ConceptReadError>> {
    use typedb_protocol::r#type::Type as TypeProto;
    let encoded = match type_ {
        Type::Entity(entity) => TypeProto::EntityType(encode_entity_type(entity, snapshot, type_manager)?),
        Type::Relation(relation) => TypeProto::RelationType(encode_relation_type(relation, snapshot, type_manager)?),
        Type::Attribute(attribute) => {
            TypeProto::AttributeType(encode_attribute_type(attribute, snapshot, type_manager)?)
        }
        Type::RoleType(role) => TypeProto::RoleType(encode_role_type(role, snapshot, type_manager)?),
    };
    Ok(typedb_protocol::Type { r#type: Some(encoded) })
}
