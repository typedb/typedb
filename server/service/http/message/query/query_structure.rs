/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, str::FromStr};

use answer::variable::Variable;
use bytes::util::HexBytesFormatter;
use compiler::{
    annotation::type_inference::get_type_annotation_from_label,
    query_structure::{
        FunctionReturnStructure, ParametrisedPipelineStructure, PipelineStructure, QueryStructure,
        QueryStructureConjunctionID, QueryStructureNestedPattern, QueryStructureStage, StructureVariableId,
    },
};
use concept::{error::ConceptReadError, type_::type_manager::TypeManager};
use encoding::value::{label::Label, value::Value};
use ir::pattern::{
    constraint::{Constraint, IsaKind, SubKind},
    ParameterID, Vertex,
};
use serde::{Deserialize, Serialize, Serializer};
use storage::snapshot::ReadableSnapshot;

use crate::service::http::message::query::concept::{
    encode_type_concept, encode_value, RoleTypeResponse, ValueResponse,
};

struct PipelineStructureContext<'a, Snapshot: ReadableSnapshot> {
    pipeline_structure: &'a PipelineStructure,
    snapshot: &'a Snapshot,
    type_manager: &'a TypeManager,
    role_names: HashMap<Variable, String>,
    variables: &'a mut HashMap<StructureVariableId, StructureVariableInfo>,
}

impl<'a, Snapshot: ReadableSnapshot> PipelineStructureContext<'a, Snapshot> {
    pub fn get_parameter_value(&self, param: &ParameterID) -> Option<Value<'static>> {
        debug_assert!(matches!(param, ParameterID::Value(_, _)));
        self.pipeline_structure.parameters.value(*param).cloned()
    }

    pub fn get_parameter_iid(&self, param: &ParameterID) -> Option<&[u8]> {
        self.pipeline_structure.parameters.iid(*param).map(|iid| iid.as_ref())
    }

    pub fn get_variable_name(&self, variable: &StructureVariableId) -> Option<String> {
        self.pipeline_structure.variable_names.get(&variable).cloned()
    }

    pub fn get_type(&self, label: &Label) -> Option<answer::Type> {
        self.pipeline_structure.parametrised_structure.resolved_labels.get(label).cloned()
    }

    fn get_call_syntax(&self, constraint: &Constraint<Variable>) -> Option<&String> {
        self.pipeline_structure.parametrised_structure.calls_syntax.get(constraint)
    }

    fn get_role_type(&self, variable: &Variable) -> Option<&str> {
        self.role_names.get(variable).map(|name| name.as_str())
    }

    fn record_variable(&mut self, variable: StructureVariableId) {
        if !self.variables.contains_key(&variable) {
            let info = StructureVariableInfo { name: self.get_variable_name(&variable) };
            self.variables.insert(variable, info);
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionStructureResponse {
    pub(super) body: PipelineStructureResponse,
    pub(super) arguments: Vec<StructureVariableId>,
    pub(super) returns: FunctionReturnStructure,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryStructureResponse {
    pub(super) query: PipelineStructureResponse,
    pub(super) preamble: Vec<FunctionStructureResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineStructureResponse {
    pub(super) conjunctions: Vec<Vec<StructureConstraintWithSpan>>,
    pub(super) pipeline: Vec<QueryStructureStage>,
    variables: HashMap<StructureVariableId, StructureVariableInfo>,
    outputs: Vec<StructureVariableId>,
}

// Kept for backwards compatibility
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PipelineStructureResponseForStudio {
    blocks: Vec<StructureBlockForStudio>,
    variables: HashMap<StructureVariableId, StructureVariableInfo>,
    outputs: Vec<StructureVariableId>,
}

impl From<PipelineStructureResponse> for PipelineStructureResponseForStudio {
    fn from(value: PipelineStructureResponse) -> Self {
        let PipelineStructureResponse { variables, outputs, conjunctions, .. } = value;
        let blocks = conjunctions.into_iter().map(|constraints| StructureBlockForStudio { constraints }).collect();
        PipelineStructureResponseForStudio { variables, outputs, blocks }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StructureBlockForStudio {
    constraints: Vec<StructureConstraintWithSpan>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StructureVariableInfo {
    name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "tag")]
pub(super) enum StructureConstraint {
    Isa {
        instance: StructureVertex,
        r#type: StructureVertex,
    },

    #[serde(rename = "isa!")]
    IsaExact {
        instance: StructureVertex,
        r#type: StructureVertex,
    },
    Has {
        owner: StructureVertex,
        attribute: StructureVertex,
    },
    Links {
        relation: StructureVertex,
        player: StructureVertex,
        role: StructureVertex,
    },

    Sub {
        subtype: StructureVertex,
        supertype: StructureVertex,
    },
    #[serde(rename = "sub!")]
    SubExact {
        subtype: StructureVertex,
        supertype: StructureVertex,
    },
    Owns {
        owner: StructureVertex,
        attribute: StructureVertex,
    },
    Relates {
        relation: StructureVertex,
        role: StructureVertex,
    },
    Plays {
        player: StructureVertex,
        role: StructureVertex,
    },

    FunctionCall {
        name: String,
        assigned: Vec<StructureVertex>,
        arguments: Vec<StructureVertex>,
    },
    Expression {
        text: String,
        assigned: Vec<StructureVertex>,
        arguments: Vec<StructureVertex>,
    },
    Is {
        lhs: StructureVertex,
        rhs: StructureVertex,
    },
    Iid {
        concept: StructureVertex,
        iid: String,
    },
    Comparison {
        lhs: StructureVertex,
        rhs: StructureVertex,
        comparator: String,
    },
    Kind {
        kind: String,
        r#type: StructureVertex,
    },
    Label {
        r#type: StructureVertex,
        label: String,
    },
    Value {
        #[serde(rename = "attributeType")]
        attribute_type: StructureVertex,
        #[serde(rename = "valueType")]
        value_type: String,
    },

    // Nested patterns are now constraints too
    Or {
        branches: Vec<QueryStructureConjunctionID>,
    },
    Not {
        conjunction: QueryStructureConjunctionID,
    },
    Try {
        conjunction: QueryStructureConjunctionID,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StructureConstraintSpan {
    begin: usize,
    end: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StructureConstraintWithSpan {
    text_span: Option<StructureConstraintSpan>,
    #[serde(flatten)]
    pub(super) constraint: StructureConstraint,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", tag = "tag")]
enum StructureVertex {
    Variable { id: StructureVariableId },
    Label { r#type: serde_json::Value },
    Value(ValueResponse),
    Unresolved { label: String },
}

pub(crate) fn encode_query_structure(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    query_structure: QueryStructure,
) -> Result<QueryStructureResponse, Box<ConceptReadError>> {
    let QueryStructure { preamble, query: pipeline } = query_structure;
    let pipeline = encode_pipeline_structure(snapshot, type_manager, &pipeline, true)?;
    let preamble = preamble
        .into_iter()
        .map(|function| {
            let pipeline = encode_pipeline_structure(snapshot, type_manager, &function.body, true)?;
            Ok::<_, Box<ConceptReadError>>(FunctionStructureResponse {
                body: pipeline,
                arguments: function.arguments,
                returns: function.return_,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryStructureResponse { query: pipeline, preamble })
}

pub(crate) fn encode_pipeline_structure(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    pipeline_structure: &PipelineStructure,
    include_nested_patterns: bool,
) -> Result<PipelineStructureResponse, Box<ConceptReadError>> {
    let mut variables = HashMap::new();
    let ParametrisedPipelineStructure { stages, conjunctions, .. } = &*pipeline_structure.parametrised_structure;
    let encoded_conjunctions = conjunctions
        .iter()
        .map(|conj| {
            encode_structure_conjunction(
                snapshot,
                type_manager,
                &pipeline_structure,
                &mut variables,
                conj.constraints.as_slice(),
                if include_nested_patterns { conj.nested.as_slice() } else { &[] },
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Ensure reduced variables are added to variables
    record_reducer_variables(snapshot, type_manager, pipeline_structure, &mut variables);
    let outputs = pipeline_structure.parametrised_structure.output_variables.clone();
    Ok(PipelineStructureResponse { conjunctions: encoded_conjunctions, outputs, variables, pipeline: stages.clone() })
}

fn record_reducer_variables(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    pipeline_structure: &PipelineStructure,
    variables: &mut HashMap<StructureVariableId, StructureVariableInfo>,
) {
    let mut context =
        PipelineStructureContext { pipeline_structure, snapshot, type_manager, role_names: HashMap::new(), variables };
    pipeline_structure
        .parametrised_structure
        .stages
        .iter()
        .filter_map(|stage| match stage {
            QueryStructureStage::Reduce { reducers, .. } => Some(reducers),
            _ => None,
        })
        .for_each(|reducers| {
            reducers.iter().for_each(|reducer| context.record_variable(reducer.assigned));
        });
}

fn encode_structure_conjunction(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    pipeline_structure: &PipelineStructure,
    variables: &mut HashMap<StructureVariableId, StructureVariableInfo>,
    conjunction: &[Constraint<Variable>],
    nested: &[QueryStructureNestedPattern],
) -> Result<Vec<StructureConstraintWithSpan>, Box<ConceptReadError>> {
    let mut constraints = Vec::new();
    let role_names = conjunction
        .iter()
        .filter_map(|constraint| constraint.as_role_name())
        .map(|rolename| (rolename.type_().as_variable().unwrap(), rolename.name().to_owned()))
        .collect();
    let mut context = PipelineStructureContext { pipeline_structure, snapshot, type_manager, role_names, variables };
    conjunction.iter().enumerate().try_for_each(|(index, constraint)| {
        encode_structure_constraint(&mut context, constraint, &mut constraints, index)
    })?;
    nested.iter().try_for_each(|nested| encode_structure_nested_pattern(nested, &mut constraints))?;
    Ok(constraints)
}

fn encode_structure_constraint(
    context: &mut PipelineStructureContext<'_, impl ReadableSnapshot>,
    constraint: &Constraint<Variable>,
    constraints: &mut Vec<StructureConstraintWithSpan>,
    index: usize,
) -> Result<(), Box<ConceptReadError>> {
    let span =
        constraint.source_span().map(|span| StructureConstraintSpan { begin: span.begin_offset, end: span.end_offset });
    match constraint {
        Constraint::Links(links) => {
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: StructureConstraint::Links {
                    relation: encode_structure_vertex(context, links.relation())?,
                    player: encode_structure_vertex(context, links.player())?,
                    role: encode_role_type_as_vertex(context, links.role_type())?,
                },
            });
        }
        Constraint::Has(has) => {
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: StructureConstraint::Has {
                    owner: encode_structure_vertex(context, has.owner())?,
                    attribute: encode_structure_vertex(context, has.attribute())?,
                },
            });
        }

        Constraint::Isa(isa) => {
            let instance = encode_structure_vertex(context, isa.thing())?;
            let r#type = encode_structure_vertex(context, isa.type_())?;
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: match isa.isa_kind() {
                    IsaKind::Exact => StructureConstraint::IsaExact { instance, r#type },
                    IsaKind::Subtype => StructureConstraint::Isa { instance, r#type },
                },
            })
        }
        Constraint::Sub(sub) => {
            let subtype = encode_structure_vertex(context, sub.subtype())?;
            let supertype = encode_structure_vertex(context, sub.supertype())?;
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: match sub.sub_kind() {
                    SubKind::Exact => StructureConstraint::SubExact { subtype, supertype },
                    SubKind::Subtype => StructureConstraint::Sub { subtype, supertype },
                },
            })
        }
        Constraint::Owns(owns) => constraints.push(StructureConstraintWithSpan {
            text_span: span,
            constraint: StructureConstraint::Owns {
                owner: encode_structure_vertex(context, owns.owner())?,
                attribute: encode_structure_vertex(context, owns.attribute())?,
            },
        }),
        Constraint::Relates(relates) => {
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: StructureConstraint::Relates {
                    relation: encode_structure_vertex(context, relates.relation())?,
                    role: encode_role_type_as_vertex(context, relates.role_type())?,
                },
            });
        }
        Constraint::Plays(plays) => {
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: StructureConstraint::Plays {
                    player: encode_structure_vertex(context, plays.player())?,
                    role: encode_structure_vertex(context, plays.role_type())?, // Doesn't have to be encode_role_type
                },
            });
        }
        Constraint::IndexedRelation(indexed) => {
            let span_1 = indexed
                .source_span_1()
                .map(|span| StructureConstraintSpan { begin: span.begin_offset, end: span.end_offset });
            let span_2 = indexed
                .source_span_2()
                .map(|span| StructureConstraintSpan { begin: span.begin_offset, end: span.end_offset });
            constraints.push(StructureConstraintWithSpan {
                text_span: span_1,
                constraint: StructureConstraint::Links {
                    relation: encode_structure_vertex(context, indexed.relation())?,
                    player: encode_structure_vertex(context, indexed.player_1())?,
                    role: encode_role_type_as_vertex(context, indexed.role_type_1())?,
                },
            });
            constraints.push(StructureConstraintWithSpan {
                text_span: span_2,
                constraint: StructureConstraint::Links {
                    relation: encode_structure_vertex(context, indexed.relation())?,
                    player: encode_structure_vertex(context, indexed.player_2())?,
                    role: encode_role_type_as_vertex(context, indexed.role_type_2())?,
                },
            });
        }
        Constraint::ExpressionBinding(expr) => {
            let text =
                context.get_call_syntax(constraint).map_or_else(|| format!("Expression#{index}"), |text| text.clone());
            let assigned = expr
                .ids_assigned()
                .map(|variable| encode_structure_vertex(context, &Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            let arguments = expr
                .expression_ids()
                .map(|variable| encode_structure_vertex(context, &Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: StructureConstraint::Expression { text, assigned, arguments },
            });
        }
        Constraint::FunctionCallBinding(function_call) => {
            let text =
                context.get_call_syntax(constraint).map_or_else(|| format!("Function#{index}"), |text| text.clone());
            let assigned = function_call
                .ids_assigned()
                .map(|variable| encode_structure_vertex(context, &Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            let arguments = function_call
                .function_call()
                .argument_ids()
                .map(|variable| encode_structure_vertex(context, &Vertex::Variable(variable)))
                .collect::<Result<Vec<_>, _>>()?;
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: StructureConstraint::FunctionCall { name: text, assigned, arguments },
            });
        }
        Constraint::Is(is) => {
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: StructureConstraint::Is {
                    lhs: encode_structure_vertex(context, is.lhs())?,
                    rhs: encode_structure_vertex(context, is.rhs())?,
                },
            });
        }
        Constraint::Iid(iid) => {
            let iid_bytes = context.get_parameter_iid(iid.iid().as_parameter().as_ref().unwrap()).unwrap();
            let iid_hex = HexBytesFormatter::borrowed(iid_bytes).format_iid();
            constraints.push(StructureConstraintWithSpan {
                text_span: span,
                constraint: StructureConstraint::Iid {
                    concept: encode_structure_vertex(context, iid.var())?,
                    iid: iid_hex,
                },
            });
        }
        Constraint::Comparison(comparison) => constraints.push(StructureConstraintWithSpan {
            text_span: span,
            constraint: StructureConstraint::Comparison {
                lhs: encode_structure_vertex(context, comparison.lhs())?,
                rhs: encode_structure_vertex(context, comparison.rhs())?,
                comparator: comparison.comparator().name().to_owned(),
            },
        }),
        Constraint::Kind(kind) => constraints.push(StructureConstraintWithSpan {
            text_span: span,
            constraint: StructureConstraint::Kind {
                kind: kind.kind().name().to_owned(),
                r#type: encode_structure_vertex(context, kind.type_())?,
            },
        }),
        Constraint::Label(label) => constraints.push(StructureConstraintWithSpan {
            text_span: span,
            constraint: StructureConstraint::Label {
                r#type: encode_structure_vertex(context, label.type_())?,
                label: label
                    .type_label()
                    .as_label()
                    .expect("Expected constant label in label constraint")
                    .scoped_name()
                    .as_str()
                    .to_owned(),
            },
        }),
        Constraint::Value(value) => constraints.push(StructureConstraintWithSpan {
            text_span: span,
            constraint: StructureConstraint::Value {
                attribute_type: encode_structure_vertex(context, value.attribute_type())?,
                value_type: value.value_type().to_string(),
            },
        }),
        // Constraints that probably don't need to be handled
        Constraint::RoleName(_) => {} // Handled separately via resolved_role_names
        // Optimisations don't represent the structure
        Constraint::LinksDeduplication(_) | Constraint::Unsatisfiable(_) => {}
    };
    Ok(())
}

fn encode_structure_nested_pattern(
    nested: &QueryStructureNestedPattern,
    constraints: &mut Vec<StructureConstraintWithSpan>,
) -> Result<(), Box<ConceptReadError>> {
    let constraint = match nested.clone() {
        QueryStructureNestedPattern::Or { branches } => StructureConstraint::Or { branches },
        QueryStructureNestedPattern::Not { conjunction } => StructureConstraint::Not { conjunction },
        QueryStructureNestedPattern::Try { conjunction } => StructureConstraint::Try { conjunction },
    };
    constraints.push(StructureConstraintWithSpan { constraint, text_span: None });
    Ok(())
}

fn encode_structure_vertex(
    context: &mut PipelineStructureContext<'_, impl ReadableSnapshot>,
    vertex: &Vertex<Variable>,
) -> Result<StructureVertex, Box<ConceptReadError>> {
    let vertex = match vertex {
        Vertex::Variable(variable) => {
            context.record_variable(variable.into());
            StructureVertex::Variable { id: variable.into() }
        }
        Vertex::Label(label) => {
            if let Some(type_) = context.get_type(label) {
                let r#type = encode_type_concept(&type_, context.snapshot, context.type_manager)?;
                StructureVertex::Label { r#type }
            } else if let Some(type_) = get_type_annotation_from_label(context.snapshot, context.type_manager, label)? {
                let r#type = encode_type_concept(&type_, context.snapshot, context.type_manager)?;
                StructureVertex::Label { r#type }
            } else {
                debug_assert!(false, "Likely unreachable, thanks to the rolename handling");
                let label = label.scoped_name.as_str().to_owned();
                StructureVertex::Unresolved { label }
            }
        }
        Vertex::Parameter(param) => {
            let value = context.get_parameter_value(param).unwrap();
            StructureVertex::Value(encode_value(value))
        }
    };
    Ok(vertex)
}

fn encode_role_type_as_vertex(
    context: &mut PipelineStructureContext<'_, impl ReadableSnapshot>,
    role_type: &Vertex<Variable>,
) -> Result<StructureVertex, Box<ConceptReadError>> {
    if let Some(label) = context.get_role_type(&role_type.as_variable().unwrap()) {
        // TODO: Make consistent with GRPC API by introducing StructureVertex::NamedRole
        // At present rolename could resolve to multiple types - Manually encode.
        Ok(StructureVertex::Label { r#type: serde_json::json!(RoleTypeResponse { label: label.to_owned() }) })
    } else {
        encode_structure_vertex(context, role_type)
    }
}

#[cfg(debug_assertions)]
pub mod bdd {
    use compiler::query_structure::{
        FunctionReturnStructure, QueryStructureConjunctionID, QueryStructureStage, StructureReduceAssign,
        StructureReducer, StructureSortVariable, StructureVariableId,
    };
    use itertools::Itertools;
    use serde_json::Value;

    use crate::service::http::message::query::{
        bdd::{
            functor_macros,
            functor_macros::{encode_functor_impl, impl_functor_for, impl_functor_for_impl, impl_functor_for_multi},
            FunctorContext, FunctorEncoded,
        },
        query_structure::{
            FunctionStructureResponse, PipelineStructureResponse, StructureConstraint, StructureConstraintWithSpan,
            StructureVertex,
        },
        AnalysedQueryResponse,
    };

    pub fn encode_query_structure_as_functor(analyzed: &AnalysedQueryResponse) -> (String, Vec<String>) {
        let AnalysedQueryResponse { structure, annotations, source: _ } = analyzed;
        let context = FunctorContext { structure: &structure.query, annotations: &annotations.query };
        let pipeline = &structure.query;
        let query = pipeline.encode_as_functor(&context);
        let preamble = structure
            .preamble
            .iter()
            .zip(annotations.preamble.iter())
            .map(|(func, annotations)| {
                let context = FunctorContext { structure: &func.body, annotations: &annotations.body };
                func.encode_as_functor(&context)
            })
            .collect();
        (query, preamble)
    }

    impl_functor_for!(struct StructureReduceAssign { assigned, reducer,  } named ReduceAssign);
    impl_functor_for!(struct StructureReducer { reducer, arguments, } named Reducer);

    impl_functor_for!(enum QueryStructureStage [
        Match { block, } |
        Insert { block, } |
        Delete { deleted_variables, block, } |
        Put { block, } |
        Update { block, } |
        Select { variables, } |
        Sort { variables, } |
        Offset { offset, } |
        Limit { limit, } |
        Require { variables, } |
        Distinct { } |
        Reduce { reducers, groupby, } | // TODO
    ]);

    impl_functor_for!(enum StructureConstraint [
        Isa { instance, r#type, } |
        IsaExact { instance, r#type, } |
        Has { owner, attribute, } |
        Links { relation, player, role, } |
        Sub { subtype, supertype, } |
        SubExact { subtype, supertype, } |
        Owns { owner, attribute, } |
        Relates { relation, role, } |
        Plays { player, role, } |
        FunctionCall { name, assigned, arguments, } |
        Expression { text, assigned, arguments, } |
        Is { lhs, rhs, } |
        Iid { concept, iid, } |
        Comparison { lhs, rhs, comparator, } |
        Kind { kind, r#type, } |
        Label { r#type, label, } |
        Value { attribute_type, value_type, } |
        Or { branches, } |
        Not { conjunction, } |
        Try { conjunction, } |
    ]);

    impl_functor_for_impl!(StructureVertex => |self, context| {
        match self {
            StructureVertex::Variable { id } => { id.encode_as_functor(context) }
            StructureVertex::Label { r#type } => { r#type.as_object().unwrap()["label"].as_str().unwrap().to_owned() }
            StructureVertex::Unresolved { label } => { label.encode_as_functor(context) }
            StructureVertex::Value(v) => {
                match &v.value {
                    Value::String(s) => std::format!("\"{}\"", s.to_string()),
                    other => other.to_string(),
                }
            }
        }
    });

    impl_functor_for_multi!(|self, context| [
        StructureVariableId =>  { format!("${}", context.structure.variables[self].name.as_ref().map(|s| s.as_str()).unwrap_or("_")) }
        QueryStructureConjunctionID => { context.structure.conjunctions[self.0 as usize].encode_as_functor(context) }
        StructureConstraintWithSpan => { self.constraint.encode_as_functor(context) }
        PipelineStructureResponse => { let pipeline = &self.pipeline; encode_functor_impl!(context, Pipeline { pipeline, }) }
        FunctionStructureResponse => {
            let FunctionStructureResponse { arguments, returns, body } = self;
            encode_functor_impl!(context, Function { arguments, returns, body, })
        }
        StructureSortVariable => {
            let Self { ascending, variable } = self;
            match ascending {
                true => encode_functor_impl!(context, Asc { variable, }),
                false => encode_functor_impl!(context, Desc { variable, }),
            }
        }
    ]);

    impl_functor_for!(enum FunctionReturnStructure [
        Stream { variables, } |
        Single { selector, variables, }  |
        Check { }  |
        Reduce {} |
    ]);
}
