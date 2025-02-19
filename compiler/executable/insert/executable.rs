/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeSet, HashMap, HashSet};

use answer::{variable::Variable, Type};
use encoding::graph::type_::Kind;
use ir::{
    pattern::{
        constraint,
        constraint::{Comparator, Constraint},
        expression::Expression,
        ParameterID, Vertex,
    },
    pipeline::VariableRegistry,
};
use itertools::Itertools;
use typeql::common::Span;

use crate::{
    annotation::type_annotations::TypeAnnotations,
    executable::{
        insert::{
            instructions::{ConceptInstruction, ConnectionInstruction, Has, Links, PutAttribute, PutObject},
            ThingPosition, TypeSource, ValueSource, VariableSource,
        },
        next_executable_id, WriteCompilationError,
    },
    filter_variants, VariablePosition,
};

#[derive(Debug)]
pub struct InsertExecutable {
    pub executable_id: u64,
    pub concept_instructions: Vec<ConceptInstruction>,
    pub connection_instructions: Vec<ConnectionInstruction>,
    pub output_row_schema: Vec<Option<(Variable, VariableSource)>>,
}

impl InsertExecutable {
    pub fn output_width(&self) -> usize {
        self.output_row_schema.len()
    }
}

/*
 * Assumptions:
 *   - Any labels have been assigned to a type variable and added as a type-annotation, though we should have a more explicit mechanism for this.
 *   - Validation has already been done - An input row will not violate schema on insertion.
 */

pub fn compile(
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
    variable_registry: &VariableRegistry,
    source_span: Option<Span>,
) -> Result<InsertExecutable, Box<WriteCompilationError>> {
    let mut concept_inserts = Vec::with_capacity(constraints.len());
    let variables = add_inserted_concepts(
        constraints,
        input_variables,
        type_annotations,
        variable_registry,
        &mut concept_inserts,
        source_span,
    )?;

    let mut connection_inserts = Vec::with_capacity(constraints.len());
    add_has(constraints, &variables, variable_registry, &mut connection_inserts)?;
    add_links(constraints, type_annotations, &variables, variable_registry, &mut connection_inserts)?;

    Ok(InsertExecutable {
        executable_id: next_executable_id(),
        concept_instructions: concept_inserts,
        connection_instructions: connection_inserts,
        output_row_schema: prepare_output_row_schema(&variables),
    })
}

pub(crate) fn add_inserted_concepts(
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
    variable_registry: &VariableRegistry,
    concept_instructions: &mut Vec<ConceptInstruction>,
    stage_source_span: Option<Span>,
) -> Result<HashMap<Variable, VariablePosition>, Box<WriteCompilationError>> {
    let first_inserted_variable_position =
        input_variables.values().map(|pos| pos.position + 1).max().unwrap_or(0) as usize;
    let mut output_variables = input_variables.clone();
    let type_bindings = collect_type_bindings(constraints, type_annotations)?;
    let value_bindings = collect_value_bindings(constraints)?;

    for isa in filter_variants!(Constraint::Isa: constraints) {
        let &Vertex::Variable(thing) = isa.thing() else { unreachable!() };

        if input_variables.contains_key(&thing) {
            return Err(Box::new(WriteCompilationError::InsertIsaStatementForInputVariable {
                variable: variable_registry
                    .variable_names()
                    .get(&thing)
                    .cloned()
                    .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string()),
                source_span: isa.source_span(),
            }));
        }

        let type_ = if let Some(type_) = type_bindings.get(isa.type_()) {
            TypeSource::Constant(*type_)
        } else {
            match isa.type_() {
                Vertex::Variable(var) if input_variables.contains_key(var) => {
                    TypeSource::InputVariable(input_variables[var])
                }
                _ => {
                    return Err(Box::new(WriteCompilationError::InsertVariableUnknownType {
                        variable: variable_registry
                            .variable_names()
                            .get(&thing)
                            .cloned()
                            .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string()),
                        source_span: isa.source_span(),
                    }))
                }
            }
        };

        // Requires variable annotations for this stage to be available.
        let types = type_annotations.vertex_annotations_of(isa.type_()).unwrap();
        debug_assert!(!types.is_empty());
        let kinds = get_kinds_from_types(types);

        if kinds.contains(&Kind::Role) {
            return Err(Box::new(WriteCompilationError::InsertIllegalRole {
                variable: variable_registry
                    .variable_names()
                    .get(&thing)
                    .cloned()
                    .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string()),
                source_span: isa.source_span(),
            }));
        }

        if kinds.contains(&Kind::Relation) || kinds.contains(&Kind::Entity) {
            if kinds.contains(&Kind::Attribute) {
                return Err(Box::new(WriteCompilationError::InsertVariableAmbiguousAttributeOrObject {
                    variable: variable_registry
                        .variable_names()
                        .get(&thing)
                        .cloned()
                        .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string()),
                    source_span: isa.source_span(),
                }));
            }
            if output_variables.contains_key(&thing) {
                return Err(Box::new(WriteCompilationError::MultipleInsertsForSameVariable {
                    variable: variable_registry.get_variable_name(thing).unwrap().clone(),
                }));
            }
            let write_to =
                VariablePosition::new((first_inserted_variable_position + concept_instructions.len()) as u32);
            output_variables.insert(thing, write_to);
            let instruction = ConceptInstruction::PutObject(PutObject { type_, write_to: ThingPosition(write_to) });
            concept_instructions.push(instruction);
        } else {
            debug_assert!(kinds.len() == 1 && kinds.contains(&Kind::Attribute));
            let value_variable = resolve_value_variable_for_inserted_attribute(
                constraints,
                thing,
                variable_registry,
                stage_source_span,
            )?;
            let value = if let Some(&constant) = value_bindings.get(&value_variable) {
                debug_assert!(!value_variable
                    .as_variable()
                    .is_some_and(|variable| input_variables.contains_key(&variable)));
                ValueSource::Parameter(constant)
            } else if let &Vertex::Variable(variable) = value_variable {
                if let Some(&position) = input_variables.get(&variable) {
                    ValueSource::Variable(position)
                } else {
                    return Err(Box::new(WriteCompilationError::MissingExpectedInput {
                        variable: variable_registry
                            .variable_names()
                            .get(&thing)
                            .cloned()
                            .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string()),
                        source_span: isa.source_span(),
                    }));
                }
            } else {
                return Err(Box::new(WriteCompilationError::MissingExpectedInput {
                    variable: variable_registry
                        .variable_names()
                        .get(&thing)
                        .cloned()
                        .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string()),
                    source_span: isa.source_span(),
                }));
            };
            if output_variables.contains_key(&thing) {
                return Err(Box::new(WriteCompilationError::MultipleInsertsForSameVariable {
                    variable: variable_registry.get_variable_name(thing).unwrap().clone(),
                }));
            }
            let write_to =
                VariablePosition::new((first_inserted_variable_position + concept_instructions.len()) as u32);
            output_variables.insert(thing, write_to);
            let instruction =
                ConceptInstruction::PutAttribute(PutAttribute { type_, value, write_to: ThingPosition(write_to) });
            concept_instructions.push(instruction);
        };
    }
    Ok(output_variables)
}

fn add_has(
    constraints: &[Constraint<Variable>],
    input_variables: &HashMap<Variable, VariablePosition>,
    variable_registry: &VariableRegistry,
    instructions: &mut Vec<ConnectionInstruction>,
) -> Result<(), Box<WriteCompilationError>> {
    filter_variants!(Constraint::Has: constraints).try_for_each(|has| {
        let owner = get_thing_input_position(
            input_variables,
            has.owner().as_variable().unwrap(),
            variable_registry,
            has.source_span(),
        )?;
        let attribute = get_thing_input_position(
            input_variables,
            has.attribute().as_variable().unwrap(),
            variable_registry,
            has.source_span(),
        )?;
        instructions.push(ConnectionInstruction::Has(Has { owner, attribute }));
        Ok(())
    })
}

fn add_links(
    constraints: &[Constraint<Variable>],
    type_annotations: &TypeAnnotations,
    input_variables: &HashMap<Variable, VariablePosition>,
    variable_registry: &VariableRegistry,
    instructions: &mut Vec<ConnectionInstruction>,
) -> Result<(), Box<WriteCompilationError>> {
    let named_role_types = collect_role_type_bindings(constraints, type_annotations, variable_registry)?;
    for links in filter_variants!(Constraint::Links: constraints) {
        let relation = get_thing_input_position(
            input_variables,
            links.relation().as_variable().unwrap(),
            variable_registry,
            links.source_span(),
        )?;
        let player = get_thing_input_position(
            input_variables,
            links.player().as_variable().unwrap(),
            variable_registry,
            links.source_span(),
        )?;
        let role = resolve_links_role(type_annotations, input_variables, variable_registry, &named_role_types, links)?;
        instructions.push(ConnectionInstruction::Links(Links { relation, player, role }));
    }
    Ok(())
}

pub(crate) fn get_thing_input_position(
    input_variables: &HashMap<Variable, VariablePosition>,
    variable: Variable,
    variable_registry: &VariableRegistry,
    source_span: Option<Span>,
) -> Result<ThingPosition, Box<WriteCompilationError>> {
    match input_variables.get(&variable) {
        Some(input) => Ok(ThingPosition(*input)),
        None => Err(Box::new(WriteCompilationError::MissingExpectedInput {
            variable: variable_registry
                .variable_names()
                .get(&variable)
                .cloned()
                .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string()),
            source_span,
        })),
    }
}

fn resolve_value_variable_for_inserted_attribute<'a>(
    constraints: &'a [Constraint<Variable>],
    variable: Variable,
    variable_registry: &VariableRegistry,
    stage_source_span: Option<Span>,
) -> Result<&'a Vertex<Variable>, Box<WriteCompilationError>> {
    // Find the comparison linking thing to value
    let (comparator, value_variable, source_span) = filter_variants!(Constraint::Comparison: constraints)
        .filter_map(|cmp| {
            if cmp.lhs() == &Vertex::Variable(variable) {
                Some((cmp.comparator(), cmp.rhs(), cmp.source_span()))
            } else if cmp.rhs() == &Vertex::Variable(variable) {
                Some((cmp.comparator(), cmp.lhs(), cmp.source_span()))
            } else {
                None
            }
        })
        .exactly_one()
        .map_err(|mut err| {
            debug_assert_eq!(err.next(), None);
            Box::new(WriteCompilationError::InsertAttributeMissingValue {
                variable: variable_registry
                    .variable_names()
                    .get(&variable)
                    .cloned()
                    .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string()),
                // fallback span
                source_span: stage_source_span,
            })
        })?;
    if comparator == Comparator::Equal {
        Ok(value_variable)
    } else {
        Err(Box::new(WriteCompilationError::InsertIllegalPredicate {
            variable: variable_registry
                .variable_names()
                .get(&variable)
                .cloned()
                .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string()),
            comparator,
            source_span,
        }))
    }
}

pub(crate) fn resolve_links_role(
    type_annotations: &TypeAnnotations,
    input_variables: &HashMap<Variable, VariablePosition>,
    variable_registry: &VariableRegistry,
    named_role_types: &HashMap<Variable, Type>,
    links: &constraint::Links<Variable>,
) -> Result<TypeSource, Box<WriteCompilationError>> {
    let &Vertex::Variable(role_variable) = links.role_type() else { unreachable!() };
    match (input_variables.get(&role_variable), named_role_types.get(&role_variable)) {
        (Some(&input), None) => Ok(TypeSource::InputVariable(input)),
        (None, Some(type_)) => Ok(TypeSource::Constant(*type_)),
        (None, None) => {
            // TODO: Do we want to support inserts with unspecified role-types?
            let annotations = type_annotations.vertex_annotations_of(&Vertex::Variable(role_variable)).unwrap();
            if annotations.len() == 1 {
                Ok(TypeSource::Constant(*annotations.iter().find(|_| true).unwrap()))
            } else {
                return Err(Box::new(WriteCompilationError::InsertLinksAmbiguousRoleType {
                    player_variable: variable_registry
                        .variable_names()
                        .get(&links.relation().as_variable().unwrap())
                        .cloned()
                        .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string()),
                    role_types: annotations.iter().join(", "),
                    source_span: links.source_span(),
                }));
            }
        }
        (Some(_), Some(_)) => unreachable!(),
    }
}

pub(crate) fn get_kinds_from_types(types: &BTreeSet<Type>) -> HashSet<Kind> {
    types.iter().map(Type::kind).collect()
}

pub(crate) fn prepare_output_row_schema(
    input_variables: &HashMap<Variable, VariablePosition>,
) -> Vec<Option<(Variable, VariableSource)>> {
    let output_width = input_variables.values().map(|i| i.position + 1).max().unwrap_or(0);
    let mut output_row_schema = vec![None; output_width as usize];
    input_variables.iter().map(|(v, i)| (i, v)).for_each(|(&i, &v)| {
        output_row_schema[i.position as usize] = Some((v, VariableSource::InputVariable(i)));
    });
    output_row_schema
}

fn collect_value_bindings(
    constraints: &[Constraint<Variable>],
) -> Result<HashMap<&Vertex<Variable>, ParameterID>, Box<WriteCompilationError>> {
    #[cfg(debug_assertions)]
    let mut seen = HashSet::new();

    filter_variants!(Constraint::ExpressionBinding : constraints)
        .map(|expr| {
            let &Expression::Constant(constant) = expr.expression().get_root() else {
                return Err(Box::new(WriteCompilationError::UnsupportedCompoundExpressions {
                    source_span: expr.source_span(),
                }));
            };
            #[cfg(debug_assertions)]
            {
                debug_assert!(!seen.contains(&expr.left()));
                seen.insert(expr.left());
            }

            Ok((expr.left(), constant))
        })
        .chain(
            constraints
                .iter()
                .flat_map(|con| con.vertices().filter(|v| v.is_parameter()))
                .map(|param| Ok((param, param.as_parameter().unwrap()))),
        )
        .collect()
}

fn collect_type_bindings(
    constraints: &[Constraint<Variable>],
    type_annotations: &TypeAnnotations,
) -> Result<HashMap<Vertex<Variable>, answer::Type>, Box<WriteCompilationError>> {
    #[cfg(debug_assertions)]
    let mut seen = HashSet::new();

    filter_variants!(Constraint::Label : constraints)
        .map(|label| {
            let annotations = type_annotations.vertex_annotations_of(label.type_()).unwrap();
            #[cfg(debug_assertions)]
            debug_assert!(annotations.len() == 1);
            let type_ = annotations.first().unwrap();

            #[cfg(debug_assertions)]
            {
                debug_assert!(!seen.contains(label.type_()));
                seen.insert(label.type_());
            }

            Ok((label.type_().clone(), *type_))
        })
        .chain(constraints.iter().flat_map(|con| con.vertices().filter(|v| v.is_label())).map(|label| {
            let type_ = type_annotations.vertex_annotations_of(label).unwrap().iter().exactly_one().unwrap();
            Ok((label.clone(), *type_))
        }))
        .collect()
}

pub(crate) fn collect_role_type_bindings(
    constraints: &[Constraint<Variable>],
    type_annotations: &TypeAnnotations,
    variable_registry: &VariableRegistry,
) -> Result<HashMap<Variable, answer::Type>, Box<WriteCompilationError>> {
    #[cfg(debug_assertions)]
    let mut seen = HashSet::new();

    filter_variants!(Constraint::RoleName : constraints)
        .map(|role_name| {
            let annotations = type_annotations.vertex_annotations_of(role_name.type_()).unwrap();
            let variable = role_name.type_().as_variable().unwrap();
            let type_ = if annotations.len() == 1 {
                annotations.iter().find(|_| true).unwrap()
            } else {
                return Err(Box::new(WriteCompilationError::AmbiguousRoleType {
                    variable: variable_registry
                        .variable_names()
                        .get(&variable)
                        .cloned()
                        .unwrap_or_else(|| VariableRegistry::UNNAMED_VARIABLE_DISPLAY_NAME.to_string()),
                    role_types: annotations.iter().join(", "),
                    source_span: role_name.source_span(),
                }));
            };

            #[cfg(debug_assertions)]
            {
                debug_assert!(!seen.contains(&variable));
                seen.insert(variable);
            }

            Ok((variable, *type_))
        })
        .collect()
}
