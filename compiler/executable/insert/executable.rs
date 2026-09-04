/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeSet, HashMap, HashSet};

use answer::{Type, variable::Variable};
use encoding::graph::type_::Kind;
use ir::{
    pattern::{
        ParameterID, Pattern, Vertex,
        conjunction::Conjunction,
        constraint::{Comparator, Constraint},
        expression::Expression,
        nested_pattern::NestedPattern,
    },
    pipeline::{VariableRegistry, block::Block},
};
use itertools::Itertools;
use typeql::common::Span;

use crate::{
    VariablePosition,
    annotation::type_annotations::{BlockAnnotations, TypeAnnotations},
    executable::{
        RequiredVariablesForWrite, WriteCompilationError,
        insert::{
            ThingPosition, TypeSource, ValueSource, VariableSource,
            instructions::{ConceptInstruction, ConnectionInstruction, Has, Links, PutAttribute, PutObject},
        },
        next_executable_id,
    },
    filter_variants,
};

#[derive(Debug)]
pub struct InsertExecutable {
    pub executable_id: u64,
    pub inserts: Vec<ConditionalInsert>,
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
    block: &Block,
    input_variable_positions: &HashMap<Variable, VariablePosition>,
    block_annotations: &BlockAnnotations,
    variable_registry: &VariableRegistry,
    desired_output_variable_positions: Option<HashMap<Variable, VariablePosition>>,
    source_span: Option<Span>,
) -> Result<InsertExecutable, Box<WriteCompilationError>> {
    debug_assert!(
        desired_output_variable_positions
            .as_ref()
            .map(|positions| { input_variable_positions.iter().all(|(k, v)| positions.get(k).unwrap() == v) })
            .unwrap_or(true)
    );
    let mut variable_positions = desired_output_variable_positions.unwrap_or_else(|| input_variable_positions.clone());
    let root_insert = ConditionalInsert::new(
        block.conjunction(),
        block_annotations,
        &mut variable_positions,
        variable_registry,
        source_span,
    )?;

    let mut inserts = Vec::with_capacity(1 + block.conjunction().nested_patterns().len());
    inserts.push(root_insert);
    for nested_pattern in block.conjunction().nested_patterns() {
        let NestedPattern::Optional(optional) = nested_pattern else {
            unreachable!("Only optionals are allowed as nested patterns in insert")
        };
        inserts.push(ConditionalInsert::new(
            optional.conjunction(),
            block_annotations,
            &mut variable_positions,
            variable_registry,
            source_span,
        )?);
    }

    Ok(InsertExecutable {
        executable_id: next_executable_id(),
        inserts,
        output_row_schema: prepare_output_row_schema(input_variable_positions, &variable_positions),
    })
}

#[derive(Debug)]
pub struct ConditionalInsert {
    pub concept_instructions: Vec<ConceptInstruction>,
    pub connection_instructions: Vec<ConnectionInstruction>,
    pub required_input_variables: RequiredVariablesForWrite,
}

impl ConditionalInsert {
    fn new(
        conjunction: &Conjunction,
        block_annotations: &BlockAnnotations,
        variable_positions: &mut HashMap<Variable, VariablePosition>,
        variable_registry: &VariableRegistry,
        stage_source_span: Option<Span>,
    ) -> Result<Self, Box<WriteCompilationError>> {
        let concept_instructions_map = add_inserted_concepts(
            conjunction,
            block_annotations,
            variable_registry,
            variable_positions,
            stage_source_span,
        )?;

        let connection_instructions =
            add_connections(conjunction, block_annotations, variable_positions, variable_registry)?;

        // We can't just use required_inputs because that's recursive and we only want those at this level.
        let required_input_variables = RequiredVariablesForWrite::build(conjunction, variable_positions);

        let concept_instructions = concept_instructions_map_to_vec(concept_instructions_map);
        Ok(Self { concept_instructions, connection_instructions, required_input_variables })
    }
}

pub(crate) fn add_inserted_concepts(
    conjunction: &Conjunction,
    block_annotations: &BlockAnnotations,
    variable_registry: &VariableRegistry,
    variable_positions: &mut HashMap<Variable, VariablePosition>,
    stage_source_span: Option<Span>,
) -> Result<HashMap<Variable, ConceptInstruction>, Box<WriteCompilationError>> {
    let constraints = conjunction.constraints();
    let type_annotations =
        block_annotations.type_annotations_of(conjunction).expect("insert conjunction must have type annotations");

    let mut next_insert_position = variable_positions.values().map(|pos| pos.position + 1).max().unwrap_or(0) as usize;
    let type_bindings = collect_type_bindings(constraints, type_annotations)?;
    let value_bindings = collect_value_bindings(constraints)?;
    let mut concept_instructions = HashMap::<Variable, ConceptInstruction>::new();
    for isa in filter_variants!(Constraint::Isa: constraints) {
        let &Vertex::Variable(thing) = isa.thing() else { unreachable!() };
        if conjunction.is_input(&thing) {
            return Err(Box::new(WriteCompilationError::InsertIsaStatementForInputVariable {
                variable: variable_registry.get_variable_name_or_unnamed(thing).to_owned(),
                source_span: isa.source_span(),
            }));
        }

        let type_ = if let Some(type_) = type_bindings.get(isa.type_()) {
            TypeSource::Constant(*type_)
        } else {
            match isa.type_() {
                Vertex::Variable(var) if variable_positions.contains_key(var) => {
                    TypeSource::Variable(variable_positions[var])
                }
                _ => {
                    return Err(Box::new(WriteCompilationError::InsertVariableUnknownType {
                        variable: variable_registry.get_variable_name_or_unnamed(thing).to_owned(),
                        source_span: isa.source_span(),
                    }));
                }
            }
        };

        // Requires variable annotations for this stage to be available.
        let types = type_annotations.vertex_annotations_of(isa.type_()).unwrap();
        debug_assert!(!types.is_empty());
        let kinds = get_kinds_from_types(types);

        if kinds.contains(&Kind::Role) {
            return Err(Box::new(WriteCompilationError::InsertIllegalRole {
                variable: variable_registry.get_variable_name_or_unnamed(thing).to_owned(),
                source_span: isa.source_span(),
            }));
        }

        if kinds.contains(&Kind::Relation) || kinds.contains(&Kind::Entity) {
            if kinds.contains(&Kind::Attribute) {
                return Err(Box::new(WriteCompilationError::InsertVariableAmbiguousAttributeOrObject {
                    variable: variable_registry.get_variable_name_or_unnamed(thing).to_owned(),
                    source_span: isa.source_span(),
                }));
            }
            if let Some(exisiting) = concept_instructions.get(&thing) {
                if exisiting.inserted_type() != &type_ {
                    return Err(Box::new(WriteCompilationError::ConflcitingTypesForInsertOfSameVariable {
                        variable: variable_registry.get_variable_name_or_unnamed(thing).to_owned(),
                        first: exisiting.inserted_type().clone(),
                        second: type_,
                    }));
                } // else let the original be
            } else {
                if !variable_positions.contains_key(&thing) {
                    variable_positions.insert(thing, VariablePosition::new(next_insert_position as u32));
                    next_insert_position += 1;
                };
                let write_to = ThingPosition(*variable_positions.get(&thing).unwrap());
                let instruction = ConceptInstruction::PutObject(PutObject { type_, write_to });
                concept_instructions.insert(thing, instruction);
            }
        } else {
            debug_assert!(kinds.len() == 1 && kinds.contains(&Kind::Attribute));
            let value_variable = resolve_value_variable_for_inserted_attribute(
                constraints,
                thing,
                variable_registry,
                stage_source_span,
            )?;
            let value = if let Some(constant) = value_bindings.get(&value_variable) {
                debug_assert!(
                    !value_variable.as_variable().is_some_and(|variable| variable_positions.contains_key(&variable))
                );
                ValueSource::Parameter(constant.clone())
            } else if let &Vertex::Variable(variable) = value_variable {
                if let Some(&position) = variable_positions.get(&variable) {
                    ValueSource::Variable(position)
                } else {
                    return Err(Box::new(WriteCompilationError::MissingExpectedInput {
                        variable: variable_registry.get_variable_name_or_unnamed(thing).to_owned(),
                        source_span: isa.source_span(),
                    }));
                }
            } else {
                return Err(Box::new(WriteCompilationError::MissingExpectedInput {
                    variable: variable_registry.get_variable_name_or_unnamed(thing).to_owned(),
                    source_span: isa.source_span(),
                }));
            };
            if let Some(exisiting) = concept_instructions.get(&thing) {
                if exisiting.inserted_type() != &type_ {
                    return Err(Box::new(WriteCompilationError::ConflcitingTypesForInsertOfSameVariable {
                        variable: variable_registry.get_variable_name_or_unnamed(thing).to_owned(),
                        first: exisiting.inserted_type().clone(),
                        second: type_,
                    }));
                } // else let the original be
            } else {
                if !variable_positions.contains_key(&thing) {
                    variable_positions.insert(thing, VariablePosition::new(next_insert_position as u32));
                    next_insert_position += 1;
                };
                let write_to = ThingPosition(*variable_positions.get(&thing).unwrap());
                let instruction = ConceptInstruction::PutAttribute(PutAttribute { type_, value, write_to });
                concept_instructions.insert(thing, instruction);
            }
        };
    }
    Ok(concept_instructions)
}

fn add_connections(
    conjunction: &Conjunction,
    block_annotations: &BlockAnnotations,
    variable_positions: &HashMap<Variable, VariablePosition>,
    variable_registry: &VariableRegistry,
) -> Result<Vec<ConnectionInstruction>, Box<WriteCompilationError>> {
    let mut connection_instructions = Vec::with_capacity(conjunction.constraints().len());
    let type_annotations =
        block_annotations.type_annotations_of(conjunction).expect("insert conjunction must have type annotations");
    add_has(conjunction, variable_positions, variable_registry, &mut connection_instructions)?;
    add_links(conjunction, type_annotations, variable_positions, variable_registry, &mut connection_instructions)?;
    Ok(connection_instructions)
}

fn add_has(
    conjunction: &Conjunction,
    variable_positions: &HashMap<Variable, VariablePosition>,
    variable_registry: &VariableRegistry,
    instructions: &mut Vec<ConnectionInstruction>,
) -> Result<(), Box<WriteCompilationError>> {
    filter_variants!(Constraint::Has: conjunction.constraints()).try_for_each(|has| {
        let owner = get_thing_position(
            variable_positions,
            has.owner().as_variable().unwrap(),
            variable_registry,
            has.source_span(),
        )?;
        let attribute = get_thing_position(
            variable_positions,
            has.attribute().as_variable().unwrap(),
            variable_registry,
            has.source_span(),
        )?;
        instructions.push(ConnectionInstruction::Has(Has { owner, attribute }));
        Ok(())
    })
}

fn add_links(
    conjunction: &Conjunction,
    type_annotations: &TypeAnnotations,
    variable_positions: &HashMap<Variable, VariablePosition>, // Also contains ones inserted.
    variable_registry: &VariableRegistry,
    instructions: &mut Vec<ConnectionInstruction>,
) -> Result<(), Box<WriteCompilationError>> {
    let resolved_role_types =
        resolve_links_roles(conjunction, type_annotations, variable_positions, variable_registry)?;
    for links in filter_variants!(Constraint::Links: conjunction.constraints()) {
        let relation = get_thing_position(
            variable_positions,
            links.relation().as_variable().unwrap(),
            variable_registry,
            links.source_span(),
        )?;
        let player = get_thing_position(
            variable_positions,
            links.player().as_variable().unwrap(),
            variable_registry,
            links.source_span(),
        )?;
        let role = resolved_role_types.get(&links.role_type().as_variable().unwrap()).unwrap().clone();
        instructions.push(ConnectionInstruction::Links(Links { relation, player, role }));
    }
    Ok(())
}

pub(crate) fn get_thing_position(
    variable_positions: &HashMap<Variable, VariablePosition>,
    variable: Variable,
    variable_registry: &VariableRegistry,
    source_span: Option<Span>,
) -> Result<ThingPosition, Box<WriteCompilationError>> {
    match variable_positions.get(&variable) {
        Some(input) => Ok(ThingPosition(*input)),
        None => Err(Box::new(WriteCompilationError::MissingExpectedInput {
            variable: variable_registry.get_variable_name_or_unnamed(variable).to_owned(),
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
                variable: variable_registry.get_variable_name_or_unnamed(variable).to_owned(),
                // fallback span
                source_span: stage_source_span,
            })
        })?;
    if comparator == Comparator::Equal {
        Ok(value_variable)
    } else {
        Err(Box::new(WriteCompilationError::InsertIllegalPredicate {
            variable: variable_registry.get_variable_name_or_unnamed(variable).to_owned(),
            comparator,
            source_span,
        }))
    }
}

pub(crate) fn get_kinds_from_types(types: &BTreeSet<Type>) -> HashSet<Kind> {
    types.iter().map(Type::kind).collect()
}

pub(crate) fn prepare_output_row_schema(
    input_positions: &HashMap<Variable, VariablePosition>,
    output_positions: &HashMap<Variable, VariablePosition>,
) -> Vec<Option<(Variable, VariableSource)>> {
    let output_width = output_positions.values().map(|i| i.position + 1).max().unwrap_or(0);
    let mut output_row_schema = vec![None; output_width as usize];
    output_positions.iter().for_each(|(var, pos)| {
        let source =
            input_positions.get(var).map(|pos| VariableSource::Input(*pos)).unwrap_or(VariableSource::Inserted);
        output_row_schema[pos.position as usize] = Some((*var, source));
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
            let Expression::Constant(constant) = expr.expression().get_root() else {
                return Err(Box::new(WriteCompilationError::UnsupportedCompoundExpressions {
                    source_span: expr.source_span(),
                }));
            };
            #[cfg(debug_assertions)]
            {
                debug_assert!(!seen.contains(&expr.left()));
                seen.insert(expr.left());
            }

            Ok((expr.left(), constant.clone()))
        })
        .chain(
            constraints
                .iter()
                .flat_map(|con| con.vertices().filter(|v| v.is_parameter()))
                .map(|param| Ok((param, param.as_parameter().unwrap().clone()))),
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

pub(crate) fn resolve_links_roles(
    conjunction: &Conjunction,
    type_annotations: &TypeAnnotations,
    variable_positions: &HashMap<Variable, VariablePosition>,
    variable_registry: &VariableRegistry,
) -> Result<HashMap<Variable, TypeSource>, Box<WriteCompilationError>> {
    filter_variants!(Constraint::Links : conjunction.constraints())
        .map(|links| {
            let role_type_vertex = links.role_type();
            let role_type = role_type_vertex.as_variable().expect("links.role_type is always a variable");
            if conjunction.is_input(&role_type) {
                let input_position = variable_positions.get(&role_type).expect("inputs have positions");
                Ok((role_type, TypeSource::Variable(*input_position)))
            } else {
                let annotations = type_annotations.vertex_annotations_of(role_type_vertex).unwrap();
                if let Ok(type_) = annotations.iter().exactly_one() {
                    Ok((role_type, TypeSource::Constant(*type_)))
                } else {
                    let player_variable = variable_registry
                        .get_variable_name_or_unnamed(links.player().as_variable().unwrap())
                        .to_owned();
                    Err(Box::new(WriteCompilationError::InsertLinksAmbiguousRoleType {
                        player_variable,
                        role_types: annotations.iter().join(", "),
                        source_span: role_type_vertex.source_span(variable_registry),
                    }))
                }
            }
        })
        .collect()
}

pub(crate) fn concept_instructions_map_to_vec(
    as_map: HashMap<Variable, ConceptInstruction>,
) -> Vec<ConceptInstruction> {
    as_map.into_values().sorted_by_key(ConceptInstruction::inserted_position).collect()
}
