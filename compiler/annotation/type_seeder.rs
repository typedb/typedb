/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    iter::zip,
    sync::Arc,
};

use answer::{variable::Variable, Type as TypeAnnotation, Type};
use concept::{
    error::ConceptReadError,
    type_::{object_type::ObjectType, type_manager::TypeManager, OwnerAPI, PlayerAPI, TypeAPI},
};
use encoding::value::value_type::{ValueType, ValueTypeCategory};
use ir::{
    pattern::{
        conjunction::Conjunction,
        constraint::{
            Comparison, Constraint, FunctionCallBinding, Has, Is, Isa, IsaKind, Kind, Label, Links, Owns, Plays,
            Relates, RoleName, Sub, SubKind, Value
        },
        disjunction::Disjunction,
        nested_pattern::NestedPattern,
        variable_category::VariableCategory,
        Scope, ScopeId, Vertex,
    },
    pipeline::{block::BlockContext, function_signature::FunctionID, VariableRegistry},
};
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::annotation::{
    function::{
        AnnotatedFunction, AnnotatedFunctions, AnnotatedUnindexedFunctions, FunctionParameterAnnotation,
        IndexedAnnotatedFunctions,
    },
    match_inference::{NestedTypeInferenceGraphDisjunction, TypeInferenceEdge, TypeInferenceGraph, VertexAnnotations},
    TypeInferenceError,
};

pub struct TypeGraphSeedingContext<'this, Snapshot: ReadableSnapshot> {
    snapshot: &'this Snapshot,
    type_manager: &'this TypeManager,
    schema_functions: &'this IndexedAnnotatedFunctions,
    local_functions: Option<&'this AnnotatedUnindexedFunctions>,
    variable_registry: &'this VariableRegistry,
}

impl<'this, Snapshot: ReadableSnapshot> TypeGraphSeedingContext<'this, Snapshot> {
    pub(crate) fn new(
        snapshot: &'this Snapshot,
        type_manager: &'this TypeManager,
        schema_functions: &'this IndexedAnnotatedFunctions,
        local_functions: Option<&'this AnnotatedUnindexedFunctions>,
        variable_registry: &'this VariableRegistry,
    ) -> Self {
        TypeGraphSeedingContext { snapshot, type_manager, schema_functions, local_functions, variable_registry }
    }

    fn get_annotated_function(&self, function_id: FunctionID) -> Option<&AnnotatedFunction> {
        match function_id {
            FunctionID::Schema(definition_key) => {
                debug_assert!(self.schema_functions.get_function(definition_key.clone()).is_some());
                self.schema_functions.get_function(definition_key.clone())
            }
            FunctionID::Preamble(index) => {
                debug_assert!(
                    self.local_functions.is_none() || self.local_functions.unwrap().get_function(index).is_some()
                );
                self.local_functions?.get_function(index)
            }
        }
    }

    pub(crate) fn create_graph<'graph>(
        &self,
        context: &BlockContext,
        upstream_annotations: &BTreeMap<Variable, Arc<BTreeSet<TypeAnnotation>>>,
        conjunction: &'graph Conjunction,
    ) -> Result<TypeInferenceGraph<'graph>, TypeInferenceError> {
        let mut graph = self.build_recursive(context, conjunction);
        // Pre-seed with upstream variable annotations.
        for variable in context.referenced_variables() {
            if let Some(annotations) = upstream_annotations.get(&variable) {
                graph.vertices.add_or_intersect(&Vertex::Variable(variable), Cow::Borrowed(annotations));
            }
        }
        // Advanced TODO: Copying upstream binary constraints as schema constraints.
        self.seed_types(&mut graph, context, &VertexAnnotations::default())?;

        debug_assert!(conjunction
            .constraints()
            .iter()
            .flat_map(|constraint| constraint.vertices())
            .unique()
            .all(|vertex| graph.vertices.contains_key(vertex)));

        Ok(graph)
    }

    pub(crate) fn seed_types(
        &self,
        graph: &mut TypeInferenceGraph<'_>,
        context: &BlockContext,
        parent_vertices: &VertexAnnotations,
    ) -> Result<(), TypeInferenceError> {
        for (vertex, parent_annotations) in parent_vertices.iter() {
            graph.vertices.insert(vertex.clone(), parent_annotations.clone());
        }

        // Seed vertices in root & disjunctions
        self.seed_vertex_annotations_from_type_and_function_return(graph)?;

        let mut some_vertex_was_directly_annotated = true;
        while some_vertex_was_directly_annotated {
            let mut changed = true;
            while changed {
                changed = self
                    .propagate_vertex_annotations(graph)
                    .map_err(|source| TypeInferenceError::ConceptRead { source })?;
            }
            some_vertex_was_directly_annotated = self
                .annotate_some_unannotated_vertex(graph, context)
                .map_err(|source| TypeInferenceError::ConceptRead { source })?;
        }

        // Seed edges in root & disjunctions
        self.seed_edges(graph).map_err(|source| TypeInferenceError::ConceptRead { source })?;

        // Now we recurse into the nested negations & optionals
        let TypeInferenceGraph { vertices, nested_negations, nested_optionals, .. } = graph;
        for nested_graph in nested_negations {
            self.seed_types(nested_graph, context, vertices)?;
        }
        for nested_graph in nested_optionals {
            self.seed_types(nested_graph, context, vertices)?;
        }
        Ok(())
    }

    fn build_recursive<'conj>(
        &self,
        context: &BlockContext,
        conjunction: &'conj Conjunction,
    ) -> TypeInferenceGraph<'conj> {
        let mut nested_disjunctions = Vec::new();
        let mut nested_optionals = Vec::new();
        let mut nested_negations = Vec::new();
        for pattern in conjunction.nested_patterns() {
            match pattern {
                NestedPattern::Disjunction(disjunction) => {
                    nested_disjunctions.push(self.build_disjunction_recursive(context, conjunction, disjunction));
                }
                NestedPattern::Negation(negation) => {
                    nested_negations.push(self.build_recursive(context, negation.conjunction()));
                }
                NestedPattern::Optional(optional) => {
                    nested_optionals.push(self.build_recursive(context, optional.conjunction()));
                }
            }
        }

        TypeInferenceGraph {
            conjunction,
            vertices: VertexAnnotations::default(),
            edges: Vec::new(),
            nested_disjunctions,
            nested_negations,
            nested_optionals,
        }
    }

    fn build_disjunction_recursive<'conj>(
        &self,
        context: &BlockContext,
        parent_conjunction: &'conj Conjunction,
        disjunction: &'conj Disjunction,
    ) -> NestedTypeInferenceGraphDisjunction<'conj> {
        let nested_graphs =
            disjunction.conjunctions().iter().map(|conj| self.build_recursive(context, conj)).collect_vec();
        let shared_variables: BTreeSet<Variable> = nested_graphs
            .iter()
            .flat_map(|nested_graph| {
                Iterator::chain(
                    self.local_variables(context, nested_graph.conjunction.scope_id()),
                    nested_graph.nested_disjunctions.iter().flat_map(|disj| disj.shared_variables.iter().copied()),
                )
                .filter(|variable| context.is_variable_available(parent_conjunction.scope_id(), *variable))
            })
            .collect();
        NestedTypeInferenceGraphDisjunction {
            disjunction: nested_graphs,
            shared_variables,
            shared_vertex_annotations: VertexAnnotations::default(),
        }
    }

    // Phase 1: Collect all type & function return annotations
    fn seed_vertex_annotations_from_type_and_function_return(
        &self,
        graph: &mut TypeInferenceGraph<'_>,
    ) -> Result<(), TypeInferenceError> {
        self.annotate_fixed_vertices(graph)?;
        // Get vertex annotations from Type & Function returns
        let TypeInferenceGraph { vertices, .. } = graph;
        for constraint in graph.conjunction.constraints() {
            match constraint {
                Constraint::Kind(c) => c.apply(self, vertices)?,
                Constraint::Label(c) => c.apply(self, vertices)?,
                Constraint::FunctionCallBinding(c) => c.apply(self, vertices)?,
                Constraint::RoleName(c) => c.apply(self, vertices)?,
                | Constraint::Is(_)
                | Constraint::Sub(_)
                | Constraint::Isa(_)
                | Constraint::Links(_)
                | Constraint::Has(_)
                | Constraint::Owns(_)
                | Constraint::Relates(_)
                | Constraint::Plays(_)
                | Constraint::Value(_)
                | Constraint::ExpressionBinding(_)
                | Constraint::Comparison(_) => (),
            }
        }
        for nested_graph in graph.nested_disjunctions.iter_mut().flat_map(|nested| &mut nested.disjunction) {
            self.seed_vertex_annotations_from_type_and_function_return(nested_graph)?;
        }
        Ok(())
    }

    fn annotate_fixed_vertices(&self, graph: &mut TypeInferenceGraph<'_>) -> Result<(), TypeInferenceError> {
        for vertex in self.fixed_vertices(graph.conjunction.constraints()) {
            match vertex {
                Vertex::Variable(_) => unreachable!("variable in fixed vertices"),
                Vertex::Label(label) => {
                    if !graph.vertices.contains_key(vertex) {
                        let annotation_opt = get_type_annotation_from_label(self.snapshot, self.type_manager, label)
                            .map_err(|source| TypeInferenceError::ConceptRead { source })?;
                        if let Some(annotation) = annotation_opt {
                            graph.vertices.insert(vertex.clone(), BTreeSet::from([annotation]));
                        } else {
                            return Err(TypeInferenceError::LabelNotResolved { name: label.to_string() });
                        }
                    } else {
                        #[cfg(debug_assertions)]
                        {
                            let annotation_opt =
                                get_type_annotation_from_label(self.snapshot, self.type_manager, label)
                                    .map_err(|source| TypeInferenceError::ConceptRead { source })?;
                            debug_assert_ne!(annotation_opt, None);
                            debug_assert_eq!(graph.vertices[vertex], BTreeSet::from([annotation_opt.unwrap()]));
                        }
                    }
                }
                &Vertex::Parameter(_) => {
                    assert!(!graph.vertices.contains_key(vertex));
                }
            }
        }
        Ok(())
    }

    fn fixed_vertices<'conj>(
        &self,
        constraints: &'conj [Constraint<Variable>],
    ) -> impl Iterator<Item = &'conj Vertex<Variable>> {
        constraints.iter().flat_map(|con| con.vertices().filter(|v| !v.is_variable()))
    }

    fn local_variables<'a>(
        &'a self,
        context: &'a BlockContext,
        conjunction_scope_id: ScopeId,
    ) -> impl Iterator<Item = Variable> + '_ {
        context.get_variable_scopes().filter(move |&(_, scope)| scope == conjunction_scope_id).map(|(var, _)| var)
    }

    fn annotate_some_unannotated_vertex(
        &self,
        graph: &mut TypeInferenceGraph<'_>,
        context: &BlockContext,
    ) -> Result<bool, ConceptReadError> {
        let unannotated_var = self.local_variables(context, graph.conjunction.scope_id()).find(|&var| {
            let vertex = Vertex::Variable(var);
            !graph.vertices.contains_key(&vertex)
        });
        if let Some(var) = unannotated_var {
            let annotations = self.get_unbounded_type_annotations(
                self.variable_registry.get_variable_category(var).unwrap_or(VariableCategory::Type),
            )?;
            let vertex = Vertex::Variable(var);
            graph.vertices.insert(vertex, annotations);
            Ok(true)
        } else {
            let mut any = false;
            for disj in &mut graph.nested_disjunctions {
                for nested_graph in &mut disj.disjunction {
                    any |= self.annotate_some_unannotated_vertex(nested_graph, context)?;
                }
            }
            Ok(any)
        }
    }

    fn get_unbounded_type_annotations(
        &self,
        category: VariableCategory,
    ) -> Result<BTreeSet<TypeAnnotation>, ConceptReadError> {
        let (include_entities, include_relations, include_attributes, include_roles) = match category {
            VariableCategory::Type => (true, true, true, true),
            VariableCategory::ThingType => (true, true, true, false),
            VariableCategory::AttributeType => (false, false, true, false),
            VariableCategory::RoleType => (false, false, false, true),
            VariableCategory::ThingList | VariableCategory::Thing => (true, true, true, false),
            VariableCategory::ObjectList | VariableCategory::Object => (true, true, false, false),
            VariableCategory::AttributeList | VariableCategory::Attribute => (false, false, true, false),
            VariableCategory::ValueList | VariableCategory::Value => (false, false, true, false),
        };
        let mut annotations = BTreeSet::new();

        let snapshot = self.snapshot;
        let type_manager = self.type_manager;

        if include_entities {
            annotations.extend(type_manager.get_entity_types(snapshot)?.into_iter().map(TypeAnnotation::Entity));
        }
        if include_relations {
            annotations.extend(type_manager.get_relation_types(snapshot)?.into_iter().map(TypeAnnotation::Relation));
        }
        if include_attributes {
            annotations.extend(type_manager.get_attribute_types(snapshot)?.into_iter().map(TypeAnnotation::Attribute));
        }
        if include_roles {
            annotations.extend(type_manager.get_role_types(snapshot)?.into_iter().map(TypeAnnotation::RoleType));
        }
        Ok(annotations)
    }

    // Phase 2: Use constraints to infer annotations on other vertices
    fn propagate_vertex_annotations(&self, graph: &mut TypeInferenceGraph<'_>) -> Result<bool, ConceptReadError> {
        let mut is_modified = false;
        for c in graph.conjunction.constraints() {
            is_modified |= self.try_propagating_vertex_annotation(c, &mut graph.vertices)?;
        }

        // Propagate to & from nested disjunctions
        for nested in &mut graph.nested_disjunctions {
            is_modified |= self.reconcile_nested_disjunction(nested, &mut graph.vertices)?;
        }

        Ok(is_modified)
    }

    fn try_propagating_vertex_annotation(
        &self,
        constraint: &Constraint<Variable>,
        vertices: &mut VertexAnnotations,
    ) -> Result<bool, ConceptReadError> {
        let any_modified = match constraint {
            Constraint::Isa(isa) => self.try_propagating_vertex_annotation_impl(isa, vertices)?,
            Constraint::Sub(sub) => self.try_propagating_vertex_annotation_impl(sub, vertices)?,
            Constraint::Links(links) => {
                let relation_role = RelationRoleEdge { links };
                let player_role = PlayerRoleEdge { links };
                self.try_propagating_vertex_annotation_impl(&relation_role, vertices)?
                    || self.try_propagating_vertex_annotation_impl(&player_role, vertices)?
            }
            Constraint::Has(has) => self.try_propagating_vertex_annotation_impl(has, vertices)?,
            Constraint::Is(is) => self.try_propagating_vertex_annotation_impl(is, vertices)?,
            Constraint::Comparison(cmp) => self.try_propagating_vertex_annotation_impl(cmp, vertices)?,
            Constraint::Owns(owns) => self.try_propagating_vertex_annotation_impl(owns, vertices)?,
            Constraint::Relates(relates) => self.try_propagating_vertex_annotation_impl(relates, vertices)?,
            Constraint::Plays(plays) => self.try_propagating_vertex_annotation_impl(plays, vertices)?,
            Constraint::Value(value) => self.try_propagating_vertex_annotation_impl(value, vertices)?,
            | Constraint::ExpressionBinding(_)
            | Constraint::FunctionCallBinding(_)
            | Constraint::RoleName(_)
            | Constraint::Label(_)
            | Constraint::Kind(_) => false,
        };
        Ok(any_modified)
    }

    fn try_propagating_vertex_annotation_impl(
        &self,
        inner: &impl BinaryConstraint,
        vertices: &mut BTreeMap<Vertex<Variable>, BTreeSet<TypeAnnotation>>,
    ) -> Result<bool, ConceptReadError> {
        let (left, right) = (inner.left(), inner.right());
        let any_modified = match (vertices.get(left), vertices.get(right)) {
            (None, None) => false,
            (Some(_), Some(_)) => false,
            (Some(left_types), None) => {
                let left_to_right = inner.annotate_left_to_right(self, left_types)?;
                vertices.insert(right.clone(), left_to_right.into_values().flatten().collect());
                true
            }
            (None, Some(right_types)) => {
                let right_to_left = inner.annotate_right_to_left(self, right_types)?;
                vertices.insert(left.clone(), right_to_left.into_values().flatten().collect());
                true
            }
        };
        Ok(any_modified)
    }

    fn reconcile_nested_disjunction(
        &self,
        nested: &mut NestedTypeInferenceGraphDisjunction<'_>,
        parent_vertices: &mut VertexAnnotations,
    ) -> Result<bool, ConceptReadError> {
        let mut something_changed = false;
        // Apply annotations ot the parent on the nested
        for &variable in nested.shared_variables.iter() {
            let vertex = Vertex::Variable(variable);
            if let Some(parent_annotations) = parent_vertices.get_mut(&vertex) {
                for nested_graph in &mut nested.disjunction {
                    nested_graph.vertices.add_or_intersect(&vertex, Cow::Borrowed(parent_annotations));
                }
            }
        }

        // Propagate it within the child & recursively into nested
        for nested_graph in &mut nested.disjunction {
            something_changed |= self.propagate_vertex_annotations(nested_graph)?;
        }

        // Update shared variables of the disjunction
        let NestedTypeInferenceGraphDisjunction {
            shared_vertex_annotations,
            disjunction: nested_graph_disjunction,
            shared_variables,
        } = nested;
        for &variable in shared_variables.iter() {
            let vertex = Vertex::Variable(variable);
            #[allow(clippy::map_entry, reason = "false positive")]
            if !shared_vertex_annotations.contains_key(&vertex) {
                if let Some(types_from_branches) =
                    self.try_union_annotations_across_all_branches(nested_graph_disjunction, &vertex)
                {
                    shared_vertex_annotations.insert(vertex, types_from_branches);
                }
            }
        }

        // Update parent from the shared variables
        for (vertex, types) in shared_vertex_annotations.iter() {
            if !parent_vertices.contains_key(vertex) {
                parent_vertices.insert(vertex.clone(), types.clone());
                something_changed = true;
            }
        }
        Ok(something_changed)
    }

    fn try_union_annotations_across_all_branches(
        &self,
        disjunction: &[TypeInferenceGraph<'_>],
        vertex: &Vertex<Variable>,
    ) -> Option<BTreeSet<TypeAnnotation>> {
        if disjunction.iter().all(|nested_graph| nested_graph.vertices.contains_key(vertex)) {
            Some(
                disjunction
                    .iter()
                    .flat_map(|nested_graph| nested_graph.vertices.get(vertex).unwrap().iter().cloned())
                    .collect(),
            )
        } else {
            None
        }
    }

    // Phase 3: seed edges
    fn seed_edges(&self, graph: &mut TypeInferenceGraph<'_>) -> Result<(), ConceptReadError> {
        let TypeInferenceGraph { conjunction, edges, vertices, .. } = graph;
        for constraint in conjunction.constraints() {
            match constraint {
                Constraint::Isa(isa) => edges.push(self.seed_edge(constraint, isa, vertices)?),
                Constraint::Sub(sub) => edges.push(self.seed_edge(constraint, sub, vertices)?),
                Constraint::Links(links) => {
                    let relation_role = RelationRoleEdge { links };
                    let player_role = PlayerRoleEdge { links };
                    edges.push(self.seed_edge(constraint, &relation_role, vertices)?);
                    edges.push(self.seed_edge(constraint, &player_role, vertices)?);
                }
                Constraint::Has(has) => edges.push(self.seed_edge(constraint, has, vertices)?),
                Constraint::Is(is) => edges.push(self.seed_edge(constraint, is, vertices)?),
                Constraint::Comparison(cmp) => {
                    if vertices.contains_key(cmp.right()) && vertices.contains_key(cmp.left()) {
                        edges.push(self.seed_edge(constraint, cmp, vertices)?)
                    }
                }
                Constraint::Owns(owns) => edges.push(self.seed_edge(constraint, owns, vertices)?),
                Constraint::Relates(relates) => edges.push(self.seed_edge(constraint, relates, vertices)?),
                Constraint::Plays(plays) => edges.push(self.seed_edge(constraint, plays, vertices)?),
                | Constraint::RoleName(_)
                | Constraint::Label(_)
                | Constraint::Kind(_)
                | Constraint::ExpressionBinding(_)
                | Constraint::FunctionCallBinding(_) => (), // Do nothing
            }
        }
        for disj in &mut graph.nested_disjunctions {
            for nested_graph in &mut disj.disjunction {
                self.seed_edges(nested_graph)?;
            }
        }
        Ok(())
    }

    fn seed_edge<'conj>(
        &self,
        constraint: &'conj Constraint<Variable>,
        inner: &impl BinaryConstraint,
        vertices: &VertexAnnotations,
    ) -> Result<TypeInferenceEdge<'conj>, ConceptReadError> {
        let (left, right) = (inner.left().clone(), inner.right().clone());
        debug_assert!(vertices.contains_key(&left) && vertices.contains_key(&right));
        let left_to_right = inner.annotate_left_to_right(self, vertices.get(&left).unwrap())?;
        let right_to_left = inner.annotate_right_to_left(self, vertices.get(&right).unwrap())?;
        Ok(TypeInferenceEdge::build(constraint, left, right, left_to_right, right_to_left))
    }
}

trait UnaryConstraint {
    fn apply<Snapshot: ReadableSnapshot>(
        &self,
        seeder: &TypeGraphSeedingContext<'_, Snapshot>,
        graph_vertices: &mut VertexAnnotations,
    ) -> Result<(), TypeInferenceError>;
}

pub(crate) fn get_type_annotation_from_label<Snapshot: ReadableSnapshot>(
    snapshot: &Snapshot,
    type_manager: &TypeManager,
    label_value: &encoding::value::label::Label<'static>,
) -> Result<Option<TypeAnnotation>, ConceptReadError> {
    if let Some(t) = type_manager.get_attribute_type(snapshot, label_value)?.map(TypeAnnotation::Attribute) {
        Ok(Some(t))
    } else if let Some(t) = type_manager.get_entity_type(snapshot, label_value)?.map(TypeAnnotation::Entity) {
        Ok(Some(t))
    } else if let Some(t) = type_manager.get_relation_type(snapshot, label_value)?.map(TypeAnnotation::Relation) {
        Ok(Some(t))
    } else if let Some(t) = type_manager.get_role_type(snapshot, label_value)?.map(TypeAnnotation::RoleType) {
        Ok(Some(t))
    } else {
        Ok(None)
    }
}

pub(crate) fn get_type_annotation_and_subtypes_from_label<Snapshot: ReadableSnapshot>(
    snapshot: &Snapshot,
    type_manager: &TypeManager,
    label_value: &encoding::value::label::Label<'static>,
) -> Result<BTreeSet<TypeAnnotation>, TypeInferenceError> {
    let type_opt = get_type_annotation_from_label(snapshot, type_manager, label_value)
        .map_err(|source| TypeInferenceError::ConceptRead { source })?;
    let Some(type_) = type_opt else {
        return Err(TypeInferenceError::LabelNotResolved { name: label_value.scoped_name().to_string() });
    };
    let mut types: BTreeSet<Type> = match &type_ {
        TypeAnnotation::Entity(type_) => type_
            .get_subtypes_transitive(snapshot, type_manager)
            .map_err(|source| TypeInferenceError::ConceptRead { source })?
            .iter()
            .map(|t| TypeAnnotation::Entity(t.clone()))
            .collect(),
        TypeAnnotation::Relation(type_) => type_
            .get_subtypes_transitive(snapshot, type_manager)
            .map_err(|source| TypeInferenceError::ConceptRead { source })?
            .iter()
            .map(|t| TypeAnnotation::Relation(t.clone()))
            .collect(),
        TypeAnnotation::Attribute(type_) => type_
            .get_subtypes_transitive(snapshot, type_manager)
            .map_err(|source| TypeInferenceError::ConceptRead { source })?
            .iter()
            .map(|t| TypeAnnotation::Attribute(t.clone()))
            .collect(),
        TypeAnnotation::RoleType(type_) => type_
            .get_subtypes_transitive(snapshot, type_manager)
            .map_err(|source| TypeInferenceError::ConceptRead { source })?
            .iter()
            .map(|t| TypeAnnotation::RoleType(t.clone()))
            .collect(),
    };
    types.insert(type_);
    Ok(types)
}

impl UnaryConstraint for Kind<Variable> {
    fn apply<Snapshot: ReadableSnapshot>(
        &self,
        seeder: &TypeGraphSeedingContext<'_, Snapshot>,
        graph_vertices: &mut VertexAnnotations,
    ) -> Result<(), TypeInferenceError> {
        let type_manager = &seeder.type_manager;
        let annotations = match self.kind() {
            typeql::token::Kind::Entity => type_manager
                .get_entity_types(seeder.snapshot)
                .map_err(|source| TypeInferenceError::ConceptRead { source })?
                .iter()
                .map(|t| TypeAnnotation::Entity(t.clone()))
                .collect(),
            typeql::token::Kind::Relation => type_manager
                .get_relation_types(seeder.snapshot)
                .map_err(|source| TypeInferenceError::ConceptRead { source })?
                .iter()
                .map(|t| TypeAnnotation::Relation(t.clone()))
                .collect(),
            typeql::token::Kind::Attribute => type_manager
                .get_attribute_types(seeder.snapshot)
                .map_err(|source| TypeInferenceError::ConceptRead { source })?
                .iter()
                .map(|t| TypeAnnotation::Attribute(t.clone()))
                .collect(),
            typeql::token::Kind::Role => type_manager
                .get_role_types(seeder.snapshot)
                .map_err(|source| TypeInferenceError::ConceptRead { source })?
                .iter()
                .map(|t| TypeAnnotation::RoleType(t.clone()))
                .collect(),
        };
        graph_vertices.add_or_intersect(self.type_(), Cow::Owned(annotations));
        Ok(())
    }
}

impl UnaryConstraint for Label<Variable> {
    fn apply<Snapshot: ReadableSnapshot>(
        &self,
        _seeder: &TypeGraphSeedingContext<'_, Snapshot>,
        graph_vertices: &mut VertexAnnotations,
    ) -> Result<(), TypeInferenceError> {
        let annotation_opt = graph_vertices.get(self.type_label());
        if let Some(annotation) = annotation_opt {
            graph_vertices.add_or_intersect(self.type_(), Cow::Owned(annotation.clone()));
            Ok(())
        } else {
            Err(TypeInferenceError::LabelNotResolved { name: self.type_label().to_string() })
        }
    }
}

impl UnaryConstraint for RoleName<Variable> {
    fn apply<Snapshot: ReadableSnapshot>(
        &self,
        seeder: &TypeGraphSeedingContext<'_, Snapshot>,
        graph_vertices: &mut VertexAnnotations,
    ) -> Result<(), TypeInferenceError> {
        let role_types_opt = seeder
            .type_manager
            .get_roles_by_name(seeder.snapshot, self.name())
            .map_err(|source| TypeInferenceError::ConceptRead { source })?;
        if let Some(role_types) = role_types_opt {
            let mut annotations = BTreeSet::new();
            for role_type in &role_types {
                annotations.insert(TypeAnnotation::RoleType(role_type.clone()));
                let subtypes = role_type
                    .get_subtypes_transitive(seeder.snapshot, seeder.type_manager)
                    .map_err(|source| TypeInferenceError::ConceptRead { source })?;
                annotations.extend(subtypes.into_iter().map(|subtype| TypeAnnotation::RoleType(subtype.clone())));
            }
            graph_vertices.add_or_intersect(self.type_(), Cow::Owned(annotations));
            Ok(())
        } else {
            Err(TypeInferenceError::RoleNameNotResolved { name: self.name().to_string() })
        }
    }
}

impl UnaryConstraint for Value<Variable> {
    fn apply<Snapshot: ReadableSnapshot>(
        &self,
        seeder: &TypeGraphSeedingContext<'_, Snapshot>,
        graph_vertices: &mut VertexAnnotations,
    ) -> Result<(), TypeInferenceError> {
        let pattern_value_type = match self.value_type() {
            ir::pattern::ValueType::Builtin(value_type) => Ok(value_type.clone()),
            ir::pattern::ValueType::Struct(struct_name) => {
                let pattern_key = seeder.type_manager.get_struct_definition_key(seeder.snapshot, struct_name);
                match pattern_key {
                    Ok(Some(key)) => Ok(ValueType::Struct(key)),
                    Ok(None) => Err(TypeInferenceError::ValueTypeNotFound { name: struct_name.clone().to_owned() }),
                    Err(source) => Err(TypeInferenceError::ConceptRead { source }),
                }
            }
        }?;

        let mut annotations = BTreeSet::new();

        seeder
            .type_manager
            .get_attribute_types(seeder.snapshot)
            .map_err(|source| TypeInferenceError::ConceptRead { source })?
            .into_iter().try_for_each(|attribute_type| {
            let attribute_value_type_opt = attribute_type.get_value_type_without_source(seeder.snapshot, seeder.type_manager)
                .map_err(|source| TypeInferenceError::ConceptRead { source })?;
            if let Some(attribute_value_type) = attribute_value_type_opt {
                if pattern_value_type == attribute_value_type {
                    annotations.insert(TypeAnnotation::Attribute(attribute_type));
                }
            }
            Ok(())
        })?;

        graph_vertices.add_or_intersect(self.type_(), Cow::Owned(annotations));
        Ok(())
    }
}

impl UnaryConstraint for FunctionCallBinding<Variable> {
    fn apply<Snapshot: ReadableSnapshot>(
        &self,
        seeder: &TypeGraphSeedingContext<'_, Snapshot>,
        graph_vertices: &mut VertexAnnotations,
    ) -> Result<(), TypeInferenceError> {
        if let Some(annotated_function) = seeder.get_annotated_function(self.function_call().function_id()) {
            for (assigned_variable, return_annotation) in
                zip(self.assigned(), annotated_function.return_.annotations().iter())
            {
                if let FunctionParameterAnnotation::Concept(types) = return_annotation {
                    graph_vertices.add_or_intersect(assigned_variable, Cow::Borrowed(types));
                }
            }
        }
        Ok(())
    }
}

trait BinaryConstraint {
    fn left(&self) -> &Vertex<Variable>;
    fn right(&self) -> &Vertex<Variable>;

    fn annotate_left_to_right(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_types: &BTreeSet<TypeAnnotation>,
    ) -> Result<BTreeMap<TypeAnnotation, BTreeSet<TypeAnnotation>>, ConceptReadError> {
        let mut left_to_right = BTreeMap::new();
        for left_type in left_types {
            let mut right_annotations = BTreeSet::new();
            self.annotate_left_to_right_for_type(seeder, left_type, &mut right_annotations)?;
            left_to_right.insert(left_type.clone(), right_annotations);
        }
        Ok(left_to_right)
    }

    fn annotate_right_to_left(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_types: &BTreeSet<TypeAnnotation>,
    ) -> Result<BTreeMap<TypeAnnotation, BTreeSet<TypeAnnotation>>, ConceptReadError> {
        let mut right_to_left = BTreeMap::new();
        for right_type in right_types {
            let mut left_annotations = BTreeSet::new();
            self.annotate_right_to_left_for_type(seeder, right_type, &mut left_annotations)?;
            right_to_left.insert(right_type.clone(), left_annotations);
        }
        Ok(right_to_left)
    }

    fn annotate_left_to_right_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError>;

    fn annotate_right_to_left_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError>;
}

// Note: The schema and data constraints for Owns, Relates & Plays behave identically
impl BinaryConstraint for Has<Variable> {
    fn left(&self) -> &Vertex<Variable> {
        self.owner()
    }

    fn right(&self) -> &Vertex<Variable> {
        self.attribute()
    }

    fn annotate_left_to_right_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        let owner = match left_type {
            TypeAnnotation::Entity(entity) => ObjectType::Entity(entity.clone()),
            TypeAnnotation::Relation(relation) => ObjectType::Relation(relation.clone()),
            _ => return Ok(()), // It can't be another type => Do nothing and let type-inference clean it up
        };
        collector.extend(
            (owner.get_owns(seeder.snapshot, seeder.type_manager)?.iter())
                .map(|owns| TypeAnnotation::Attribute(owns.attribute())),
        );
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        let attribute = match right_type {
            TypeAnnotation::Attribute(attribute) => attribute,
            _ => return Ok(()), // It can't be another type => Do nothing and let type-inference clean it up
        };
        collector.extend(
            (attribute.get_owner_types(seeder.snapshot, seeder.type_manager)?.iter())
                .map(|(owner, _)| TypeAnnotation::from(owner.clone())),
        );
        Ok(())
    }
}

impl BinaryConstraint for Owns<Variable> {
    fn left(&self) -> &Vertex<Variable> {
        self.owner()
    }

    fn right(&self) -> &Vertex<Variable> {
        self.attribute()
    }

    fn annotate_left_to_right_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        let owner = match left_type {
            TypeAnnotation::Entity(entity) => ObjectType::Entity(entity.clone()),
            TypeAnnotation::Relation(relation) => ObjectType::Relation(relation.clone()),
            _ => {
                return Ok(());
            } // It can't be another type => Do nothing and let type-inference clean it up
        };
        owner
            .get_owns(seeder.snapshot, seeder.type_manager)?
            .iter()
            .map(|owns| TypeAnnotation::Attribute(owns.attribute()))
            .for_each(|type_| {
                collector.insert(type_);
            });
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        let attribute = match right_type {
            TypeAnnotation::Attribute(attribute) => attribute,
            _ => {
                return Ok(());
            } // It can't be another type => Do nothing and let type-inference clean it up
        };
        attribute
            .get_owner_types(seeder.snapshot, seeder.type_manager)?
            .iter()
            .map(|(owner, _)| match owner {
                ObjectType::Entity(entity) => TypeAnnotation::Entity(entity.clone()),
                ObjectType::Relation(relation) => TypeAnnotation::Relation(relation.clone()),
            })
            .for_each(|type_| {
                collector.insert(type_);
            });
        Ok(())
    }
}

impl BinaryConstraint for Isa<Variable> {
    fn left(&self) -> &Vertex<Variable> {
        self.thing()
    }

    fn right(&self) -> &Vertex<Variable> {
        self.type_()
    }

    fn annotate_left_to_right_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        if self.isa_kind() == IsaKind::Subtype {
            match left_type {
                TypeAnnotation::Attribute(attribute) => {
                    attribute
                        .get_supertypes_transitive(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::Attribute(subtype.clone().into_owned()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
                TypeAnnotation::Entity(entity) => {
                    entity
                        .get_supertypes_transitive(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::Entity(subtype.clone().into_owned()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
                TypeAnnotation::Relation(relation) => {
                    relation
                        .get_supertypes_transitive(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::Relation(subtype.clone().into_owned()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
                TypeAnnotation::RoleType(_) => unreachable!("Cannot get instances of role types."),
            }
        }
        collector.insert(left_type.clone());
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        if self.isa_kind() == IsaKind::Subtype {
            match right_type {
                TypeAnnotation::Attribute(attribute) => {
                    attribute
                        .get_subtypes_transitive(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::Attribute(subtype.clone().into_owned()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
                TypeAnnotation::Entity(entity) => {
                    entity
                        .get_subtypes_transitive(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::Entity(subtype.clone().into_owned()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
                TypeAnnotation::Relation(relation) => {
                    relation
                        .get_subtypes_transitive(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::Relation(subtype.clone().into_owned()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
                TypeAnnotation::RoleType(_) => unreachable!("Cannot get instances of role types."),
            }
        }
        collector.insert(right_type.clone());
        Ok(())
    }
}

impl BinaryConstraint for Sub<Variable> {
    fn left(&self) -> &Vertex<Variable> {
        self.subtype()
    }

    fn right(&self) -> &Vertex<Variable> {
        self.supertype()
    }

    fn annotate_left_to_right_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        if self.sub_kind() == SubKind::Subtype {
            match left_type {
                TypeAnnotation::Attribute(attribute) => {
                    attribute
                        .get_supertypes_transitive(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::Attribute(subtype.clone()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
                TypeAnnotation::Entity(entity) => {
                    entity
                        .get_supertypes_transitive(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::Entity(subtype.clone()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
                TypeAnnotation::Relation(relation) => {
                    relation
                        .get_supertypes_transitive(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::Relation(subtype.clone()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
                TypeAnnotation::RoleType(role_type) => {
                    role_type
                        .get_supertypes_transitive(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::RoleType(subtype.clone()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
            }
            collector.insert(left_type.clone());
        } else {
            match left_type {
                TypeAnnotation::Attribute(attribute) => {
                    if let Some(subtype) = attribute.get_supertype(seeder.snapshot, seeder.type_manager)? {
                        collector.insert(TypeAnnotation::Attribute(subtype));
                    }
                }
                TypeAnnotation::Entity(entity) => {
                    if let Some(subtype) = entity.get_supertype(seeder.snapshot, seeder.type_manager)? {
                        collector.insert(TypeAnnotation::Entity(subtype));
                    }
                }
                TypeAnnotation::Relation(relation) => {
                    if let Some(subtype) = relation.get_supertype(seeder.snapshot, seeder.type_manager)? {
                        collector.insert(TypeAnnotation::Relation(subtype));
                    }
                }
                TypeAnnotation::RoleType(role_type) => {
                    if let Some(subtype) = role_type.get_supertype(seeder.snapshot, seeder.type_manager)? {
                        collector.insert(TypeAnnotation::RoleType(subtype));
                    }
                }
            }
        }
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        if self.sub_kind() == SubKind::Subtype {
            match right_type {
                TypeAnnotation::Attribute(attribute) => {
                    attribute
                        .get_subtypes_transitive(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::Attribute(subtype.clone()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
                TypeAnnotation::Entity(entity) => {
                    entity
                        .get_subtypes_transitive(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::Entity(subtype.clone()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
                TypeAnnotation::Relation(relation) => {
                    relation
                        .get_subtypes_transitive(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::Relation(subtype.clone()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
                TypeAnnotation::RoleType(role_type) => {
                    role_type
                        .get_subtypes_transitive(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::RoleType(subtype.clone()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
            }
            collector.insert(right_type.clone());
        } else {
            match right_type {
                TypeAnnotation::Attribute(attribute) => {
                    attribute
                        .get_subtypes(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::Attribute(subtype.clone()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
                TypeAnnotation::Entity(entity) => {
                    entity
                        .get_subtypes(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::Entity(subtype.clone()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
                TypeAnnotation::Relation(relation) => {
                    relation
                        .get_subtypes(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::Relation(subtype.clone()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
                TypeAnnotation::RoleType(role_type) => {
                    role_type
                        .get_subtypes(seeder.snapshot, seeder.type_manager)?
                        .iter()
                        .map(|subtype| TypeAnnotation::RoleType(subtype.clone()))
                        .for_each(|subtype| {
                            collector.insert(subtype);
                        });
                }
            }
        }
        Ok(())
    }
}

impl BinaryConstraint for Is<Variable> {
    fn left(&self) -> &Vertex<Variable> {
        self.lhs()
    }

    fn right(&self) -> &Vertex<Variable> {
        self.rhs()
    }

    fn annotate_left_to_right_for_type(
        &self,
        _seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        collector.insert(left_type.clone());
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        _seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        collector.insert(right_type.clone());
        Ok(())
    }
}

// TODO: This is very inefficient. If needed, We can replace uses by a specialised implementation which pre-computes attributes by value-type.
impl BinaryConstraint for Comparison<Variable> {
    fn left(&self) -> &Vertex<Variable> {
        self.lhs()
    }

    fn right(&self) -> &Vertex<Variable> {
        self.rhs()
    }

    fn annotate_left_to_right_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        let left_value_type = match left_type {
            TypeAnnotation::Attribute(attribute) => {
                attribute.get_value_type_without_source(seeder.snapshot, seeder.type_manager)?
            }
            _ => return Ok(()), // It can't be another type => Do nothing and let type-inference clean it up
        };
        if let Some(value_type) = left_value_type {
            let comparable_types = ValueTypeCategory::comparable_categories(value_type.category());
            for subattr in seeder.type_manager.get_attribute_types(seeder.snapshot)?.iter() {
                if let Some(subvaluetype) =
                    subattr.get_value_type_without_source(seeder.snapshot, seeder.type_manager)?
                {
                    if comparable_types.contains(&subvaluetype.category()) {
                        collector.insert(TypeAnnotation::Attribute(subattr.clone()));
                    }
                }
            }
        }
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        let right_value_type = match right_type {
            TypeAnnotation::Attribute(attribute) => {
                attribute.get_value_type_without_source(seeder.snapshot, seeder.type_manager)?
            }
            _ => unreachable!("Expected attribute type"),
        };
        if let Some(value_type) = right_value_type {
            let comparable_types = ValueTypeCategory::comparable_categories(value_type.category());
            for subattr in seeder.type_manager.get_attribute_types(seeder.snapshot)?.iter() {
                if let Some(subvaluetype) =
                    subattr.get_value_type_without_source(seeder.snapshot, seeder.type_manager)?
                {
                    if comparable_types.contains(&subvaluetype.category()) {
                        collector.insert(TypeAnnotation::Attribute(subattr.clone()));
                    }
                }
            }
        }
        Ok(())
    }
}

struct PlayerRoleEdge<'graph> {
    links: &'graph Links<Variable>,
}

struct RelationRoleEdge<'graph> {
    links: &'graph Links<Variable>,
}

impl<'graph> BinaryConstraint for PlayerRoleEdge<'graph> {
    fn left(&self) -> &Vertex<Variable> {
        self.links.player()
    }

    fn right(&self) -> &Vertex<Variable> {
        self.links.role_type()
    }

    fn annotate_left_to_right_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        let player = match left_type {
            TypeAnnotation::Entity(entity) => ObjectType::Entity(entity.clone()),
            TypeAnnotation::Relation(relation) => ObjectType::Relation(relation.clone()),
            _ => {
                return Ok(());
            } // It can't be another type => Do nothing and let type-inference clean it up
        };
        player
            .get_plays(seeder.snapshot, seeder.type_manager)?
            .iter()
            .map(|plays| TypeAnnotation::RoleType(plays.role()))
            .for_each(|type_| {
                collector.insert(type_);
            });
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        let role_type = match right_type {
            TypeAnnotation::RoleType(role_type) => role_type,
            _ => {
                return Ok(());
            } // It can't be another type => Do nothing and let type-inference clean it up
        };
        role_type
            .get_player_types(seeder.snapshot, seeder.type_manager)?
            .keys()
            .map(|player| match player {
                ObjectType::Entity(entity) => TypeAnnotation::Entity(entity.clone()),
                ObjectType::Relation(relation) => TypeAnnotation::Relation(relation.clone()),
            })
            .for_each(|type_| {
                collector.insert(type_);
            });
        Ok(())
    }
}

impl BinaryConstraint for Plays<Variable> {
    fn left(&self) -> &Vertex<Variable> {
        self.player()
    }

    fn right(&self) -> &Vertex<Variable> {
        self.role_type()
    }

    fn annotate_left_to_right_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        let player = match left_type {
            TypeAnnotation::Entity(entity) => ObjectType::Entity(entity.clone()),
            TypeAnnotation::Relation(relation) => ObjectType::Relation(relation.clone()),
            _ => {
                return Ok(());
            } // It can't be another type => Do nothing and let type-inference clean it up
        };
        player
            .get_plays(seeder.snapshot, seeder.type_manager)?
            .iter()
            .map(|plays| TypeAnnotation::RoleType(plays.role()))
            .for_each(|type_| {
                collector.insert(type_);
            });
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        let role_type = match right_type {
            TypeAnnotation::RoleType(role_type) => role_type,
            _ => {
                return Ok(());
            } // It can't be another type => Do nothing and let type-inference clean it up
        };
        role_type
            .get_player_types(seeder.snapshot, seeder.type_manager)?
            .keys()
            .map(|player| match player {
                ObjectType::Entity(entity) => TypeAnnotation::Entity(entity.clone()),
                ObjectType::Relation(relation) => TypeAnnotation::Relation(relation.clone()),
            })
            .for_each(|type_| {
                collector.insert(type_);
            });
        Ok(())
    }
}

impl<'graph> BinaryConstraint for RelationRoleEdge<'graph> {
    fn left(&self) -> &Vertex<Variable> {
        self.links.relation()
    }

    fn right(&self) -> &Vertex<Variable> {
        self.links.role_type()
    }

    fn annotate_left_to_right_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        let relation = match left_type {
            TypeAnnotation::Relation(relation) => relation.clone(),
            _ => {
                return Ok(());
            } // It can't be another type => Do nothing and let type-inference clean it up
        };
        relation
            .get_relates(seeder.snapshot, seeder.type_manager)?
            .iter()
            .map(|relates| TypeAnnotation::RoleType(relates.role()))
            .for_each(|type_| {
                collector.insert(type_);
            });
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        let role_type = match right_type {
            TypeAnnotation::RoleType(role_type) => role_type,
            _ => {
                return Ok(());
            } // It can't be another type => Do nothing and let type-inference clean it up
        };
        role_type
            .get_relation_types(seeder.snapshot, seeder.type_manager)?
            .keys()
            .map(|relation_type| TypeAnnotation::Relation(relation_type.clone()))
            .for_each(|type_| {
                collector.insert(type_);
            });
        Ok(())
    }
}

impl BinaryConstraint for Relates<Variable> {
    fn left(&self) -> &Vertex<Variable> {
        self.relation()
    }

    fn right(&self) -> &Vertex<Variable> {
        self.role_type()
    }

    fn annotate_left_to_right_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        let relation = match left_type {
            TypeAnnotation::Relation(relation) => relation.clone(),
            _ => {
                return Ok(());
            } // It can't be another type => Do nothing and let type-inference clean it up
        };
        relation
            .get_relates(seeder.snapshot, seeder.type_manager)?
            .iter()
            .map(|relates| TypeAnnotation::RoleType(relates.role()))
            .for_each(|type_| {
                collector.insert(type_);
            });
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        seeder: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut BTreeSet<TypeAnnotation>,
    ) -> Result<(), ConceptReadError> {
        let role_type = match right_type {
            TypeAnnotation::RoleType(role_type) => role_type,
            _ => {
                return Ok(());
            } // It can't be another type => Do nothing and let type-inference clean it up
        };
        role_type
            .get_relation_types(seeder.snapshot, seeder.type_manager)?
            .keys()
            .map(|relation_type| TypeAnnotation::Relation(relation_type.clone()))
            .for_each(|type_| {
                collector.insert(type_);
            });
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::Arc,
    };

    use answer::Type as TypeAnnotation;
    use encoding::value::{label::Label, value_type::ValueType};
    use ir::{
        pattern::{
            constraint::{Comparator, IsaKind},
            Vertex,
        },
        pipeline::block::Block,
        translation::TranslationContext,
    };
    use storage::snapshot::CommittableSnapshot;

    use crate::annotation::{
        function::IndexedAnnotatedFunctions,
        match_inference::{TypeInferenceGraph, VertexAnnotations},
        tests::{
            managers,
            schema_consts::{setup_types, LABEL_CAT, LABEL_NAME},
            setup_storage,
        },
        type_inference::tests::expected_edge,
        type_seeder::TypeGraphSeedingContext,
    };

    #[test]
    fn test_has() {
        // dog sub animal, owns dog-name; cat sub animal owns cat-name;
        // cat-name sub animal-name; dog-name sub animal-name;

        // Some version of `$a isa animal, has name $n;`
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((_, type_cat, _), (type_name, type_catname, type_dogname), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);

        // Case 1: $a isa cat, has name $n;
        let mut translation_context = TranslationContext::new();
        let mut builder = Block::builder(translation_context.new_block_builder_context());
        let mut conjunction = builder.conjunction_mut();
        let var_animal = conjunction.get_or_declare_variable("animal").unwrap();
        let var_name = conjunction.get_or_declare_variable("name").unwrap();
        let var_animal_type = conjunction.get_or_declare_variable("animal_type").unwrap();
        let var_name_type = conjunction.get_or_declare_variable("name_type").unwrap();

        // Try seeding
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, var_animal_type.into()).unwrap();
        conjunction.constraints_mut().add_label(var_animal_type, LABEL_CAT.clone()).unwrap();
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into()).unwrap();
        conjunction.constraints_mut().add_label(var_name_type, LABEL_NAME.clone()).unwrap();
        conjunction.constraints_mut().add_has(var_animal, var_name).unwrap();

        let block = builder.finish().unwrap();
        let conjunction = block.conjunction();

        let constraints = conjunction.constraints();
        let expected_graph = TypeInferenceGraph {
            conjunction,
            vertices: VertexAnnotations::from([
                (var_animal.into(), BTreeSet::from([type_cat.clone()])),
                (var_name.into(), BTreeSet::from([type_name.clone(), type_catname.clone(), type_dogname.clone()])),
                (var_animal_type.into(), BTreeSet::from([type_cat.clone()])),
                (var_name_type.into(), BTreeSet::from([type_name.clone()])),
                (Vertex::Label(LABEL_CAT), BTreeSet::from([type_cat.clone()])),
                (Vertex::Label(LABEL_NAME), BTreeSet::from([type_name.clone()])),
            ]),
            edges: vec![
                expected_edge(
                    &constraints[0],
                    var_animal.into(),
                    var_animal_type.into(),
                    vec![(type_cat.clone(), type_cat.clone())],
                ),
                expected_edge(
                    &constraints[2],
                    var_name.into(),
                    var_name_type.into(),
                    vec![
                        (type_catname.clone(), type_name.clone()),
                        (type_dogname.clone(), type_name.clone()),
                        (type_name.clone(), type_name.clone()),
                    ],
                ),
                expected_edge(
                    &constraints[4],
                    var_animal.into(),
                    var_name.into(),
                    vec![(type_cat.clone(), type_name.clone()), (type_cat.clone(), type_catname.clone())],
                ),
            ],
            nested_disjunctions: vec![],
            nested_negations: vec![],
            nested_optionals: vec![],
        };

        let snapshot = storage.clone().open_snapshot_write();
        let empty_function_cache = IndexedAnnotatedFunctions::empty();
        let seeder = TypeGraphSeedingContext::new(
            &snapshot,
            &type_manager,
            &empty_function_cache,
            None,
            &translation_context.variable_registry,
        );
        let graph = seeder.create_graph(block.block_context(), &BTreeMap::new(), conjunction).unwrap();
        assert_eq!(expected_graph, graph);
    }

    #[test]
    fn test_no_constraints() {
        // dog sub animal, owns dog-name; cat sub animal owns cat-name;
        // cat-name sub animal-name; dog-name sub animal-name;
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let ((type_animal, type_cat, type_dog), (type_name, type_catname, type_dogname), (type_fears, _, _)) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);

        // Case 1: $a has $n;
        let mut translation_context = TranslationContext::new();
        let mut builder = Block::builder(translation_context.new_block_builder_context());
        let mut conjunction = builder.conjunction_mut();
        let var_animal = conjunction.get_or_declare_variable("animal").unwrap();
        let var_name = conjunction.get_or_declare_variable("name").unwrap();

        // Try seeding
        conjunction.constraints_mut().add_has(var_animal, var_name).unwrap();

        let block = builder.finish().unwrap();
        let conjunction = block.conjunction();

        let constraints = conjunction.constraints();
        let mut expected_graph = TypeInferenceGraph {
            conjunction,
            vertices: VertexAnnotations::from([
                (var_animal.into(), BTreeSet::from([type_cat.clone(), type_dog.clone(), type_animal.clone()])),
                (var_name.into(), BTreeSet::from([type_name.clone(), type_catname.clone(), type_dogname.clone()])),
            ]),
            edges: vec![expected_edge(
                &constraints[0],
                var_animal.into(),
                var_name.into(),
                vec![
                    (type_cat.clone(), type_catname.clone()),
                    (type_cat.clone(), type_name.clone()),
                    (type_dog.clone(), type_dogname.clone()),
                    (type_dog.clone(), type_name.clone()),
                    (type_animal.clone(), type_name.clone()),
                ],
            )],
            nested_disjunctions: vec![],
            nested_negations: vec![],
            nested_optionals: vec![],
        };

        let snapshot = storage.clone().open_snapshot_write();
        let empty_function_cache = IndexedAnnotatedFunctions::empty();
        let seeder = TypeGraphSeedingContext::new(
            &snapshot,
            &type_manager,
            &empty_function_cache,
            None,
            &translation_context.variable_registry,
        );
        let graph = seeder.create_graph(block.block_context(), &BTreeMap::new(), conjunction).unwrap();
        if expected_graph != graph {
            // We need this because of non-determinism
            expected_graph.vertices.get_mut(&var_animal.into()).unwrap().insert(type_fears.clone());
            assert_eq!(expected_graph, graph)
        }
    }

    #[test]
    fn test_comparison() {
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let (_, (type_name, type_catname, type_dogname), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);
        let type_age = {
            let mut snapshot = storage.clone().open_snapshot_write();
            let type_age = type_manager.create_attribute_type(&mut snapshot, &Label::build("age")).unwrap();
            type_age.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Long).unwrap();
            snapshot.commit().unwrap();
            TypeAnnotation::Attribute(type_age)
        };
        {
            // // Case 1: $a > $b;
            let mut translation_context = TranslationContext::new();
            let mut builder = Block::builder(translation_context.new_block_builder_context());
            let mut conjunction = builder.conjunction_mut();
            let var_a = conjunction.get_or_declare_variable("a").unwrap();
            let var_b = conjunction.get_or_declare_variable("b").unwrap();
            // Try seeding
            conjunction.constraints_mut().add_comparison(var_a.into(), var_b.into(), Comparator::Greater).unwrap();

            let block = builder.finish().unwrap();
            let conjunction = block.conjunction();

            let types_a =
                BTreeSet::from([type_age.clone(), type_name.clone(), type_catname.clone(), type_dogname.clone()]);
            let types_b =
                BTreeSet::from([type_age.clone(), type_name.clone(), type_catname.clone(), type_dogname.clone()]);
            let constraints = conjunction.constraints();
            let expected_graph = TypeInferenceGraph {
                conjunction,
                vertices: VertexAnnotations::from([(var_a.into(), types_a), (var_b.into(), types_b)]),
                edges: vec![expected_edge(
                    &constraints[0],
                    var_a.into(),
                    var_b.into(),
                    vec![
                        (type_age.clone(), type_age.clone()),
                        (type_catname.clone(), type_catname.clone()),
                        (type_catname.clone(), type_dogname.clone()),
                        (type_catname.clone(), type_name.clone()),
                        (type_dogname.clone(), type_catname.clone()),
                        (type_dogname.clone(), type_dogname.clone()),
                        (type_dogname.clone(), type_name.clone()),
                        (type_name.clone(), type_catname.clone()),
                        (type_name.clone(), type_dogname.clone()),
                        (type_name.clone(), type_name.clone()),
                    ],
                )],
                nested_disjunctions: vec![],
                nested_negations: vec![],
                nested_optionals: vec![],
            };

            let snapshot = storage.clone().open_snapshot_write();
            let empty_function_cache = IndexedAnnotatedFunctions::empty();
            let seeder = TypeGraphSeedingContext::new(
                &snapshot,
                &type_manager,
                &empty_function_cache,
                None,
                &translation_context.variable_registry,
            );
            let graph = seeder.create_graph(block.block_context(), &BTreeMap::new(), conjunction).unwrap();
            assert_eq!(expected_graph.vertices, graph.vertices);
            assert_eq!(expected_graph, graph);
        }
    }
}
