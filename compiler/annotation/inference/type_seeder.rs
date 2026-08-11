/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Cow,
    cell::LazyCell,
    collections::{BTreeMap, BTreeSet, HashSet},
    iter::zip,
};

use answer::{Type as TypeAnnotation, Type, variable::Variable};
use concept::{
    error::ConceptReadError,
    type_::{OwnerAPI, PlayerAPI, TypeAPI, type_manager::TypeManager},
};
use encoding::value::{
    ValueEncodable,
    value_type::{ValueType, ValueTypeCategory},
};
use error::needs_update_when_feature_is_implemented;
use ir::{
    pattern::{
        ParameterID, Pattern, Vertex,
        conjunction::Conjunction,
        constraint::{
            Comparison, Constraint, ExpressionBinding, FunctionCallBinding, Has, Is, Isa, IsaKind, Kind, Label, Links,
            Owns, Plays, Relates, RoleName, Sub, SubKind, Value,
        },
        disjunction::Disjunction,
        nested_pattern::NestedPattern,
        variable_category::VariableCategory,
    },
    pipeline::{ParameterRegistry, VariableRegistry},
};
use itertools::Itertools;
use storage::snapshot::ReadableSnapshot;

use crate::annotation::{
    TypeInferenceError,
    function::{AnnotatedFunctionSignatures, FunctionParameterAnnotation},
    inference::{
        ConceptVertexTypes, ExtendMappedOperations, FromIteratorMappedOperations, TypeAnnotationSetTrait,
        ValueVertexTypes, VertexAnnotations, VertexTypeAnnotations,
        match_inference::{
            NestedTypeInferenceGraphDisjunction, TypeInferenceEdge, TypeInferenceExpression, TypeInferenceGraph,
        },
    },
    type_inference::{TypeInferenceMode, get_type_annotation_from_label},
};

pub struct TypeGraphSeedingContext<'this, Snapshot: ReadableSnapshot> {
    snapshot: &'this Snapshot,
    type_manager: &'this TypeManager,
    function_annotations: &'this dyn AnnotatedFunctionSignatures,
    variable_registry: &'this VariableRegistry,
    stage_type: TypeInferenceMode,
}

impl<'this, Snapshot: ReadableSnapshot> TypeGraphSeedingContext<'this, Snapshot> {
    fn is_write_stage(&self) -> bool {
        self.stage_type == TypeInferenceMode::ExactAndExplicit
    }

    fn prune_abstract(&self) -> bool {
        self.stage_type != TypeInferenceMode::IncludeAbstractSubtypes
    }

    fn may_assert_no_abstract(&self, variable: &Vertex<Variable>, types: &ConceptVertexTypes) {
        #[cfg(debug_assertions)]
        if self.stage_type != TypeInferenceMode::IncludeAbstractSubtypes {
            let is_thing = matches!(variable, Vertex::Variable(var) if {
                self.variable_registry.get_variable_category(*var).map_or(false, |cat| cat.is_category_thing())
            });
            debug_assert!(!is_thing || types.iter().all(|t| self.is_not_abstract(t).unwrap()));
        }
    }
}

impl<'this, Snapshot: ReadableSnapshot> TypeGraphSeedingContext<'this, Snapshot> {
    pub(crate) fn new(
        snapshot: &'this Snapshot,
        type_manager: &'this TypeManager,
        function_annotations: &'this dyn AnnotatedFunctionSignatures,
        variable_registry: &'this VariableRegistry,
        stage_type: TypeInferenceMode,
    ) -> Self {
        TypeGraphSeedingContext { snapshot, type_manager, function_annotations, variable_registry, stage_type }
    }

    pub(crate) fn create_graph<'graph>(
        &self,
        upstream_annotations: &VertexAnnotations,
        conjunction: &'graph Conjunction,
    ) -> Result<TypeInferenceGraph<'graph>, TypeInferenceError> {
        let mut graph = self.build_recursive(conjunction);
        // Pre-seed with upstream variable annotations.
        for variable in conjunction.visible_referenced_variables() {
            if let Some(annotations) = upstream_annotations.get(&Vertex::Variable(variable)) {
                graph.vertices.add_or_intersect(&Vertex::Variable(variable), Cow::Borrowed(annotations));
            }
        }
        // Advanced TODO: Copying upstream binary constraints as schema constraints.
        self.seed_types(&mut graph, &VertexAnnotations::default())?;

        debug_assert!(
            conjunction
                .constraints()
                .iter()
                .flat_map(|constraint| constraint.vertices())
                .filter(|vertex| !vertex.is_parameter())
                .unique()
                .all(|vertex| {
                    graph.vertices.contains_key(vertex)
                        || self.variable_registry.get_variable_category(vertex.as_variable().unwrap()).unwrap()
                            == VariableCategory::Value
                })
        );

        Ok(graph)
    }

    pub(crate) fn seed_types(
        &self,
        graph: &mut TypeInferenceGraph<'_>,
        parent_vertices: &VertexAnnotations,
    ) -> Result<(), TypeInferenceError> {
        debug_assert!(
            parent_vertices.is_empty(),
            "TODO: Cleanup if this never fires. It's always passed an empty one for some reason"
        );
        let vars_in_pattern =
            graph.conjunction.visible_referenced_variables().map(Vertex::Variable).collect::<HashSet<_>>();
        for (vertex, parent_annotations) in parent_vertices.iter() {
            if vars_in_pattern.contains(vertex) {
                graph.vertices.insert(vertex.clone(), parent_annotations.clone());
            }
        }

        // Seed vertices in root & disjunctions
        self.seed_vertex_annotations_from_type_and_called_function_signatures(graph)?;

        self.annotate_all_unannotated_value_vertices(graph)?;

        let mut some_concept_vertex_was_directly_annotated = true;
        while some_concept_vertex_was_directly_annotated {
            let mut changed = true;
            while changed {
                changed = self.propagate_vertex_annotations(graph)?;
            }
            some_concept_vertex_was_directly_annotated = self.annotate_some_unannotated_concept_vertex(graph)?;
        }

        // Prune abstract types from type annotations of thing variables
        if self.prune_abstract() {
            self.prune_abstract_types_from_thing_vertex_annotations_recursive(graph)?;
        }

        // Seed edges in root & disjunctions
        self.seed_edges(graph, self.stage_type)?;
        self.seed_expressions(graph)?;
        Ok(())
    }

    fn build_recursive<'conj>(&self, conjunction: &'conj Conjunction) -> TypeInferenceGraph<'conj> {
        let mut nested_disjunctions = Vec::new();
        for pattern in conjunction.nested_patterns() {
            match pattern {
                NestedPattern::Disjunction(disjunction) => {
                    nested_disjunctions.push(self.build_disjunction_recursive(disjunction));
                }
                NestedPattern::Negation(_) | NestedPattern::Optional(_) => {
                    // Done after full type-inference for the conjunctions & disjunctions.
                }
            }
        }

        TypeInferenceGraph {
            conjunction,
            vertices: VertexAnnotations::default(),
            edges: Vec::new(),
            expressions: Vec::new(),
            nested_disjunctions,
        }
    }

    fn build_disjunction_recursive<'conj>(
        &self,
        disjunction: &'conj Disjunction,
    ) -> NestedTypeInferenceGraphDisjunction<'conj> {
        let nested_graphs = disjunction.conjunctions().iter().map(|conj| self.build_recursive(conj)).collect_vec();
        let shared_variables = disjunction.visible_referenced_variables().collect();
        NestedTypeInferenceGraphDisjunction {
            disjunction_pattern: &disjunction,
            disjunction: nested_graphs,
            shared_variables,
            shared_vertex_annotations: VertexAnnotations::default(),
        }
    }

    // Phase 1: Collect all type & function return annotations
    fn seed_vertex_annotations_from_type_and_called_function_signatures(
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
                Constraint::Value(c) => c.apply(self, vertices)?,
                | Constraint::Iid(_)
                | Constraint::Is(_)
                | Constraint::Sub(_)
                | Constraint::Isa(_)
                | Constraint::Links(_)
                | Constraint::Has(_)
                | Constraint::Owns(_)
                | Constraint::Relates(_)
                | Constraint::Plays(_)
                | Constraint::ExpressionBinding(_)
                | Constraint::Comparison(_) // Done later
                | Constraint::LinksDeduplication(_) => (),
                Constraint::IndexedRelation(_) => {
                    unreachable!("IndexedRelations are only generated after type inference")
                }
                Constraint::Unsatisfiable(_) => {
                    unreachable!("Unsatisfiable are only generated after type inference")
                }
            }
        }
        // This leads to better error messages
        for c in graph.conjunction.constraints().iter().filter(|c| matches!(c, Constraint::Isa(_))) {
            self.try_propagating_vertex_annotation(c, vertices)?;
        }
        for c in graph.conjunction.constraints().iter().filter_map(|c| c.as_comparison()) {
            c.apply(self, vertices)?;
        }
        for nested_graph in graph.nested_disjunctions.iter_mut().flat_map(|nested| &mut nested.disjunction) {
            self.seed_vertex_annotations_from_type_and_called_function_signatures(nested_graph)?;
        }
        Ok(())
    }

    fn annotate_all_unannotated_value_vertices(
        &self,
        graph: &mut TypeInferenceGraph<'_>,
    ) -> Result<(), TypeInferenceError> {
        let all_value_types = VertexTypeAnnotations::value_from(BTreeSet::from_iter([
            ValueType::Boolean,
            ValueType::Integer,
            ValueType::Double,
            ValueType::Decimal,
            ValueType::Date,
            ValueType::DateTime,
            ValueType::DateTimeTZ,
            ValueType::Duration,
            ValueType::String,
        ]));
        for expr in graph.conjunction.constraints().iter().filter_map(|c| c.as_expression_binding()) {
            expr.ids_assigned()
                .chain(expr.expression_ids())
                .filter(|id| {
                    let variable_category = self.variable_registry.get_variable_category(*id);
                    Some(VariableCategory::Value) == variable_category
                })
                .for_each(|id| {
                    graph.vertices.annotations.entry(Vertex::Variable(id)).or_insert_with(|| all_value_types.clone());
                })
        }

        for nested_graph in graph.nested_disjunctions.iter_mut().flat_map(|nested| &mut nested.disjunction) {
            self.annotate_all_unannotated_value_vertices(nested_graph)?;
        }
        for nested in &mut graph.nested_disjunctions {
            self.reconcile_nested_disjunction(nested, &mut graph.vertices)?;
        }
        Ok(())
    }

    fn annotate_fixed_vertices(&self, graph: &mut TypeInferenceGraph<'_>) -> Result<(), TypeInferenceError> {
        for vertex in self.fixed_vertices(graph.conjunction.constraints()) {
            match vertex {
                Vertex::Variable(_) => unreachable!("variable in fixed vertices"),
                Vertex::Label(label) => {
                    if !graph.vertices.contains_key(vertex) {
                        let annotation_opt = get_type_annotation_from_label(self.snapshot, self.type_manager, label)?;
                        if let Some(annotation) = annotation_opt {
                            graph.vertices.insert(vertex.clone(), VertexTypeAnnotations::concept_from([annotation]));
                        } else {
                            return Err(TypeInferenceError::LabelNotResolved {
                                name: label.to_string(),
                                source_span: label.source_span(),
                            });
                        }
                    } else {
                        #[cfg(debug_assertions)]
                        {
                            let annotation_opt =
                                get_type_annotation_from_label(self.snapshot, self.type_manager, label)?;
                            debug_assert_ne!(annotation_opt, None);
                            debug_assert_eq!(
                                graph.vertices[vertex],
                                VertexTypeAnnotations::concept_from([annotation_opt.unwrap()])
                            );
                        }
                    }
                }
                &Vertex::Parameter(_) => {
                    debug_assert!(!graph.vertices.contains_key(vertex));
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

    fn variables_in_constraints<'a>(&self, conjunction: &'a Conjunction) -> impl Iterator<Item = Variable> + 'a {
        conjunction.constraints().iter().flat_map(|constraint| constraint.ids())
    }

    fn annotate_some_unannotated_concept_vertex(
        &self,
        graph: &mut TypeInferenceGraph<'_>,
    ) -> Result<bool, Box<ConceptReadError>> {
        // TODO: We could look for constraints (instead of variable categories) as a basis for annotation.
        //  We'd need TypeManager methods to iterate over all owns / relates / plays declarations.

        // If any variables remain that aren't in any producing constraint, seed them with all types
        //  TODO: This isn't very uncommon - when all disjunction branches produce a variable.
        //   Ideally, we'd use annotations from the disjunction.
        let unannotated_var = self.variables_in_constraints(graph.conjunction).find(|&var| {
            let vertex = Vertex::Variable(var);
            self.variable_registry.get_variable_category(var).unwrap_or(VariableCategory::Value)
                != VariableCategory::Value
                && !graph.vertices.contains_key(&vertex)
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
                    any |= self.annotate_some_unannotated_concept_vertex(nested_graph)?;
                }
            }
            Ok(any)
        }
    }

    fn get_unbounded_type_annotations(
        &self,
        category: VariableCategory,
    ) -> Result<VertexTypeAnnotations, Box<ConceptReadError>> {
        // We can't refine based on categories since categories are global.
        // Had categories been per scope, we could indeed have been more specific.
        let (include_thing_types, include_role_types) = match category {
            VariableCategory::Type => (true, true),
            VariableCategory::RoleType => (false, true),
            VariableCategory::ValueList | VariableCategory::Value => (false, false),
            VariableCategory::ThingType
            | VariableCategory::AttributeType
            | VariableCategory::ThingList
            | VariableCategory::Thing
            | VariableCategory::ObjectList
            | VariableCategory::Object
            | VariableCategory::AttributeList
            | VariableCategory::Attribute => (true, false),
            VariableCategory::AttributeOrValue => unreachable!("Insufficiently bound variable!"),
        };
        let mut annotations = ConceptVertexTypes(BTreeSet::new());

        let snapshot = self.snapshot;
        let type_manager = self.type_manager;
        if include_thing_types {
            annotations.extend_into(type_manager.get_entity_types(snapshot)?);
            annotations.extend_into(type_manager.get_relation_types(snapshot)?);
            annotations.extend_into(type_manager.get_attribute_types(snapshot)?);
        }
        if include_role_types {
            annotations.extend_into(type_manager.get_role_types(snapshot)?);
        }
        Ok(VertexTypeAnnotations::Concept(annotations))
    }

    // Phase 2: Use constraints to infer annotations on other vertices
    fn propagate_vertex_annotations(&self, graph: &mut TypeInferenceGraph<'_>) -> Result<bool, TypeInferenceError> {
        let mut is_modified = false;
        // Prioritise `isa` constraints
        for c in graph.conjunction.constraints().iter().filter(|c| matches!(c, Constraint::Isa(_))) {
            is_modified |= self.try_propagating_vertex_annotation(c, &mut graph.vertices)?;
        }

        for c in graph.conjunction.constraints().iter().filter(|c| !matches!(c, Constraint::Isa(_))) {
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
    ) -> Result<bool, Box<ConceptReadError>> {
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
            Constraint::Owns(owns) => self.try_propagating_vertex_annotation_impl(owns, vertices)?,
            Constraint::Relates(relates) => self.try_propagating_vertex_annotation_impl(relates, vertices)?,
            Constraint::Plays(plays) => self.try_propagating_vertex_annotation_impl(plays, vertices)?,
            Constraint::Comparison(_) // Unlike in 2.x, We don't use comparisons to propagate.
            | Constraint::Iid(_)
            | Constraint::ExpressionBinding(_)
            | Constraint::FunctionCallBinding(_)
            | Constraint::RoleName(_)
            | Constraint::Label(_)
            | Constraint::Kind(_)
            | Constraint::Value(_)
            | Constraint::LinksDeduplication(_) => false,
            Constraint::IndexedRelation(_) => unreachable!("Indexed relations are only generated after type inference"),
            Constraint::Unsatisfiable(_) => unreachable!("Unsatisfiable are only generated after type inference"),
        };
        Ok(any_modified)
    }

    fn try_propagating_vertex_annotation_impl(
        &self,
        inner: &impl BinaryConstraint,
        vertices: &mut VertexAnnotations,
    ) -> Result<bool, Box<ConceptReadError>> {
        let (left, right) = (inner.left(), inner.right());
        let any_modified = match (vertices.get(left), vertices.get(right)) {
            (Some(VertexTypeAnnotations::Concept(left_types)), None) => {
                let mut right_types = ConceptVertexTypes(BTreeSet::new());
                left_types
                    .iter()
                    .try_for_each(|type_| inner.annotate_left_to_right_for_type(self, type_, &mut right_types))?;
                vertices.insert(right.clone(), VertexTypeAnnotations::Concept(right_types.into()));
                true
            }
            (None, Some(VertexTypeAnnotations::Concept(right_types))) => {
                let mut left_types = ConceptVertexTypes(BTreeSet::new());
                right_types
                    .iter()
                    .try_for_each(|type_| inner.annotate_right_to_left_for_type(self, type_, &mut left_types))?;
                vertices.insert(left.clone(), VertexTypeAnnotations::Concept(left_types.into()));
                true
            }
            (None, None)
            | (Some(_), Some(_))
            | (None, Some(VertexTypeAnnotations::Value(_)))
            | (Some(VertexTypeAnnotations::Value(_)), None) => false,
        };
        Ok(any_modified)
    }

    fn reconcile_nested_disjunction(
        &self,
        nested: &mut NestedTypeInferenceGraphDisjunction<'_>,
        parent_vertices: &mut VertexAnnotations,
    ) -> Result<bool, TypeInferenceError> {
        let mut something_changed = false;
        // Apply annotations ot the parent on the nested
        for &variable in nested.shared_variables.iter() {
            let vertex = Vertex::Variable(variable);
            if let Some(parent_annotations) = parent_vertices.get_mut(&vertex) {
                for nested_graph in &mut nested.disjunction {
                    // Note: This adds a vertex annotation even if this branch does not reference the variable
                    // This is needed to prevent one branch from narrowing the parent's annotations
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
            disjunction_pattern: _,
            shared_vertex_annotations,
            disjunction: nested_graph_disjunction,
            shared_variables,
        } = nested;
        for &variable in shared_variables.iter() {
            let vertex = Vertex::Variable(variable);
            #[allow(clippy::map_entry, reason = "false positive")]
            if !shared_vertex_annotations.contains_key(&vertex) {
                if let Some(types_from_branches) =
                    self.try_union_annotations_across_all_branches(nested_graph_disjunction, &vertex)?
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
    ) -> Result<Option<VertexTypeAnnotations>, TypeInferenceError> {
        if disjunction.iter().all(|nested_graph| nested_graph.vertices.contains_key(vertex)) {
            let mut union: Option<VertexTypeAnnotations> = None;
            disjunction.iter().try_for_each(|nested_graph| {
                let branch_annotations = nested_graph.vertices.get(vertex).unwrap();
                if let Some(inner) = &mut union {
                    inner.extend_to_union(branch_annotations)?;
                } else {
                    union = Some(branch_annotations.clone())
                }
                Ok::<(), TypeInferenceError>(())
            })?;
            Ok(union)
        } else {
            Ok(None)
        }
    }

    // Phase 3: seed edges
    fn seed_edges(
        &self,
        graph: &mut TypeInferenceGraph<'_>,
        stage_type: TypeInferenceMode,
    ) -> Result<(), TypeInferenceError> {
        #[cfg(debug_assertions)]
        graph.vertices.iter().for_each(|(variable, types)| match types {
            VertexTypeAnnotations::Concept(types) => self.may_assert_no_abstract(variable, types),
            VertexTypeAnnotations::Value(_) => (),
        });
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
                    // We don't use comparisons to propagate, but we still want to use it to prune.
                    // And we only prune concept types across edges for now.
                    if let (Some(VertexTypeAnnotations::Concept(_)), Some(VertexTypeAnnotations::Concept(_))) =
                        (vertices.get(cmp.left()), vertices.get(cmp.right()))
                    {
                        edges.push(self.seed_edge(constraint, cmp, vertices)?)
                    }
                }
                Constraint::Owns(owns) => edges.push(self.seed_edge(constraint, owns, vertices)?),
                Constraint::Relates(relates) => edges.push(self.seed_edge(constraint, relates, vertices)?),
                Constraint::Plays(plays) => edges.push(self.seed_edge(constraint, plays, vertices)?),
                | Constraint::Iid(_)
                | Constraint::RoleName(_)
                | Constraint::Label(_)
                | Constraint::Kind(_)
                | Constraint::Value(_)
                | Constraint::ExpressionBinding(_)
                | Constraint::FunctionCallBinding(_)
                | Constraint::LinksDeduplication(_) => (), // Do nothing
                Constraint::IndexedRelation(_) => {
                    unreachable!("Indexed relations are only generated after type inference")
                }
                Constraint::Unsatisfiable(_) => {
                    unreachable!("Unsatisfiable are only generated after type inference")
                }
            }
        }
        for disj in &mut graph.nested_disjunctions {
            for nested_graph in &mut disj.disjunction {
                self.seed_edges(nested_graph, stage_type)?;
            }
        }
        Ok(())
    }

    fn seed_edge<'conj>(
        &self,
        constraint: &'conj Constraint<Variable>,
        inner: &impl BinaryConstraint,
        vertices: &VertexAnnotations,
    ) -> Result<TypeInferenceEdge<'conj>, TypeInferenceError> {
        let (left, right) = (inner.left().clone(), inner.right().clone());
        let Some(VertexTypeAnnotations::Concept(left_vertex_types)) = vertices.get(&left) else {
            return Err(TypeInferenceError::InternalVertexTypesMismatch { expected: "concept".to_owned() });
        };
        let Some(VertexTypeAnnotations::Concept(right_vertex_types)) = vertices.get(&right) else {
            return Err(TypeInferenceError::InternalVertexTypesMismatch { expected: "concept".to_owned() });
        };
        let left_to_right = inner.annotate_left_to_right(self, left_vertex_types, right_vertex_types)?;
        let right_to_left = inner.annotate_right_to_left(self, right_vertex_types, left_vertex_types)?;
        debug_assert!(left_to_right.values().all(|v| !v.is_empty()));
        debug_assert!(right_to_left.values().all(|v| !v.is_empty()));
        Ok(TypeInferenceEdge::build(constraint, left, right, left_to_right, right_to_left))
    }

    fn seed_expressions(&self, graph: &mut TypeInferenceGraph<'_>) -> Result<(), TypeInferenceError> {
        let expressions = graph.conjunction.constraints().iter().filter_map(Constraint::as_expression_binding);
        for expr in expressions {
            graph.expressions.push(self.seed_expression(&graph.vertices, expr)?);
        }
        for disj in &mut graph.nested_disjunctions {
            for nested_graph in &mut disj.disjunction {
                self.seed_expressions(nested_graph)?;
            }
        }
        Ok(())
    }

    fn seed_expression<'conj>(
        &self,
        vertices: &VertexAnnotations,
        expression: &'conj ExpressionBinding<Variable>,
    ) -> Result<TypeInferenceExpression<'conj>, TypeInferenceError> {
        needs_update_when_feature_is_implemented!(error::UnimplementedFeature::Structs);
        let attribute_argument_types = expression.expression_ids().filter_map(|arg| {
            match vertices.get(&Vertex::Variable(arg)).expect("All vertices should be annotated by now") {
                VertexTypeAnnotations::Concept(types) => Some(types),
                VertexTypeAnnotations::Value(_) => None,
            }
        });
        let attribute_types_flattened = attribute_argument_types
            .flat_map(|concept_types| concept_types.iter())
            .unique()
            .filter_map(|type_| type_.is_attribute_type().then(|| type_.as_attribute_type()));

        let mut value_types_of_attributes = BTreeMap::new();
        for attribute_type in attribute_types_flattened {
            if let Some(value_type) = attribute_type.get_value_type_without_source(self.snapshot, self.type_manager)? {
                value_types_of_attributes.insert(attribute_type, value_type);
            }
        }
        Ok(TypeInferenceExpression {
            expression,
            assigned: expression.left().clone(),
            args: expression.expression_ids().map(|v| Vertex::Variable(v)).collect(),
            compiled_expression: None,
            value_types_of_attributes,
        })
    }

    fn is_not_abstract(&self, type_: &TypeAnnotation) -> Result<bool, Box<ConceptReadError>> {
        type_.is_abstract(self.snapshot, self.type_manager).map(|b| !b)
    }

    fn prune_abstract_types_from_thing_vertex_annotations_recursive(
        &self,
        graph: &mut TypeInferenceGraph<'_>,
    ) -> Result<(), TypeInferenceError> {
        for annotated_vertex in &mut graph.vertices {
            let (Vertex::Variable(id), VertexTypeAnnotations::Concept(annotations)) = annotated_vertex else {
                continue;
            };
            if self.variable_registry.get_variable_category(*id).is_some_and(|cat| cat.is_category_thing()) {
                try_retain(annotations, |type_| self.is_not_abstract(type_))?;
            }
        }
        for nested in graph.nested_disjunctions.iter_mut().flat_map(|nested| nested.disjunction.iter_mut()) {
            self.prune_abstract_types_from_thing_vertex_annotations_recursive(nested)?;
        }
        Ok(())
    }
}

fn try_retain<T: Ord + Copy, E>(set: &mut BTreeSet<T>, predicate: impl Fn(&T) -> Result<bool, E>) -> Result<(), E> {
    let mut to_be_removed = Vec::new();
    for item in set.iter() {
        if !predicate(item)? {
            to_be_removed.push(*item);
        }
    }
    for annotation in to_be_removed.iter() {
        set.remove(annotation);
    }
    Ok(())
}

trait UnaryConstraint {
    fn apply<Snapshot: ReadableSnapshot>(
        &self,
        context: &TypeGraphSeedingContext<'_, Snapshot>,
        graph_vertices: &mut VertexAnnotations,
    ) -> Result<(), TypeInferenceError>;
}

pub(crate) fn get_type_annotation_and_subtypes_from_label<Snapshot: ReadableSnapshot>(
    snapshot: &Snapshot,
    type_manager: &TypeManager,
    label_value: &encoding::value::label::Label,
) -> Result<BTreeSet<TypeAnnotation>, TypeInferenceError> {
    let type_opt = get_type_annotation_from_label(snapshot, type_manager, label_value)?;
    let Some(type_) = type_opt else {
        return Err(TypeInferenceError::LabelNotResolved {
            name: label_value.scoped_name().to_string(),
            source_span: label_value.source_span(),
        });
    };
    let mut types: BTreeSet<Type> = match &type_ {
        TypeAnnotation::Entity(type_) => {
            BTreeSet::from_into_ref(&type_.get_subtypes_transitive(snapshot, type_manager)?)
        }
        TypeAnnotation::Relation(type_) => {
            BTreeSet::from_into_ref(&type_.get_subtypes_transitive(snapshot, type_manager)?)
        }
        TypeAnnotation::Attribute(type_) => {
            BTreeSet::from_into_ref(&type_.get_subtypes_transitive(snapshot, type_manager)?)
        }
        TypeAnnotation::RoleType(type_) => {
            BTreeSet::from_into_ref(&type_.get_subtypes_transitive(snapshot, type_manager)?)
        }
    };
    types.insert(type_);
    Ok(types)
}

impl UnaryConstraint for Kind<Variable> {
    fn apply<Snapshot: ReadableSnapshot>(
        &self,
        context: &TypeGraphSeedingContext<'_, Snapshot>,
        graph_vertices: &mut VertexAnnotations,
    ) -> Result<(), TypeInferenceError> {
        use encoding::graph::type_::Kind as EncodingKind;
        let type_manager = &context.type_manager;
        let annotations = match self.kind() {
            EncodingKind::Entity => BTreeSet::from_into(type_manager.get_entity_types(context.snapshot)?),
            EncodingKind::Relation => BTreeSet::from_into(type_manager.get_relation_types(context.snapshot)?),
            EncodingKind::Attribute => BTreeSet::from_into(type_manager.get_attribute_types(context.snapshot)?),
            EncodingKind::Role => BTreeSet::from_into(type_manager.get_role_types(context.snapshot)?),
        };
        graph_vertices
            .add_or_intersect::<ConceptVertexTypes>(self.type_(), Cow::Owned(ConceptVertexTypes(annotations)));
        Ok(())
    }
}

impl UnaryConstraint for Label<Variable> {
    fn apply<Snapshot: ReadableSnapshot>(
        &self,
        _context: &TypeGraphSeedingContext<'_, Snapshot>,
        graph_vertices: &mut VertexAnnotations,
    ) -> Result<(), TypeInferenceError> {
        let annotation_opt = graph_vertices.get(self.type_label());
        if let Some(annotation) = annotation_opt {
            graph_vertices.add_or_intersect::<VertexTypeAnnotations>(self.type_(), Cow::Owned(annotation.clone()));
            Ok(())
        } else {
            Err(TypeInferenceError::LabelNotResolved {
                name: self.type_label().to_string(),
                source_span: self.source_span(),
            })
        }
    }
}

impl UnaryConstraint for RoleName<Variable> {
    fn apply<Snapshot: ReadableSnapshot>(
        &self,
        context: &TypeGraphSeedingContext<'_, Snapshot>,
        graph_vertices: &mut VertexAnnotations,
    ) -> Result<(), TypeInferenceError> {
        let role_types_opt = context.type_manager.get_roles_by_name(context.snapshot, self.name())?;
        if let Some(role_types) = role_types_opt {
            let mut annotations = ConceptVertexTypes(BTreeSet::new());
            for &role_type in &*role_types {
                annotations.insert(role_type.into());
                if !context.is_write_stage() {
                    annotations
                        .extend_into_ref(&role_type.get_subtypes_transitive(context.snapshot, context.type_manager)?);
                }
            }
            graph_vertices.add_or_intersect::<ConceptVertexTypes>(self.type_(), Cow::Owned(annotations));
            Ok(())
        } else {
            Err(TypeInferenceError::RoleNameNotResolved {
                name: self.name().to_string(),
                source_span: self.source_span(),
            })
        }
    }
}

impl UnaryConstraint for Value<Variable> {
    fn apply<Snapshot: ReadableSnapshot>(
        &self,
        context: &TypeGraphSeedingContext<'_, Snapshot>,
        graph_vertices: &mut VertexAnnotations,
    ) -> Result<(), TypeInferenceError> {
        let pattern_value_type = match self.value_type() {
            ir::pattern::ValueType::Builtin(value_type) => Ok(value_type.clone()),
            ir::pattern::ValueType::Struct(struct_name) => {
                let pattern_key = context.type_manager.get_struct_definition_key(context.snapshot, struct_name);
                match pattern_key {
                    Ok(Some(key)) => Ok(ValueType::Struct(key)),
                    Ok(None) => Err(TypeInferenceError::ValueTypeNotFound {
                        name: struct_name.clone().to_owned(),
                        source_span: self.source_span(),
                    }),
                    Err(source) => Err(TypeInferenceError::ConceptRead { typedb_source: source }),
                }
            }
        }?;

        let mut annotations = ConceptVertexTypes(BTreeSet::new());
        let attribute_types = context.type_manager.get_attribute_types(context.snapshot)?;
        for attribute_type in attribute_types {
            let attribute_value_type_opt =
                attribute_type.get_value_type_without_source(context.snapshot, context.type_manager)?;
            if let Some(attribute_value_type) = attribute_value_type_opt {
                if pattern_value_type == attribute_value_type {
                    annotations.insert(attribute_type.into());
                }
            }
        }

        graph_vertices.add_or_intersect::<ConceptVertexTypes>(self.attribute_type(), Cow::Owned(annotations));
        Ok(())
    }
}

impl UnaryConstraint for FunctionCallBinding<Variable> {
    fn apply<Snapshot: ReadableSnapshot>(
        &self,
        context: &TypeGraphSeedingContext<'_, Snapshot>,
        graph_vertices: &mut VertexAnnotations,
    ) -> Result<(), TypeInferenceError> {
        if let Some(annotated_function_signature) =
            context.function_annotations.get_annotated_signature(&self.function_call().function_id())
        {
            for (assigned_variable, return_annotation) in
                zip(self.assigned(), annotated_function_signature.returns.iter())
            {
                match return_annotation {
                    FunctionParameterAnnotation::Concept(types) => {
                        graph_vertices.add_or_intersect::<ConceptVertexTypes>(
                            assigned_variable,
                            Cow::Owned(ConceptVertexTypes(types.clone())),
                        );
                    }
                    FunctionParameterAnnotation::Value(value_type) => {
                        graph_vertices.add_or_intersect::<ValueVertexTypes>(
                            assigned_variable,
                            Cow::Owned(ValueVertexTypes(BTreeSet::from([*value_type]))),
                        );
                    }
                    FunctionParameterAnnotation::AnyConcept => {
                        debug_assert!(false, "We can't return AnyConcept");
                    }
                }
            }
            // TODO: Should we be pruning, or should we be error-ing?
            let args = self.function_call().argument_ids();
            for (arg_var, arg_annotations) in zip(args, &annotated_function_signature.arguments) {
                match arg_annotations {
                    FunctionParameterAnnotation::Concept(types) => {
                        graph_vertices.add_or_intersect::<ConceptVertexTypes>(
                            &Vertex::Variable(arg_var),
                            Cow::Owned(ConceptVertexTypes(types.clone())),
                        );
                    }
                    FunctionParameterAnnotation::Value(value_type) => {
                        graph_vertices.add_or_intersect::<ValueVertexTypes>(
                            &Vertex::Variable(arg_var),
                            Cow::Owned(ValueVertexTypes(BTreeSet::from([*value_type]))),
                        );
                    }
                    FunctionParameterAnnotation::AnyConcept => {
                        // Let other constraints seed it
                    }
                }
            }
        }
        Ok(())
    }
}

impl UnaryConstraint for Comparison<Variable> {
    fn apply<Snapshot: ReadableSnapshot>(
        &self,
        context: &TypeGraphSeedingContext<'_, Snapshot>,
        graph_vertices: &mut VertexAnnotations,
    ) -> Result<(), TypeInferenceError> {
        let attributes_lazy = LazyCell::new(|| {
            let types = BTreeSet::from_into(context.type_manager.get_attribute_types(context.snapshot)?);
            Ok(ConceptVertexTypes(types))
        });
        if let Vertex::Variable(var) = self.lhs() {
            if context.variable_registry.get_variable_category(*var).map_or(false, |cat| cat.is_category_thing()) {
                let attributes = (*attributes_lazy).as_ref().map_err(TypeInferenceError::clone)?;
                graph_vertices.add_or_intersect(self.lhs(), Cow::Borrowed(attributes));
            }
        }
        if let Vertex::Variable(var) = self.rhs() {
            if context.variable_registry.get_variable_category(*var).map_or(false, |cat| cat.is_category_thing()) {
                let attributes = (*attributes_lazy).as_ref().map_err(TypeInferenceError::clone)?;
                graph_vertices.add_or_intersect(self.rhs(), Cow::Borrowed(attributes));
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
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_types: &ConceptVertexTypes,
        allowed_right_types: &ConceptVertexTypes,
    ) -> Result<BTreeMap<TypeAnnotation, ConceptVertexTypes>, Box<ConceptReadError>> {
        let mut left_to_right = BTreeMap::new();
        context.may_assert_no_abstract(self.left(), &left_types);
        context.may_assert_no_abstract(self.right(), &allowed_right_types);
        for left_type in left_types {
            let mut right_annotations = ConceptVertexTypes(BTreeSet::new());
            self.annotate_left_to_right_for_type(context, left_type, &mut right_annotations)?;
            right_annotations.retain_intersection(allowed_right_types);
            context.may_assert_no_abstract(self.right(), &right_annotations);
            if !right_annotations.is_empty() {
                left_to_right.insert(*left_type, right_annotations);
            }
        }
        Ok(left_to_right)
    }

    fn annotate_right_to_left(
        &self,
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_types: &ConceptVertexTypes,
        allowed_left_types: &ConceptVertexTypes,
    ) -> Result<BTreeMap<TypeAnnotation, ConceptVertexTypes>, Box<ConceptReadError>> {
        let mut right_to_left = BTreeMap::new();
        context.may_assert_no_abstract(self.left(), &allowed_left_types);
        context.may_assert_no_abstract(self.right(), &right_types);
        for right_type in right_types {
            let mut left_annotations = ConceptVertexTypes(BTreeSet::new());
            self.annotate_right_to_left_for_type(context, right_type, &mut left_annotations)?;
            left_annotations.retain_intersection(allowed_left_types);
            context.may_assert_no_abstract(self.left(), &left_annotations);
            if !left_annotations.is_empty() {
                right_to_left.insert(*right_type, left_annotations);
            }
        }
        Ok(right_to_left)
    }

    fn annotate_left_to_right_for_type(
        &self,
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>>;

    fn annotate_right_to_left_for_type(
        &self,
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>>;
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
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        let Some(owner) = left_type.try_as_object_type() else {
            return Ok(()); // It can't be another type => Do nothing and let type-inference clean it up
        };
        collector.extend_mapped_ref(&owner.get_owns(context.snapshot, context.type_manager)?, |owns| {
            TypeAnnotation::Attribute(owns.attribute())
        });
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        let TypeAnnotation::Attribute(attribute) = right_type else {
            return Ok(()); // It can't be another type => Do nothing and let type-inference clean it up
        };
        collector.extend_into(attribute.get_owner_types(context.snapshot, context.type_manager)?.keys().cloned());
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
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        let Some(owner) = left_type.try_as_object_type() else {
            // It can't be another type => Do nothing and let type-inference clean it up
            return Ok(());
        };
        collector.extend_mapped_ref(&owner.get_owns(context.snapshot, context.type_manager)?, |owns| {
            TypeAnnotation::Attribute(owns.attribute())
        });
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        let TypeAnnotation::Attribute(attribute) = right_type else {
            // It can't be another type => Do nothing and let type-inference clean it up
            return Ok(());
        };
        collector.extend_into_ref(attribute.get_owner_types(context.snapshot, context.type_manager)?.keys());
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
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        if !context.is_write_stage() && self.isa_kind() == IsaKind::Subtype {
            match left_type {
                TypeAnnotation::Attribute(attribute) => collector
                    .extend_into_ref(&attribute.get_supertypes_transitive(context.snapshot, context.type_manager)?),
                TypeAnnotation::Entity(entity) => collector
                    .extend_into_ref(&entity.get_supertypes_transitive(context.snapshot, context.type_manager)?),
                TypeAnnotation::Relation(relation) => collector
                    .extend_into_ref(&relation.get_supertypes_transitive(context.snapshot, context.type_manager)?),
                TypeAnnotation::RoleType(_) => {
                    // Add nothing to the collector -> it'll get pruned
                }
            }
        }
        collector.insert(*left_type);
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        if !context.is_write_stage() && self.isa_kind() == IsaKind::Subtype {
            match right_type {
                TypeAnnotation::Attribute(attribute) => collector
                    .extend_into_ref(&attribute.get_subtypes_transitive(context.snapshot, context.type_manager)?),
                TypeAnnotation::Entity(entity) => {
                    collector.extend_into_ref(&entity.get_subtypes_transitive(context.snapshot, context.type_manager)?)
                }
                TypeAnnotation::Relation(relation) => collector
                    .extend_into_ref(&relation.get_subtypes_transitive(context.snapshot, context.type_manager)?),
                TypeAnnotation::RoleType(_) => {
                    // Add nothing to the collector -> it'll get pruned
                }
            }
        }
        collector.insert(*right_type);
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
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        if self.sub_kind() == SubKind::Subtype {
            match left_type {
                TypeAnnotation::Attribute(attribute) => collector
                    .extend_into_ref(&attribute.get_supertypes_transitive(context.snapshot, context.type_manager)?),
                TypeAnnotation::Entity(entity) => collector
                    .extend_into_ref(&entity.get_supertypes_transitive(context.snapshot, context.type_manager)?),
                TypeAnnotation::Relation(relation) => collector
                    .extend_into_ref(&relation.get_supertypes_transitive(context.snapshot, context.type_manager)?),
                TypeAnnotation::RoleType(role_type) => collector
                    .extend_into_ref(&role_type.get_supertypes_transitive(context.snapshot, context.type_manager)?),
            }
            collector.insert(*left_type);
        } else {
            match left_type {
                TypeAnnotation::Attribute(attribute) => {
                    if let Some(supertype) = attribute.get_supertype(context.snapshot, context.type_manager)? {
                        collector.insert(supertype.into());
                    }
                }
                TypeAnnotation::Entity(entity) => {
                    if let Some(supertype) = entity.get_supertype(context.snapshot, context.type_manager)? {
                        collector.insert(supertype.into());
                    }
                }
                TypeAnnotation::Relation(relation) => {
                    if let Some(supertype) = relation.get_supertype(context.snapshot, context.type_manager)? {
                        collector.insert(supertype.into());
                    }
                }
                TypeAnnotation::RoleType(role_type) => {
                    if let Some(supertype) = role_type.get_supertype(context.snapshot, context.type_manager)? {
                        collector.insert(supertype.into());
                    }
                }
            }
        }
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        if self.sub_kind() == SubKind::Subtype {
            match right_type {
                TypeAnnotation::Attribute(attribute) => collector
                    .extend_into_ref(&attribute.get_subtypes_transitive(context.snapshot, context.type_manager)?),
                TypeAnnotation::Entity(entity) => {
                    collector.extend_into_ref(&entity.get_subtypes_transitive(context.snapshot, context.type_manager)?)
                }
                TypeAnnotation::Relation(relation) => collector
                    .extend_into_ref(&relation.get_subtypes_transitive(context.snapshot, context.type_manager)?),
                TypeAnnotation::RoleType(role_type) => collector
                    .extend_into_ref(&role_type.get_subtypes_transitive(context.snapshot, context.type_manager)?),
            }
            collector.insert(*right_type);
        } else {
            match right_type {
                TypeAnnotation::Attribute(attribute) => {
                    collector.extend_into_ref(&attribute.get_subtypes(context.snapshot, context.type_manager)?)
                }
                TypeAnnotation::Entity(entity) => {
                    collector.extend_into_ref(&entity.get_subtypes(context.snapshot, context.type_manager)?);
                }
                TypeAnnotation::Relation(relation) => {
                    collector.extend_into_ref(&relation.get_subtypes(context.snapshot, context.type_manager)?)
                }
                TypeAnnotation::RoleType(role_type) => {
                    collector.extend_into_ref(&role_type.get_subtypes(context.snapshot, context.type_manager)?)
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
        _context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        collector.insert(*left_type);
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        _context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        collector.insert(*right_type);
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

    fn annotate_left_to_right(
        &self,
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_types: &ConceptVertexTypes,
        allowed_right_types: &ConceptVertexTypes,
    ) -> Result<BTreeMap<TypeAnnotation, ConceptVertexTypes>, Box<ConceptReadError>> {
        let mut left_to_right = BTreeMap::new();
        context.may_assert_no_abstract(self.left(), &left_types);
        context.may_assert_no_abstract(self.right(), &allowed_right_types);
        // TODO: Optimise?
        for left_type in left_types {
            let mut right_annotations = ConceptVertexTypes(BTreeSet::new());
            let left_value_type = match left_type {
                TypeAnnotation::Attribute(attribute) => {
                    attribute.get_value_type_without_source(context.snapshot, context.type_manager)?
                }
                _ => None,
            };
            if let Some(value_type) = left_value_type {
                let comparable_types = ValueTypeCategory::comparable_categories(value_type.category());
                for subattr in allowed_right_types {
                    if let Some(subvaluetype) = subattr
                        .as_attribute_type()
                        .get_value_type_without_source(context.snapshot, context.type_manager)?
                    {
                        if comparable_types.contains(&subvaluetype.category()) {
                            right_annotations.insert(subattr.as_attribute_type().into());
                        }
                    }
                }
            }
            context.may_assert_no_abstract(self.right(), &right_annotations);
            if !right_annotations.is_empty() {
                left_to_right.insert(*left_type, right_annotations);
            }
        }
        Ok(left_to_right)
    }

    fn annotate_right_to_left(
        &self,
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_types: &ConceptVertexTypes,
        allowed_left_types: &ConceptVertexTypes,
    ) -> Result<BTreeMap<TypeAnnotation, ConceptVertexTypes>, Box<ConceptReadError>> {
        let mut right_to_left = BTreeMap::new();
        #[cfg(debug_assertions)]
        context.may_assert_no_abstract(self.left(), &allowed_left_types);
        context.may_assert_no_abstract(self.right(), &right_types);
        // TODO: Optimise?
        for right_type in right_types {
            let mut left_annotations = ConceptVertexTypes(BTreeSet::new());
            let right_value_type = match right_type {
                TypeAnnotation::Attribute(attribute) => {
                    attribute.get_value_type_without_source(context.snapshot, context.type_manager)?
                }
                _ => None,
            };
            if let Some(value_type) = right_value_type {
                let comparable_types = ValueTypeCategory::comparable_categories(value_type.category());
                for &subattr in allowed_left_types {
                    if let Some(subvaluetype) = subattr
                        .as_attribute_type()
                        .get_value_type_without_source(context.snapshot, context.type_manager)?
                    {
                        if comparable_types.contains(&subvaluetype.category()) {
                            left_annotations.insert(subattr.into());
                        }
                    }
                }
            }
            context.may_assert_no_abstract(self.left(), &left_annotations);
            if !left_annotations.is_empty() {
                right_to_left.insert(*right_type, left_annotations);
            }
        }
        Ok(right_to_left)
    }

    fn annotate_left_to_right_for_type(
        &self,
        _context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        _left_type: &TypeAnnotation,
        _collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        unreachable!()
    }

    fn annotate_right_to_left_for_type(
        &self,
        _context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        _left_type: &TypeAnnotation,
        _collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        unreachable!()
    }
}

struct PlayerRoleEdge<'graph> {
    links: &'graph Links<Variable>,
}

struct RelationRoleEdge<'graph> {
    links: &'graph Links<Variable>,
}

impl BinaryConstraint for PlayerRoleEdge<'_> {
    fn left(&self) -> &Vertex<Variable> {
        self.links.player()
    }

    fn right(&self) -> &Vertex<Variable> {
        self.links.role_type()
    }

    fn annotate_left_to_right_for_type(
        &self,
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        let Some(player) = left_type.try_as_object_type() else {
            // It can't be another type => Do nothing and let type-inference clean it up
            return Ok(());
        };
        collector.extend_mapped_ref(&player.get_plays(context.snapshot, context.type_manager)?, |plays| {
            TypeAnnotation::RoleType(plays.role())
        });
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        let TypeAnnotation::RoleType(role_type) = right_type else {
            return Ok(());
            // It can't be another type => Do nothing and let type-inference clean it up
        };
        collector.extend_into_ref(role_type.get_player_types(context.snapshot, context.type_manager)?.keys());
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
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        let Some(player) = left_type.try_as_object_type() else {
            // It can't be another type => Do nothing and let type-inference clean it up
            return Ok(());
        };
        collector.extend_mapped_ref(&player.get_plays(context.snapshot, context.type_manager)?, |plays| {
            TypeAnnotation::RoleType(plays.role())
        });
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        let TypeAnnotation::RoleType(role_type) = right_type else {
            // It can't be another type => Do nothing and let type-inference clean it up
            return Ok(());
        };
        collector.extend_into_ref(role_type.get_player_types(context.snapshot, context.type_manager)?.keys());
        Ok(())
    }
}

impl BinaryConstraint for RelationRoleEdge<'_> {
    fn left(&self) -> &Vertex<Variable> {
        self.links.relation()
    }

    fn right(&self) -> &Vertex<Variable> {
        self.links.role_type()
    }

    fn annotate_left_to_right_for_type(
        &self,
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        let TypeAnnotation::Relation(relation) = left_type else {
            // It can't be another type => Do nothing and let type-inference clean it up
            return Ok(());
        };
        for relates in relation.get_relates(context.snapshot, context.type_manager)?.iter() {
            let is_write_stage_and_relates_is_abstract = context.is_write_stage()
                && relation.is_related_role_type_abstract(context.snapshot, context.type_manager, relates.role())?;
            if !is_write_stage_and_relates_is_abstract {
                collector.insert(TypeAnnotation::RoleType(relates.role()));
            }
        }
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        let TypeAnnotation::RoleType(role) = right_type else {
            // It can't be another type => Do nothing and let type-inference clean it up
            return Ok(());
        };
        for (&relation, _) in role.get_relation_types(context.snapshot, context.type_manager)?.iter() {
            let is_write_stage_and_relates_is_abstract = context.is_write_stage()
                && relation.is_related_role_type_abstract(context.snapshot, context.type_manager, *role)?;
            if !is_write_stage_and_relates_is_abstract {
                collector.insert(TypeAnnotation::Relation(relation));
            }
        }
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
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        left_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        let TypeAnnotation::Relation(relation) = left_type else {
            // It can't be another type => Do nothing and let type-inference clean it up
            return Ok(());
        };
        collector.extend_mapped_ref(&relation.get_relates(context.snapshot, context.type_manager)?, |relates| {
            TypeAnnotation::RoleType(relates.role())
        });
        Ok(())
    }

    fn annotate_right_to_left_for_type(
        &self,
        context: &TypeGraphSeedingContext<'_, impl ReadableSnapshot>,
        right_type: &TypeAnnotation,
        collector: &mut ConceptVertexTypes,
    ) -> Result<(), Box<ConceptReadError>> {
        let TypeAnnotation::RoleType(role_type) = right_type else {
            // It can't be another type => Do nothing and let type-inference clean it up
            return Ok(());
        };
        collector.extend_into_ref(role_type.get_relation_types(context.snapshot, context.type_manager)?.keys());
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use answer::Type as TypeAnnotation;
    use concept::type_::{Ordering, OwnerAPI};
    use encoding::value::{label::Label, value_type::ValueType};
    use ir::{
        pattern::{
            ParameterID, Vertex,
            constraint::{Comparator, IsaKind},
        },
        pipeline::{ParameterRegistry, block::Block},
        translation::PipelineTranslationContext,
    };
    use resource::profile::{CommitProfile, StorageCounters};
    use storage::snapshot::CommittableSnapshot;

    use crate::annotation::{
        function::EmptyAnnotatedFunctionSignatures,
        inference::{
            VertexAnnotations,
            match_inference::{TypeInferenceGraph, tests::expected_edge},
            type_seeder::{TypeGraphSeedingContext, TypeInferenceMode},
        },
        tests::{
            managers,
            schema_consts::{LABEL_CAT, LABEL_NAME, setup_types},
            setup_storage,
        },
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
        let mut translation_context = PipelineTranslationContext::new();
        let mut value_parameters = ParameterRegistry::new();
        let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
        let mut conjunction = builder.conjunction_mut();
        let var_animal = conjunction.constraints_mut().get_or_declare_variable("animal", None).unwrap();
        let var_name = conjunction.constraints_mut().get_or_declare_variable("name", None).unwrap();
        let var_animal_type = conjunction.constraints_mut().get_or_declare_variable("animal_type", None).unwrap();
        let var_name_type = conjunction.constraints_mut().get_or_declare_variable("name_type", None).unwrap();

        // Try seeding
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_animal, var_animal_type.into(), None).unwrap();
        conjunction.constraints_mut().add_label(var_animal_type, LABEL_CAT.clone()).unwrap();
        conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_name, var_name_type.into(), None).unwrap();
        conjunction.constraints_mut().add_label(var_name_type, LABEL_NAME.clone()).unwrap();
        conjunction.constraints_mut().add_has(var_animal, var_name, None).unwrap();

        let block = builder.finish().unwrap();
        let conjunction = block.conjunction();

        let constraints = conjunction.constraints();
        let expected_graph = TypeInferenceGraph {
            conjunction,
            vertices: VertexAnnotations::from_iter([
                (var_animal.into(), BTreeSet::from([type_cat])),
                (var_name.into(), BTreeSet::from([type_catname, type_dogname])),
                (var_animal_type.into(), BTreeSet::from([type_cat])),
                (var_name_type.into(), BTreeSet::from([type_name])),
                (Vertex::Label(LABEL_CAT), BTreeSet::from([type_cat])),
                (Vertex::Label(LABEL_NAME), BTreeSet::from([type_name])),
            ]),
            edges: vec![
                expected_edge(&constraints[0], var_animal.into(), var_animal_type.into(), vec![(type_cat, type_cat)]),
                expected_edge(
                    &constraints[2],
                    var_name.into(),
                    var_name_type.into(),
                    vec![(type_catname, type_name), (type_dogname, type_name)],
                ),
                expected_edge(&constraints[4], var_animal.into(), var_name.into(), vec![(type_cat, type_catname)]),
            ],
            expressions: Vec::new(),
            nested_disjunctions: Vec::new(),
        };

        let snapshot = storage.clone().open_snapshot_write();
        let empty_function_cache = EmptyAnnotatedFunctionSignatures;
        let context = TypeGraphSeedingContext::new(
            &snapshot,
            &type_manager,
            &empty_function_cache,
            &translation_context.variable_registry,
            TypeInferenceMode::ConcreteSubtypesOnly,
        );
        let graph = context.create_graph(&VertexAnnotations::new(), conjunction).unwrap();
        assert_eq!(expected_graph, graph);
    }

    #[test]
    fn test_comparison() {
        let (_tmp_dir, storage) = setup_storage();
        let (type_manager, thing_manager) = managers();

        let (_, (type_name, type_catname, type_dogname), _) =
            setup_types(storage.clone().open_snapshot_write(), &type_manager, &thing_manager);

        let label_owner = Label::build("owner", None);
        let (type_owner, type_age) = {
            let mut snapshot = storage.clone().open_snapshot_write();
            let type_owner = type_manager.create_entity_type(&mut snapshot, &label_owner).unwrap();
            let type_age = type_manager.create_attribute_type(&mut snapshot, &Label::build("age", None)).unwrap();
            type_age.set_value_type(&mut snapshot, &type_manager, &thing_manager, ValueType::Integer).unwrap();
            type_owner
                .set_owns(
                    &mut snapshot,
                    &type_manager,
                    &thing_manager,
                    type_age,
                    Ordering::Unordered,
                    StorageCounters::DISABLED,
                )
                .unwrap();
            type_owner
                .set_owns(
                    &mut snapshot,
                    &type_manager,
                    &thing_manager,
                    type_catname.as_attribute_type(),
                    Ordering::Unordered,
                    StorageCounters::DISABLED,
                )
                .unwrap();
            type_owner
                .set_owns(
                    &mut snapshot,
                    &type_manager,
                    &thing_manager,
                    type_dogname.as_attribute_type(),
                    Ordering::Unordered,
                    StorageCounters::DISABLED,
                )
                .unwrap();
            snapshot.commit(&mut CommitProfile::DISABLED).unwrap();
            (TypeAnnotation::Entity(type_owner), TypeAnnotation::Attribute(type_age))
        };

        {
            // Case 1: $x isa! owner, has $a; $a > $b;
            let mut translation_context = PipelineTranslationContext::new();
            let mut value_parameters = ParameterRegistry::new();
            let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
            let mut conjunction = builder.conjunction_mut();

            let var_x = conjunction.constraints_mut().get_or_declare_variable("x", None).unwrap();
            let var_a = conjunction.constraints_mut().get_or_declare_variable("a", None).unwrap();
            let var_b = conjunction.constraints_mut().get_or_declare_variable("b", None).unwrap();

            // Try seeding
            conjunction
                .constraints_mut()
                .add_isa(IsaKind::Exact, var_x, Vertex::Label(label_owner.clone()), None)
                .unwrap();
            conjunction.constraints_mut().add_has(var_x, var_a, None).unwrap();
            conjunction.constraints_mut().add_has(var_x, var_b, None).unwrap();
            conjunction
                .constraints_mut()
                .add_comparison(var_a.into(), var_b.into(), Comparator::Greater, None)
                .unwrap();

            let block = builder.finish().unwrap();
            let conjunction = block.conjunction();

            let types_x = BTreeSet::from([type_owner]);
            let types_a = BTreeSet::from([type_age, type_catname, type_dogname]);
            let types_b = BTreeSet::from([type_age, type_catname, type_dogname]);
            let constraints = conjunction.constraints();
            let expected_graph = TypeInferenceGraph {
                conjunction,
                vertices: VertexAnnotations::from_iter([
                    (Vertex::Label(label_owner.clone()), types_x.clone()),
                    (var_x.into(), types_x),
                    (var_a.into(), types_a),
                    (var_b.into(), types_b),
                ]),
                edges: vec![
                    expected_edge(
                        &constraints[0],
                        var_x.into(),
                        Vertex::Label(label_owner),
                        vec![(type_owner, type_owner)],
                    ),
                    expected_edge(
                        &constraints[1],
                        var_x.into(),
                        var_a.into(),
                        vec![(type_owner, type_age), (type_owner, type_catname), (type_owner, type_dogname)],
                    ),
                    expected_edge(
                        &constraints[2],
                        var_x.into(),
                        var_b.into(),
                        vec![(type_owner, type_age), (type_owner, type_catname), (type_owner, type_dogname)],
                    ),
                    expected_edge(
                        &constraints[3],
                        var_a.into(),
                        var_b.into(),
                        vec![
                            (type_age, type_age),
                            (type_catname, type_catname),
                            (type_catname, type_dogname),
                            (type_dogname, type_catname),
                            (type_dogname, type_dogname),
                        ],
                    ),
                ],
                expressions: Vec::new(),
                nested_disjunctions: Vec::new(),
            };

            let snapshot = storage.clone().open_snapshot_write();
            let empty_function_cache = EmptyAnnotatedFunctionSignatures;
            let context = TypeGraphSeedingContext::new(
                &snapshot,
                &type_manager,
                &empty_function_cache,
                &translation_context.variable_registry,
                TypeInferenceMode::ConcreteSubtypesOnly,
            );
            let graph = context.create_graph(&VertexAnnotations::new(), conjunction).unwrap();
            assert_eq!(expected_graph.vertices, graph.vertices);
            assert_eq!(expected_graph.edges, graph.edges);
        }

        {
            // Case 1: $x isa! $t; $x == 5;
            let mut translation_context = PipelineTranslationContext::new();
            let mut value_parameters = ParameterRegistry::new();
            let mut builder = Block::builder(translation_context.new_block_builder_context(&mut value_parameters));
            let mut conjunction = builder.conjunction_mut();

            let var_x = conjunction.constraints_mut().get_or_declare_variable("x", None).unwrap();
            let var_t = conjunction.constraints_mut().get_or_declare_variable("t", None).unwrap();
            let parameter_5 = Vertex::Parameter(ParameterID::Value(
                0,
                ir::pattern::ValueType::Builtin(ValueType::Integer),
                typeql::common::Span { begin_offset: 0, end_offset: 0 },
            ));
            // Try seeding
            conjunction.constraints_mut().add_isa(IsaKind::Exact, var_x, Vertex::Variable(var_t), None).unwrap();
            conjunction.constraints_mut().add_comparison(var_x.into(), parameter_5, Comparator::Equal, None).unwrap();

            let block = builder.finish().unwrap();
            let conjunction = block.conjunction();

            let types_x = BTreeSet::from([type_age, type_catname, type_dogname]);
            let types_t = BTreeSet::from([type_name, type_age, type_catname, type_dogname]);
            let constraints = conjunction.constraints();
            let expected_graph = TypeInferenceGraph {
                conjunction,
                vertices: VertexAnnotations::from_iter([(var_x.into(), types_x), (var_t.into(), types_t)]),
                edges: vec![expected_edge(
                    &constraints[0],
                    var_x.into(),
                    var_t.into(),
                    vec![(type_age, type_age), (type_catname, type_catname), (type_dogname, type_dogname)],
                )],
                expressions: Vec::new(),
                nested_disjunctions: Vec::new(),
            };

            let snapshot = storage.clone().open_snapshot_write();
            let empty_function_cache = EmptyAnnotatedFunctionSignatures;
            let context = TypeGraphSeedingContext::new(
                &snapshot,
                &type_manager,
                &empty_function_cache,
                &translation_context.variable_registry,
                TypeInferenceMode::ConcreteSubtypesOnly,
            );
            let graph = context.create_graph(&VertexAnnotations::new(), conjunction).unwrap();
            assert_eq!(expected_graph.vertices, graph.vertices);
            assert_eq!(expected_graph.edges, graph.edges);
        }
    }
}
