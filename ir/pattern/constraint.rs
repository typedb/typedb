/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{hash_map, HashMap},
    fmt,
    hash::{DefaultHasher, Hash, Hasher},
    iter, mem,
    ops::Deref,
};

use answer::variable::Variable;
use itertools::Itertools;
use structural_equality::StructuralEquality;
use typeql::common::Span;

use crate::{
    pattern::{
        conjunction::Conjunction,
        expression::{ExpressionRepresentationError, ExpressionTree},
        function_call::FunctionCall,
        variable_category::VariableCategory,
        IrID, ParameterID, ScopeId, ValueType, VariableDependency, Vertex,
    },
    pipeline::{block::BlockBuilderContext, function_signature::FunctionSignature, ParameterRegistry},
    LiteralParseError, RepresentationError,
};

#[derive(Debug, Clone)]
pub struct Constraints {
    scope: ScopeId,
    constraints: Vec<Constraint<Variable>>,
}

impl Deref for Constraints {
    type Target = [Constraint<Variable>];
    fn deref(&self) -> &Self::Target {
        self.constraints.as_slice()
    }
}

impl Constraints {
    pub(crate) fn new(scope: ScopeId) -> Self {
        Self { scope, constraints: Vec::new() }
    }

    pub(crate) fn scope(&self) -> ScopeId {
        self.scope
    }

    pub fn constraints(&self) -> &[Constraint<Variable>] {
        &self.constraints
    }

    pub fn constraints_mut(&mut self) -> &mut Vec<Constraint<Variable>> {
        &mut self.constraints
    }

    fn add_constraint(&mut self, constraint: impl Into<Constraint<Variable>>) -> &Constraint<Variable> {
        let constraint = constraint.into();
        self.constraints.push(constraint);
        self.constraints.last().unwrap()
    }

    pub(crate) fn variable_dependency(&self) -> HashMap<Variable, VariableDependency<'_>> {
        self.constraints().iter().fold(HashMap::new(), |mut acc, constraint| {
            for var in constraint.produced_ids() {
                match acc.entry(var) {
                    hash_map::Entry::Occupied(mut entry) => {
                        *entry.get_mut() &= VariableDependency::producing(constraint);
                    }
                    hash_map::Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert(VariableDependency::producing(constraint));
                    }
                }
            }
            for var in constraint.required_ids() {
                match acc.entry(var) {
                    hash_map::Entry::Occupied(mut entry) => {
                        *entry.get_mut() &= VariableDependency::required(constraint);
                    }
                    hash_map::Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert(VariableDependency::required(constraint));
                    }
                }
            }
            acc
        })
    }
}

impl StructuralEquality for Constraints {
    fn hash(&self) -> u64 {
        StructuralEquality::hash(self.constraints())
    }

    fn equals(&self, other: &Self) -> bool {
        self.constraints().equals(other.constraints())
    }
}

impl fmt::Display for Constraints {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for constraint in &self.constraints {
            let indent = " ".repeat(f.width().unwrap_or(0));
            writeln!(f, "{}{}", indent, constraint)?
        }
        Ok(())
    }
}

pub struct ConstraintsBuilder<'cx, 'reg> {
    context: &'cx mut BlockBuilderContext<'reg>,
    constraints: &'cx mut Constraints,
}

impl<'cx, 'reg> ConstraintsBuilder<'cx, 'reg> {
    pub fn new(context: &'cx mut BlockBuilderContext<'reg>, constraints: &'cx mut Constraints) -> Self {
        Self { context, constraints }
    }

    pub fn add_label(
        &mut self,
        variable: Variable,
        label: encoding::value::label::Label, // contains a span already!
    ) -> Result<&Label<Variable>, Box<RepresentationError>> {
        debug_assert!(self.context.is_variable_available(self.constraints.scope, variable));
        let type_ = Label::new(variable, label);
        self.context.set_variable_category(variable, VariableCategory::Type, type_.clone().into())?;
        let as_ref = self.constraints.add_constraint(type_);
        Ok(as_ref.as_label().unwrap())
    }

    pub fn add_role_name(
        &mut self,
        variable: Variable,
        name: &str,
        source_span: Option<Span>,
    ) -> Result<&RoleName<Variable>, Box<RepresentationError>> {
        debug_assert!(self.context.is_variable_available(self.constraints.scope, variable));
        let role_name = RoleName::new(variable, name.to_owned(), source_span);
        self.context.set_variable_category(variable, VariableCategory::RoleType, role_name.clone().into())?;
        let as_ref = self.constraints.add_constraint(role_name);
        Ok(as_ref.as_role_name().unwrap())
    }

    pub(crate) fn add_kind(
        &mut self,
        kind: typeql::token::Kind,
        variable: Variable,
        source_span: Option<Span>,
    ) -> Result<&Kind<Variable>, Box<RepresentationError>> {
        debug_assert!(self.context.is_variable_available(self.constraints.scope, variable));
        let category = match kind {
            typeql::token::Kind::Entity => VariableCategory::ThingType,
            typeql::token::Kind::Relation => VariableCategory::ThingType,
            typeql::token::Kind::Attribute => VariableCategory::ThingType,
            typeql::token::Kind::Role => VariableCategory::RoleType,
        };
        let kind = Kind::new(kind, variable, source_span);
        self.context.set_variable_category(variable, category, kind.clone().into())?;
        let as_ref = self.constraints.add_constraint(kind);
        Ok(as_ref.as_kind().unwrap())
    }

    pub fn add_sub(
        &mut self,
        kind: SubKind,
        subtype: Vertex<Variable>,
        supertype: Vertex<Variable>,
        source_span: Option<Span>,
    ) -> Result<&Sub<Variable>, Box<RepresentationError>> {
        let subtype_var = subtype.as_variable();
        let supertype_var = supertype.as_variable();
        let sub = Sub::new(kind, subtype, supertype, source_span);

        if let Some(subtype) = subtype_var {
            debug_assert!(self.context.is_variable_available(self.constraints.scope, subtype));
            self.context.set_variable_category(subtype, VariableCategory::Type, sub.clone().into())?;
        };

        if let Some(supertype) = supertype_var {
            debug_assert!(self.context.is_variable_available(self.constraints.scope, supertype));
            self.context.set_variable_category(supertype, VariableCategory::Type, sub.clone().into())?;
        };

        let as_ref = self.constraints.add_constraint(sub);
        Ok(as_ref.as_sub().unwrap())
    }

    pub fn add_is(
        &mut self,
        left: Variable,
        right: Variable,
        source_span: Option<Span>,
    ) -> Result<&Is<Variable>, Box<RepresentationError>> {
        let is = Is::new(left, right, source_span);
        let constraint = self.constraints.add_constraint(is);
        Ok(constraint.as_is().unwrap())
    }

    pub fn add_isa(
        &mut self,
        kind: IsaKind,
        thing: Variable,
        type_: Vertex<Variable>,
        source_span: Option<Span>,
    ) -> Result<&Isa<Variable>, Box<RepresentationError>> {
        let type_var = type_.as_variable();
        let isa = Isa::new(kind, thing, type_, source_span);

        debug_assert!(self.context.is_variable_available(self.constraints.scope, thing));
        self.context.set_variable_category(thing, VariableCategory::Thing, isa.clone().into())?;

        if let Some(type_) = type_var {
            debug_assert!(self.context.is_variable_available(self.constraints.scope, type_));
            self.context.set_variable_category(type_, VariableCategory::ThingType, isa.clone().into())?;
        };

        let constraint = self.constraints.add_constraint(isa);
        Ok(constraint.as_isa().unwrap())
    }

    pub fn add_iid(
        &mut self,
        var: Variable,
        iid: ParameterID,
        source_span: Option<Span>,
    ) -> Result<&Iid<Variable>, Box<RepresentationError>> {
        let iid = Iid::new(var, iid, source_span);

        debug_assert!(self.context.is_variable_available(self.constraints.scope, var));
        self.context.set_variable_category(var, VariableCategory::Thing, iid.clone().into())?;

        let constraint = self.constraints.add_constraint(iid);
        Ok(constraint.as_iid().unwrap())
    }

    pub fn add_has(
        &mut self,
        owner: Variable,
        attribute: Variable,
        source_span: Option<Span>,
    ) -> Result<&Has<Variable>, Box<RepresentationError>> {
        let has = Has::new(owner, attribute, source_span);

        debug_assert!(self.context.is_variable_available(self.constraints.scope, owner));
        self.context.set_variable_category(owner, VariableCategory::Object, has.clone().into())?;

        debug_assert!(self.context.is_variable_available(self.constraints.scope, attribute));
        self.context.set_variable_category(attribute, VariableCategory::Attribute, has.clone().into())?;

        let constraint = self.constraints.add_constraint(has);
        Ok(constraint.as_has().unwrap())
    }

    pub fn add_links(
        &mut self,
        relation: Variable,
        player: Variable,
        role_type: Variable,
        source_span: Option<Span>,
    ) -> Result<&Links<Variable>, Box<RepresentationError>> {
        let links = Constraint::from(Links::new(relation, player, role_type, source_span));

        debug_assert!(
            self.context.is_variable_available(self.constraints.scope, relation)
                && self.context.is_variable_available(self.constraints.scope, player)
                && self.context.is_variable_available(self.constraints.scope, role_type)
        );

        self.context.set_variable_category(relation, VariableCategory::Object, links.clone())?;
        self.context.set_variable_category(player, VariableCategory::Object, links.clone())?;

        self.context.set_variable_category(role_type, VariableCategory::RoleType, links.clone())?;

        let constraint = self.constraints.add_constraint(links);
        Ok(constraint.as_links().unwrap())
    }

    pub fn as_links_deduplication(
        &mut self,
        links1: Links<Variable>,
        links2: Links<Variable>,
    ) -> Result<&LinksDeduplication<Variable>, Box<RepresentationError>> {
        debug_assert!(
            self.context.is_variable_available(self.constraints.scope, links1.role_type.as_variable().unwrap())
                && self.context.is_variable_available(self.constraints.scope, links1.player.as_variable().unwrap())
                && self.context.is_variable_available(self.constraints.scope, links2.role_type.as_variable().unwrap())
                && self.context.is_variable_available(self.constraints.scope, links2.player.as_variable().unwrap())
        );
        let dedup = Constraint::from(LinksDeduplication::new(links1, links2));
        let constraint = self.constraints.add_constraint(dedup);
        Ok(constraint.as_links_deduplication().unwrap())
    }

    pub fn add_comparison(
        &mut self,
        lhs: Vertex<Variable>,
        rhs: Vertex<Variable>,
        comparator: Comparator,
        source_span: Option<Span>,
    ) -> Result<&Comparison<Variable>, Box<RepresentationError>> {
        comparator.validate_arguments(&lhs, &rhs, self.parameters(), source_span)?;
        let comparison = Comparison::new(lhs.clone(), rhs.clone(), comparator, source_span);
        if let Vertex::Variable(lhs_var) = lhs {
            debug_assert!(self.context.is_variable_available(self.constraints.scope, lhs_var));
            self.context.set_variable_category(
                lhs_var,
                VariableCategory::AttributeOrValue,
                comparison.clone().into(),
            )?;
        }
        if let Vertex::Variable(rhs_var) = rhs {
            debug_assert!(self.context.is_variable_available(self.constraints.scope, rhs_var));
            self.context.set_variable_category(
                rhs_var,
                VariableCategory::AttributeOrValue,
                comparison.clone().into(),
            )?;
        }

        let as_ref = self.constraints.add_constraint(comparison);
        Ok(as_ref.as_comparison().unwrap())
    }

    pub fn add_function_binding(
        &mut self,
        assigned: Vec<Variable>,
        callee_signature: &FunctionSignature,
        arguments: Vec<Variable>,
        function_name: &str,
        source_span: Option<Span>,
    ) -> Result<&FunctionCallBinding<Variable>, Box<RepresentationError>> {
        let function_call =
            self.create_function_call(&assigned, callee_signature, arguments, function_name, source_span)?;
        let binding = FunctionCallBinding::new(assigned, function_call, callee_signature.return_is_stream, source_span);
        for (index, var) in binding.ids_assigned().enumerate() {
            self.context.set_variable_category(var, callee_signature.returns[index].0, binding.clone().into())?;
        }
        for (callee_arg_index, caller_var) in binding.function_call.argument_ids().enumerate() {
            self.context.set_variable_category(
                caller_var,
                callee_signature.arguments[callee_arg_index],
                binding.clone().into(),
            )?;
        }
        let as_ref = self.constraints.add_constraint(binding);
        Ok(as_ref.as_function_call_binding().unwrap())
    }

    fn create_function_call(
        &mut self,
        assigned: &[Variable],
        callee_signature: &FunctionSignature,
        arguments: Vec<Variable>,
        function_name: &str,
        source_span: Option<Span>,
    ) -> Result<FunctionCall<Variable>, Box<RepresentationError>> {
        use RepresentationError::{FunctionCallArgumentCountMismatch, FunctionCallReturnCountMismatch};
        debug_assert!(assigned.iter().all(|var| self.context.is_variable_available(self.constraints.scope, *var)));
        debug_assert!(arguments.iter().all(|var| self.context.is_variable_available(self.constraints.scope, *var)));

        // Validate
        if assigned.len() != callee_signature.returns.len() {
            Err(FunctionCallReturnCountMismatch {
                name: function_name.to_string(),
                assigned_var_count: assigned.len(),
                function_return_count: callee_signature.returns.len(),
                source_span,
            })?
        }
        if arguments.len() != callee_signature.arguments.len() {
            Err(FunctionCallArgumentCountMismatch {
                name: function_name.to_string(),
                expected: callee_signature.arguments.len(),
                actual: arguments.len(),
                source_span,
            })?
        }

        Ok(FunctionCall::new(callee_signature.function_id.clone(), arguments))
    }

    pub fn add_assignment(
        &mut self,
        variable: Variable,
        expression: ExpressionTree<Variable>,
        source_span: Option<Span>,
    ) -> Result<&ExpressionBinding<Variable>, Box<RepresentationError>> {
        debug_assert!(self.context.is_variable_available(self.constraints.scope, variable));
        let binding = ExpressionBinding::new(variable, expression, source_span);
        binding.validate(self.context).map_err(|typedb_source| RepresentationError::ExpressionRepresentationError {
            typedb_source,
            source_span,
        })?;

        let binding = Constraint::from(binding);
        // WARNING: we don't know if the expression will produce a Value, a ValueList, or a ThingList! We will know this at compilation time
        // assume Value for now
        self.context.set_variable_category(variable, VariableCategory::Value, binding.clone())?;

        let as_ref = self.constraints.add_constraint(binding);
        Ok(as_ref.as_expression_binding().unwrap())
    }

    pub fn add_owns(
        &mut self,
        owner_type: Vertex<Variable>,
        attribute_type: Vertex<Variable>,
        source_span: Option<Span>,
    ) -> Result<&Owns<Variable>, Box<RepresentationError>> {
        let owner_type_var = owner_type.as_variable();
        let attribute_type_var = attribute_type.as_variable();
        let owns = Constraint::from(Owns::new(owner_type, attribute_type, source_span));

        if let Some(owner_type) = owner_type_var {
            debug_assert!(self.context.is_variable_available(self.constraints.scope, owner_type));
            self.context.set_variable_category(owner_type, VariableCategory::ThingType, owns.clone())?;
        };

        if let Some(attribute_type) = attribute_type_var {
            debug_assert!(self.context.is_variable_available(self.constraints.scope, attribute_type));
            self.context.set_variable_category(attribute_type, VariableCategory::AttributeType, owns.clone())?;
        };

        let constraint = self.constraints.add_constraint(owns);
        Ok(constraint.as_owns().unwrap())
    }

    pub fn add_relates(
        &mut self,
        relation_type: Vertex<Variable>,
        role_type: Vertex<Variable>,
        source_span: Option<Span>,
    ) -> Result<&Relates<Variable>, Box<RepresentationError>> {
        let relation_type_var = relation_type.as_variable();
        let role_type_var = role_type.as_variable();
        let relates = Constraint::from(Relates::new(relation_type, role_type, source_span));

        if let Some(relation_type) = relation_type_var {
            debug_assert!(self.context.is_variable_available(self.constraints.scope, relation_type));
            self.context.set_variable_category(relation_type, VariableCategory::ThingType, relates.clone())?;
        };

        if let Some(role_type) = role_type_var {
            debug_assert!(self.context.is_variable_available(self.constraints.scope, role_type));
            self.context.set_variable_category(role_type, VariableCategory::RoleType, relates.clone())?;
        };

        let constraint = self.constraints.add_constraint(relates);
        Ok(constraint.as_relates().unwrap())
    }

    pub fn add_plays(
        &mut self,
        player_type: Vertex<Variable>,
        role_type: Vertex<Variable>,
        source_span: Option<Span>,
    ) -> Result<&Plays<Variable>, Box<RepresentationError>> {
        let player_type_var = player_type.as_variable();
        let role_type_var = role_type.as_variable();
        let plays = Constraint::from(Plays::new(player_type, role_type, source_span));

        if let Some(player_type) = player_type_var {
            debug_assert!(self.context.is_variable_available(self.constraints.scope, player_type));
            self.context.set_variable_category(player_type, VariableCategory::ThingType, plays.clone())?;
        };

        if let Some(role_type) = role_type_var {
            debug_assert!(self.context.is_variable_available(self.constraints.scope, role_type));
            self.context.set_variable_category(role_type, VariableCategory::RoleType, plays.clone())?;
        };

        let constraint = self.constraints.add_constraint(plays);
        Ok(constraint.as_plays().unwrap())
    }

    pub fn add_value(
        &mut self,
        attribute_type: Vertex<Variable>,
        value_type: ValueType,
        source_span: Option<Span>,
    ) -> Result<&Value<Variable>, Box<RepresentationError>> {
        let attribute_type_var = attribute_type.as_variable();
        let value = Constraint::from(Value::new(attribute_type, value_type, source_span));

        if let Some(attribute_type) = attribute_type_var {
            debug_assert!(self.context.is_variable_available(self.constraints.scope, attribute_type));
            self.context.set_variable_category(attribute_type, VariableCategory::AttributeType, value.clone())?;
        };

        let constraint = self.constraints.add_constraint(value);
        Ok(constraint.as_value().unwrap())
    }

    pub fn create_anonymous_variable(
        &mut self,
        source_span: Option<Span>,
    ) -> Result<Variable, Box<RepresentationError>> {
        self.context.create_anonymous_variable(self.constraints.scope, source_span)
    }

    pub fn get_or_declare_variable(
        &mut self,
        name: &str,
        source_span: Option<Span>,
    ) -> Result<Variable, Box<RepresentationError>> {
        self.context.get_or_declare_variable(name, self.constraints.scope, source_span)
    }

    pub(crate) fn parameters(&mut self) -> &mut ParameterRegistry {
        self.context.parameters()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Constraint<ID> {
    Is(Is<ID>),
    Kind(Kind<ID>),
    Label(Label<ID>),
    RoleName(RoleName<ID>),
    Sub(Sub<ID>),
    Isa(Isa<ID>),
    Iid(Iid<ID>),
    Links(Links<ID>),
    IndexedRelation(IndexedRelation<ID>),
    Has(Has<ID>),
    ExpressionBinding(ExpressionBinding<ID>),
    FunctionCallBinding(FunctionCallBinding<ID>),
    Comparison(Comparison<ID>),
    Owns(Owns<ID>),
    Relates(Relates<ID>),
    Plays(Plays<ID>),
    Value(Value<ID>),
    LinksDeduplication(LinksDeduplication<ID>),
    Unsatisfiable(Unsatisfiable),
}

impl<ID: IrID> Constraint<ID> {
    pub fn name(&self) -> &str {
        match self {
            Constraint::Is(_) => typeql::token::Keyword::Is.as_str(),
            Constraint::Kind(kind) => kind.kind.as_str(),
            Constraint::Label(_) => typeql::token::Keyword::Label.as_str(),
            Constraint::Sub(_) => typeql::token::Keyword::Sub.as_str(),
            Constraint::Isa(_) => typeql::token::Keyword::Isa.as_str(),
            Constraint::Iid(_) => typeql::token::Keyword::IID.as_str(),
            Constraint::Links(_) => typeql::token::Keyword::Links.as_str(),
            Constraint::IndexedRelation(_) => "indexed-relation",
            Constraint::Has(_) => typeql::token::Keyword::Has.as_str(),
            Constraint::ExpressionBinding(_) => "expression",
            Constraint::FunctionCallBinding(_) => "function-call",
            Constraint::Comparison(comp) => comp.comparator.name(),
            Constraint::Owns(_) => typeql::token::Keyword::Owns.as_str(),
            Constraint::Relates(_) => typeql::token::Keyword::Relates.as_str(),
            Constraint::Plays(_) => typeql::token::Keyword::Plays.as_str(),
            Constraint::Value(_) => typeql::token::Keyword::Value.as_str(),

            Constraint::RoleName(_) => "role-name",
            Constraint::LinksDeduplication(_) => "links-deduplication",
            Constraint::Unsatisfiable(_) => "optimised-away",
        }
    }

    pub fn ids(&self) -> Box<dyn Iterator<Item = ID> + '_> {
        match self {
            Constraint::Is(is) => Box::new(is.ids()),
            Constraint::Kind(kind) => Box::new(kind.ids()),
            Constraint::Label(label) => Box::new(label.ids()),
            Constraint::RoleName(role_name) => Box::new(role_name.ids()),
            Constraint::Sub(sub) => Box::new(sub.ids()),
            Constraint::Isa(isa) => Box::new(isa.ids()),
            Constraint::Iid(iid) => Box::new(iid.ids()),
            Constraint::Links(rp) => Box::new(rp.ids()),
            Constraint::IndexedRelation(indexed) => Box::new(indexed.ids()),
            Constraint::Has(has) => Box::new(has.ids()),
            Constraint::ExpressionBinding(binding) => Box::new(binding.ids()),
            Constraint::FunctionCallBinding(binding) => Box::new(binding.ids()),
            Constraint::Comparison(comparison) => Box::new(comparison.ids()),
            Constraint::Owns(owns) => Box::new(owns.ids()),
            Constraint::Relates(relates) => Box::new(relates.ids()),
            Constraint::Plays(plays) => Box::new(plays.ids()),
            Constraint::Value(value) => Box::new(value.ids()),
            Constraint::LinksDeduplication(dedup) => Box::new(dedup.ids()),
            Constraint::Unsatisfiable(inner) => Box::new(inner.ids()),
        }
    }

    pub fn produced_ids(&self) -> Box<dyn Iterator<Item = ID> + '_> {
        match self {
            Constraint::Is(is) => Box::new(is.ids()),
            Constraint::Kind(kind) => Box::new(kind.ids()),
            Constraint::Label(label) => Box::new(label.ids()),
            Constraint::RoleName(role_name) => Box::new(role_name.ids()),
            Constraint::Sub(sub) => Box::new(sub.ids()),
            Constraint::Isa(isa) => Box::new(isa.ids()),
            Constraint::Iid(iid) => Box::new(iid.ids()),
            Constraint::Links(rp) => Box::new(rp.ids()),
            Constraint::IndexedRelation(indexed) => Box::new(indexed.ids()),
            Constraint::Has(has) => Box::new(has.ids()),
            Constraint::ExpressionBinding(binding) => Box::new(binding.ids_assigned()),
            Constraint::FunctionCallBinding(binding) => Box::new(binding.ids_assigned()),
            Constraint::Comparison(_) => Box::new(iter::empty()),
            Constraint::Owns(owns) => Box::new(owns.ids()),
            Constraint::Relates(relates) => Box::new(relates.ids()),
            Constraint::Plays(plays) => Box::new(plays.ids()),
            Constraint::Value(value) => Box::new(value.ids()),
            Constraint::LinksDeduplication(_) => Box::new(iter::empty()),
            Constraint::Unsatisfiable(inner) => Box::new(inner.ids()),
        }
    }

    pub fn required_ids(&self) -> Box<dyn Iterator<Item = ID> + '_> {
        match self {
            Constraint::Is(is) => Box::new(is.ids()), // FIXME _technically_ it's legal to only have one side of `is` bound
            | Constraint::Kind(_)
            | Constraint::Label(_)
            | Constraint::RoleName(_)
            | Constraint::Sub(_)
            | Constraint::Isa(_)
            | Constraint::Iid(_)
            | Constraint::Links(_)
            | Constraint::IndexedRelation(_)
            | Constraint::Has(_)
            | Constraint::Owns(_)
            | Constraint::Relates(_)
            | Constraint::Plays(_)
            | Constraint::Value(_)
            | Constraint::LinksDeduplication(_)
            | Constraint::Unsatisfiable(_) => Box::new(iter::empty()),
            Constraint::ExpressionBinding(binding) => Box::new(binding.required_ids()),
            Constraint::FunctionCallBinding(binding) => Box::new(binding.required_ids()),
            Constraint::Comparison(comparison) => Box::new(comparison.ids()),
        }
    }

    pub fn vertices(&self) -> Box<dyn Iterator<Item = &Vertex<ID>> + '_> {
        match self {
            Constraint::Is(is) => Box::new(is.vertices()),
            Constraint::Kind(kind) => Box::new(kind.vertices()),
            Constraint::Label(label) => Box::new(label.vertices()),
            Constraint::RoleName(role_name) => Box::new(role_name.vertices()),
            Constraint::Sub(sub) => Box::new(sub.vertices()),
            Constraint::Isa(isa) => Box::new(isa.vertices()),
            Constraint::Iid(iid) => Box::new(iid.vertices()),
            Constraint::Links(rp) => Box::new(rp.vertices()),
            Constraint::IndexedRelation(indexed) => Box::new(indexed.vertices()),
            Constraint::Has(has) => Box::new(has.vertices()),
            Constraint::ExpressionBinding(binding) => Box::new(binding.vertices_assigned()),
            Constraint::FunctionCallBinding(binding) => Box::new(binding.vertices_assigned()),
            Constraint::Comparison(comparison) => Box::new(comparison.vertices()),
            Constraint::Owns(owns) => Box::new(owns.vertices()),
            Constraint::Relates(relates) => Box::new(relates.vertices()),
            Constraint::Plays(plays) => Box::new(plays.vertices()),
            Constraint::Value(value) => Box::new(value.vertices()),
            Constraint::LinksDeduplication(dedup) => Box::new(dedup.vertices()),
            Constraint::Unsatisfiable(inner) => Box::new(inner.vertices()),
        }
    }

    pub fn ids_foreach<F>(&self, function: F)
    where
        F: FnMut(ID),
    {
        match self {
            Self::Is(is) => is.ids_foreach(function),
            Self::Kind(kind) => kind.ids_foreach(function),
            Self::Label(label) => label.ids_foreach(function),
            Self::RoleName(role_name) => role_name.ids_foreach(function),
            Self::Sub(sub) => sub.ids_foreach(function),
            Self::Isa(isa) => isa.ids_foreach(function),
            Self::Iid(iid) => iid.ids_foreach(function),
            Self::Links(rp) => rp.ids_foreach(function),
            Self::IndexedRelation(indexed) => indexed.ids_foreach(function),
            Self::Has(has) => has.ids_foreach(function),
            Self::ExpressionBinding(binding) => binding.ids_foreach(function),
            Self::FunctionCallBinding(binding) => binding.ids_foreach(function),
            Self::Comparison(comparison) => comparison.ids_foreach(function),
            Self::Owns(owns) => owns.ids_foreach(function),
            Self::Relates(relates) => relates.ids_foreach(function),
            Self::Plays(plays) => plays.ids_foreach(function),
            Self::Value(value) => value.ids_foreach(function),
            Self::LinksDeduplication(dedup) => dedup.ids_foreach(function),
            Self::Unsatisfiable(inner) => inner.ids_foreach(function),
        }
    }

    pub fn map<T: Clone + Ord>(self, mapping: &HashMap<ID, T>) -> Constraint<T> {
        match self {
            Self::Is(inner) => Constraint::Is(inner.map(mapping)),
            Self::Kind(inner) => Constraint::Kind(inner.map(mapping)),
            Self::Label(inner) => Constraint::Label(inner.map(mapping)),
            Self::RoleName(inner) => Constraint::RoleName(inner.map(mapping)),
            Self::Sub(inner) => Constraint::Sub(inner.map(mapping)),
            Self::Isa(inner) => Constraint::Isa(inner.map(mapping)),
            Self::Iid(inner) => Constraint::Iid(inner.map(mapping)),
            Self::Links(inner) => Constraint::Links(inner.map(mapping)),
            Self::IndexedRelation(inner) => Constraint::IndexedRelation(inner.map(mapping)),
            Self::Has(inner) => Constraint::Has(inner.map(mapping)),
            Self::ExpressionBinding(inner) => Constraint::ExpressionBinding(inner.map(mapping)),
            Self::FunctionCallBinding(inner) => Constraint::FunctionCallBinding(inner.map(mapping)),
            Self::Comparison(inner) => Constraint::Comparison(inner.map(mapping)),
            Self::Owns(inner) => Constraint::Owns(inner.map(mapping)),
            Self::Relates(inner) => Constraint::Relates(inner.map(mapping)),
            Self::Plays(inner) => Constraint::Plays(inner.map(mapping)),
            Self::Value(inner) => Constraint::Value(inner.map(mapping)),
            Self::LinksDeduplication(inner) => Constraint::LinksDeduplication(inner.map(mapping)),
            Self::Unsatisfiable(inner) => Constraint::Unsatisfiable(inner.map(mapping)),
        }
    }

    pub(crate) fn source_span(&self) -> Option<Span> {
        match self {
            Constraint::Is(inner) => inner.source_span(),
            Constraint::Kind(inner) => None,
            Constraint::Label(inner) => inner.source_span(),
            Constraint::RoleName(inner) => inner.source_span(),
            Constraint::Sub(inner) => inner.source_span(),
            Constraint::Isa(inner) => inner.source_span(),
            Constraint::Iid(inner) => inner.source_span(),
            Constraint::Links(inner) => inner.source_span(),
            Constraint::IndexedRelation(inner) => inner.source_span(),
            Constraint::Has(inner) => inner.source_span(),
            Constraint::ExpressionBinding(inner) => inner.source_span(),
            Constraint::FunctionCallBinding(inner) => inner.source_span(),
            Constraint::Comparison(inner) => inner.source_span(),
            Constraint::Owns(inner) => inner.source_span(),
            Constraint::Relates(inner) => inner.source_span(),
            Constraint::Plays(inner) => inner.source_span(),
            Constraint::Value(inner) => inner.source_span(),
            Constraint::LinksDeduplication(inner) => None,
            Constraint::Unsatisfiable(inner) => None,
        }
    }

    pub(crate) fn as_kind(&self) -> Option<&Kind<ID>> {
        match self {
            Constraint::Kind(kind) => Some(kind),
            _ => None,
        }
    }

    pub(crate) fn as_label(&self) -> Option<&Label<ID>> {
        match self {
            Constraint::Label(label) => Some(label),
            _ => None,
        }
    }

    pub(crate) fn as_role_name(&self) -> Option<&RoleName<ID>> {
        match self {
            Constraint::RoleName(role_name) => Some(role_name),
            _ => None,
        }
    }

    pub(crate) fn as_sub(&self) -> Option<&Sub<ID>> {
        match self {
            Constraint::Sub(sub) => Some(sub),
            _ => None,
        }
    }

    pub(crate) fn as_is(&self) -> Option<&Is<ID>> {
        match self {
            Constraint::Is(is) => Some(is),
            _ => None,
        }
    }

    pub(crate) fn as_isa(&self) -> Option<&Isa<ID>> {
        match self {
            Constraint::Isa(isa) => Some(isa),
            _ => None,
        }
    }

    pub fn as_iid(&self) -> Option<&Iid<ID>> {
        match self {
            Constraint::Iid(iid) => Some(iid),
            _ => None,
        }
    }

    pub fn as_links(&self) -> Option<&Links<ID>> {
        match self {
            Constraint::Links(rp) => Some(rp),
            _ => None,
        }
    }

    pub(crate) fn as_links_deduplication(&self) -> Option<&LinksDeduplication<ID>> {
        match self {
            Constraint::LinksDeduplication(dedup) => Some(dedup),
            _ => None,
        }
    }

    pub fn as_indexed_relation(&self) -> Option<&IndexedRelation<ID>> {
        match self {
            Constraint::IndexedRelation(indexed_relation) => Some(indexed_relation),
            _ => None,
        }
    }

    pub fn as_has(&self) -> Option<&Has<ID>> {
        match self {
            Constraint::Has(has) => Some(has),
            _ => None,
        }
    }

    pub fn as_comparison(&self) -> Option<&Comparison<ID>> {
        match self {
            Constraint::Comparison(cmp) => Some(cmp),
            _ => None,
        }
    }

    pub(crate) fn as_function_call_binding(&self) -> Option<&FunctionCallBinding<ID>> {
        match self {
            Constraint::FunctionCallBinding(binding) => Some(binding),
            _ => None,
        }
    }

    pub fn as_expression_binding(&self) -> Option<&ExpressionBinding<ID>> {
        match self {
            Constraint::ExpressionBinding(binding) => Some(binding),
            _ => None,
        }
    }

    pub(crate) fn as_owns(&self) -> Option<&Owns<ID>> {
        match self {
            Constraint::Owns(owns) => Some(owns),
            _ => None,
        }
    }

    pub fn as_relates(&self) -> Option<&Relates<ID>> {
        match self {
            Constraint::Relates(relates) => Some(relates),
            _ => None,
        }
    }

    pub fn as_plays(&self) -> Option<&Plays<ID>> {
        match self {
            Constraint::Plays(plays) => Some(plays),
            _ => None,
        }
    }

    pub(crate) fn as_value(&self) -> Option<&Value<ID>> {
        match self {
            Constraint::Value(value) => Some(value),
            _ => None,
        }
    }
}

impl<ID: StructuralEquality + Ord> StructuralEquality for Constraint<ID> {
    fn hash(&self) -> u64 {
        StructuralEquality::hash(&mem::discriminant(self))
            ^ match self {
                Self::Is(inner) => inner.hash(),
                Self::Kind(inner) => inner.hash(),
                Self::Label(inner) => inner.hash(),
                Self::RoleName(inner) => inner.hash(),
                Self::Sub(inner) => inner.hash(),
                Self::Isa(inner) => inner.hash(),
                Self::Iid(inner) => inner.hash(),
                Self::Links(inner) => inner.hash(),
                Self::IndexedRelation(inner) => inner.hash(),
                Self::Has(inner) => inner.hash(),
                Self::ExpressionBinding(inner) => inner.hash(),
                Self::FunctionCallBinding(inner) => inner.hash(),
                Self::Comparison(inner) => inner.hash(),
                Self::Owns(inner) => inner.hash(),
                Self::Relates(inner) => inner.hash(),
                Self::Plays(inner) => inner.hash(),
                Self::Value(inner) => inner.hash(),
                Self::LinksDeduplication(inner) => inner.hash(),
                Self::Unsatisfiable(inner) => StructuralEquality::hash(&inner),
            }
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Is(inner), Self::Is(other_inner)) => inner.equals(other_inner),
            (Self::Kind(inner), Self::Kind(other_inner)) => inner.equals(other_inner),
            (Self::Label(inner), Self::Label(other_inner)) => inner.equals(other_inner),
            (Self::RoleName(inner), Self::RoleName(other_inner)) => inner.equals(other_inner),
            (Self::Sub(inner), Self::Sub(other_inner)) => inner.equals(other_inner),
            (Self::Isa(inner), Self::Isa(other_inner)) => inner.equals(other_inner),
            (Self::Iid(inner), Self::Iid(other_inner)) => inner.equals(other_inner),
            (Self::Links(inner), Self::Links(other_inner)) => inner.equals(other_inner),
            (Self::IndexedRelation(inner), Self::IndexedRelation(other_inner)) => inner.equals(other_inner),
            (Self::Has(inner), Self::Has(other_inner)) => inner.equals(other_inner),
            (Self::ExpressionBinding(inner), Self::ExpressionBinding(other_inner)) => inner.equals(other_inner),
            (Self::FunctionCallBinding(inner), Self::FunctionCallBinding(other_inner)) => inner.equals(other_inner),
            (Self::Comparison(inner), Self::Comparison(other_inner)) => inner.equals(other_inner),
            (Self::Owns(inner), Self::Owns(other_inner)) => inner.equals(other_inner),
            (Self::Relates(inner), Self::Relates(other_inner)) => inner.equals(other_inner),
            (Self::Plays(inner), Self::Plays(other_inner)) => inner.equals(other_inner),
            (Self::Value(inner), Self::Value(other_inner)) => inner.equals(other_inner),
            (Self::LinksDeduplication(inner), Self::LinksDeduplication(other_inner)) => inner.equals(other_inner),
            (Self::Unsatisfiable(inner), Self::Unsatisfiable(other_inner)) => inner.equals(other_inner),
            // note: this style forces updating the match when the variants change
            (Self::Is { .. }, _)
            | (Self::Kind { .. }, _)
            | (Self::Label { .. }, _)
            | (Self::RoleName { .. }, _)
            | (Self::Sub { .. }, _)
            | (Self::Isa { .. }, _)
            | (Self::Iid { .. }, _)
            | (Self::Links { .. }, _)
            | (Self::IndexedRelation { .. }, _)
            | (Self::Has { .. }, _)
            | (Self::ExpressionBinding { .. }, _)
            | (Self::FunctionCallBinding { .. }, _)
            | (Self::Comparison { .. }, _)
            | (Self::Owns { .. }, _)
            | (Self::Relates { .. }, _)
            | (Self::Plays { .. }, _)
            | (Self::Value { .. }, _)
            | (Self::LinksDeduplication { .. }, _)
            | (Self::Unsatisfiable(_), _) => false,
        }
    }
}

impl<ID: IrID> fmt::Display for Constraint<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Is(constraint) => fmt::Display::fmt(constraint, f),
            Self::Kind(constraint) => fmt::Display::fmt(constraint, f),
            Self::Label(constraint) => fmt::Display::fmt(constraint, f),
            Self::RoleName(constraint) => fmt::Display::fmt(constraint, f),
            Self::Sub(constraint) => fmt::Display::fmt(constraint, f),
            Self::Isa(constraint) => fmt::Display::fmt(constraint, f),
            Self::Iid(constraint) => fmt::Display::fmt(constraint, f),
            Self::Links(constraint) => fmt::Display::fmt(constraint, f),
            Self::IndexedRelation(constraint) => fmt::Display::fmt(constraint, f),
            Self::Has(constraint) => fmt::Display::fmt(constraint, f),
            Self::ExpressionBinding(constraint) => fmt::Display::fmt(constraint, f),
            Self::FunctionCallBinding(constraint) => fmt::Display::fmt(constraint, f),
            Self::Comparison(constraint) => fmt::Display::fmt(constraint, f),
            Self::Owns(constraint) => fmt::Display::fmt(constraint, f),
            Self::Relates(constraint) => fmt::Display::fmt(constraint, f),
            Self::Plays(constraint) => fmt::Display::fmt(constraint, f),
            Self::Value(constraint) => fmt::Display::fmt(constraint, f),
            Self::LinksDeduplication(constraint) => fmt::Display::fmt(constraint, f),
            Self::Unsatisfiable(constraint) => fmt::Display::fmt(constraint, f),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Label<ID> {
    type_var: Vertex<ID>,
    type_label: Vertex<ID>,
    source_span: Option<Span>,
}

impl<ID> Label<ID> {
    fn new(left: ID, type_label: encoding::value::label::Label) -> Self {
        let source_span = type_label.source_span();
        Self { type_var: Vertex::Variable(left), type_label: Vertex::Label(type_label), source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> Label<ID> {
    pub fn type_(&self) -> &Vertex<ID> {
        &self.type_var
    }

    pub fn type_label(&self) -> &Vertex<ID> {
        &self.type_label
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> {
        self.type_var.as_variable().into_iter()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &Vertex<ID>> {
        [&self.type_var, &self.type_label].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.type_var.as_variable().inspect(|&id| function(id));
    }

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> Label<T> {
        Label {
            type_var: self.type_var.map(mapping),
            type_label: self.type_label.map(mapping),
            source_span: self.source_span,
        }
    }
}

impl<ID: IrID> From<Label<ID>> for Constraint<ID> {
    fn from(val: Label<ID>) -> Self {
        Constraint::Label(val)
    }
}

impl<ID: Hash> Hash for Label<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.type_var, state);
        Hash::hash(&self.type_label, state);
    }
}
impl<ID: PartialEq> PartialEq for Label<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.type_var.eq(&other.type_var) && self.type_label.eq(&other.type_label)
    }
}

impl<ID: PartialEq> Eq for Label<ID> {}

impl<ID: StructuralEquality> StructuralEquality for Label<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.type_var.hash_into(&mut hasher);
        self.type_label.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.type_var.equals(&other.type_var) && self.type_label.equals(&other.type_label)
    }
}

impl<ID: IrID> fmt::Display for Label<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} label {}", self.type_var, self.type_label)
    }
}

#[derive(Debug, Clone)]
pub struct RoleName<ID> {
    left: Vertex<ID>,
    name: String,
    source_span: Option<Span>,
}

impl<ID> RoleName<ID> {
    pub fn new(left: ID, name: String, source_span: Option<Span>) -> Self {
        Self { left: Vertex::Variable(left), name, source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> RoleName<ID> {
    pub fn type_(&self) -> &Vertex<ID> {
        &self.left
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> + Sized {
        self.left.as_variable().into_iter()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &Vertex<ID>> + Sized {
        [&self.left].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.left.as_variable().inspect(|&id| function(id));
    }

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> RoleName<T> {
        RoleName { left: self.left.map(mapping), name: self.name, source_span: self.source_span }
    }
}

impl<ID: IrID> From<RoleName<ID>> for Constraint<ID> {
    fn from(val: RoleName<ID>) -> Self {
        Constraint::RoleName(val)
    }
}

impl<ID: Hash> Hash for RoleName<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.left, state);
        Hash::hash(&self.name, state);
    }
}
impl<ID: PartialEq> PartialEq for RoleName<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.left.eq(&other.left) && self.name.eq(&other.name)
    }
}

impl<ID: PartialEq> Eq for RoleName<ID> {}

impl<ID: StructuralEquality> StructuralEquality for RoleName<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.left.hash_into(&mut hasher);
        hasher.write_u64(StructuralEquality::hash(self.name.as_str()));
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.left.equals(&other.left) && self.name.as_str().equals(other.name.as_str())
    }
}

impl<ID: IrID> fmt::Display for RoleName<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} role-name {}", self.left, &self.name)
    }
}
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct Kind<ID> {
    kind: typeql::token::Kind,
    type_: Vertex<ID>,
    source_span: Option<Span>,
}

impl<ID> Kind<ID> {
    fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> Kind<ID> {
    pub fn new(kind: typeql::token::Kind, type_: ID, source_span: Option<Span>) -> Self {
        Self { kind, type_: Vertex::Variable(type_), source_span }
    }

    pub fn type_(&self) -> &Vertex<ID> {
        &self.type_
    }

    pub fn kind(&self) -> typeql::token::Kind {
        self.kind
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> + Sized {
        self.type_.as_variable().into_iter()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &Vertex<ID>> + Sized {
        [&self.type_].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.type_.as_variable().inspect(|&id| function(id));
    }

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> Kind<T> {
        Kind { kind: self.kind, type_: self.type_.map(mapping), source_span: self.source_span }
    }
}

impl<ID: IrID> From<Kind<ID>> for Constraint<ID> {
    fn from(kind: Kind<ID>) -> Self {
        Constraint::Kind(kind)
    }
}

impl<ID: StructuralEquality> StructuralEquality for Kind<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(StructuralEquality::hash(self.kind.as_str()));
        hasher.write_u64(StructuralEquality::hash(&self.type_));
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.kind.as_str().equals(other.kind.as_str()) && self.type_.equals(&other.type_)
    }
}

impl<ID: IrID> fmt::Display for Kind<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl<ID: fmt::Debug> fmt::Debug for Kind<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.kind, self.type_)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum SubKind {
    Exact,
    Subtype,
}

impl From<typeql::statement::type_::SubKind> for SubKind {
    fn from(kind: typeql::statement::type_::SubKind) -> Self {
        match kind {
            typeql::statement::type_::SubKind::Direct => Self::Exact,
            typeql::statement::type_::SubKind::Transitive => Self::Subtype,
        }
    }
}

impl StructuralEquality for SubKind {
    fn hash(&self) -> u64 {
        StructuralEquality::hash(&mem::discriminant(self))
    }

    fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

impl fmt::Display for SubKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exact => write!(f, "!"),
            // This is not a great Display implementation since there is no symbol to read this variant
            Self::Subtype => write!(f, ""),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Sub<ID> {
    kind: SubKind,
    subtype: Vertex<ID>,
    supertype: Vertex<ID>,
    source_span: Option<Span>,
}

impl<ID> Sub<ID> {
    fn new(kind: SubKind, subtype: Vertex<ID>, supertype: Vertex<ID>, source_span: Option<Span>) -> Self {
        Sub { subtype, supertype, kind, source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> Sub<ID> {
    pub fn subtype(&self) -> &Vertex<ID> {
        &self.subtype
    }

    pub fn supertype(&self) -> &Vertex<ID> {
        &self.supertype
    }

    pub fn sub_kind(&self) -> SubKind {
        self.kind
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> + Sized {
        [self.subtype.as_variable(), self.supertype.as_variable()].into_iter().flatten()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &Vertex<ID>> + Sized {
        [&self.subtype, &self.supertype].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.subtype.as_variable().inspect(|&id| function(id));
        self.supertype.as_variable().inspect(|&id| function(id));
    }

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> Sub<T> {
        Sub::new(self.kind, self.subtype.map(mapping), self.supertype.map(mapping), self.source_span)
    }
}

impl<ID: IrID> From<Sub<ID>> for Constraint<ID> {
    fn from(val: Sub<ID>) -> Self {
        Constraint::Sub(val)
    }
}

impl<ID: Hash> Hash for Sub<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.kind, state);
        Hash::hash(&self.subtype, state);
        Hash::hash(&self.supertype, state);
    }
}

impl<ID: PartialEq> Eq for Sub<ID> {}

impl<ID: PartialEq> PartialEq for Sub<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.kind.eq(&other.kind) && self.subtype.eq(&other.subtype) && self.supertype.eq(&other.supertype)
    }
}

impl<ID: StructuralEquality> StructuralEquality for Sub<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(StructuralEquality::hash(&self.kind));
        hasher.write_u64(StructuralEquality::hash(&self.subtype));
        hasher.write_u64(StructuralEquality::hash(&self.supertype));
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.kind.equals(&other.kind) && self.subtype.equals(&other.subtype) && self.supertype.equals(&other.supertype)
    }
}

impl<ID: IrID> fmt::Display for Sub<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} sub {}", self.subtype, self.supertype)
    }
}

#[derive(Debug, Clone)]
pub struct Is<ID> {
    lhs: Vertex<ID>,
    rhs: Vertex<ID>,
    source_span: Option<Span>,
}

impl<ID> Is<ID> {
    fn new(lhs: ID, rhs: ID, source_span: Option<Span>) -> Self {
        Self { lhs: Vertex::Variable(lhs), rhs: Vertex::Variable(rhs), source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> Is<ID> {
    pub fn lhs(&self) -> &Vertex<ID> {
        &self.lhs
    }

    pub fn rhs(&self) -> &Vertex<ID> {
        &self.rhs
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> + Sized {
        [self.lhs.as_variable(), self.rhs.as_variable()].into_iter().flatten()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &Vertex<ID>> + Sized {
        [&self.lhs, &self.rhs].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.lhs.as_variable().inspect(|&id| function(id));
        self.rhs.as_variable().inspect(|&id| function(id));
    }

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> Is<T> {
        Is { lhs: self.lhs.map(mapping), rhs: self.rhs.map(mapping), source_span: self.source_span }
    }
}

impl<ID: IrID> From<Is<ID>> for Constraint<ID> {
    fn from(val: Is<ID>) -> Self {
        Constraint::Is(val)
    }
}

impl<ID: Hash> Hash for Is<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.lhs, state);
        Hash::hash(&self.rhs, state);
    }
}

impl<ID: PartialEq> Eq for Is<ID> {}

impl<ID: PartialEq> PartialEq for Is<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.lhs.eq(&other.lhs) && self.rhs.eq(&other.rhs)
    }
}

impl<ID: StructuralEquality> StructuralEquality for Is<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.lhs.hash_into(&mut hasher);
        self.rhs.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.lhs.equals(&other.lhs) && self.rhs.equals(&other.rhs)
    }
}

impl<ID: IrID> fmt::Display for Is<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} is {}", self.lhs, self.rhs)
    }
}

#[derive(Debug, Clone)]
pub struct Isa<ID> {
    kind: IsaKind,
    thing: Vertex<ID>,
    type_: Vertex<ID>,
    source_span: Option<Span>,
}

impl<ID> Isa<ID> {
    fn new(kind: IsaKind, thing: ID, type_: Vertex<ID>, source_span: Option<Span>) -> Self {
        Self { kind, thing: Vertex::Variable(thing), type_, source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> Isa<ID> {
    pub fn thing(&self) -> &Vertex<ID> {
        &self.thing
    }

    pub fn type_(&self) -> &Vertex<ID> {
        &self.type_
    }

    pub fn isa_kind(&self) -> IsaKind {
        self.kind
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> + Sized {
        [self.thing.as_variable(), self.type_.as_variable()].into_iter().flatten()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &Vertex<ID>> + Sized {
        [&self.thing, &self.type_].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.thing.as_variable().inspect(|&id| function(id));
        self.type_.as_variable().inspect(|&id| function(id));
    }

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> Isa<T> {
        Isa {
            kind: self.kind,
            thing: self.thing.map(mapping),
            type_: self.type_.map(mapping),
            source_span: self.source_span,
        }
    }
}

impl<ID: IrID> From<Isa<ID>> for Constraint<ID> {
    fn from(val: Isa<ID>) -> Self {
        Constraint::Isa(val)
    }
}

impl<ID: Hash> Hash for Isa<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.kind, state);
        Hash::hash(&self.thing, state);
        Hash::hash(&self.type_, state);
    }
}

impl<ID: PartialEq> Eq for Isa<ID> {}

impl<ID: PartialEq> PartialEq for Isa<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.kind.eq(&other.kind) && self.thing.eq(&other.thing) && self.type_.eq(&other.type_)
    }
}

impl<ID: StructuralEquality> StructuralEquality for Isa<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.kind.hash_into(&mut hasher);
        self.thing.hash_into(&mut hasher);
        self.type_.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.kind.equals(&other.kind) && self.thing.equals(&other.thing) && self.type_.equals(&other.type_)
    }
}

impl<ID: IrID> fmt::Display for Isa<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}{} {}", self.thing, typeql::token::Keyword::Isa, self.kind, self.type_)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum IsaKind {
    Exact,
    Subtype,
}

impl From<typeql::statement::thing::isa::IsaKind> for IsaKind {
    fn from(kind: typeql::statement::thing::isa::IsaKind) -> Self {
        match kind {
            typeql::statement::thing::isa::IsaKind::Exact => Self::Exact,
            typeql::statement::thing::isa::IsaKind::Subtype => Self::Subtype,
        }
    }
}

impl StructuralEquality for IsaKind {
    fn hash(&self) -> u64 {
        StructuralEquality::hash(&mem::discriminant(self))
    }

    fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

impl fmt::Display for IsaKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exact => write!(f, "!"),
            // This is not a great Display implementation since there is no symbol to read this variant
            Self::Subtype => write!(f, ""),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Iid<ID> {
    var: Vertex<ID>,
    iid: Vertex<ID>,
    source_span: Option<Span>,
}

impl<ID> Iid<ID> {
    pub fn new(var: ID, iid: ParameterID, source_span: Option<Span>) -> Self {
        Self { var: Vertex::Variable(var), iid: Vertex::Parameter(iid), source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> Iid<ID> {
    pub fn var(&self) -> &Vertex<ID> {
        &self.var
    }

    pub fn iid(&self) -> &Vertex<ID> {
        &self.iid
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> + Sized {
        self.var.as_variable().into_iter()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &Vertex<ID>> + Sized {
        [&self.var, &self.iid].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.var.as_variable().inspect(|&id| function(id));
    }

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> Iid<T> {
        Iid { var: self.var.map(mapping), iid: self.iid.map(mapping), source_span: self.source_span }
    }
}

impl<ID: IrID> From<Iid<ID>> for Constraint<ID> {
    fn from(val: Iid<ID>) -> Self {
        Constraint::Iid(val)
    }
}

impl<ID: Hash> Hash for Iid<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.var, state);
        Hash::hash(&self.iid, state);
    }
}

impl<ID: PartialEq> Eq for Iid<ID> {}

impl<ID: PartialEq> PartialEq for Iid<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.var.eq(&other.var) && self.iid.eq(&other.iid)
    }
}

impl<ID: StructuralEquality> StructuralEquality for Iid<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.var.hash_into(&mut hasher);
        self.iid.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.var.equals(&other.var) && self.iid.equals(&other.iid)
    }
}

impl<ID: IrID> fmt::Display for Iid<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} iid {}", self.var, self.iid)
    }
}
#[derive(Debug, Clone)]
pub struct Links<ID> {
    relation: Vertex<ID>,
    player: Vertex<ID>,
    role_type: Vertex<ID>,
    source_span: Option<Span>,
}

impl<ID> Links<ID> {
    pub fn new(relation: ID, player: ID, role_type: ID, source_span: Option<Span>) -> Self {
        Self {
            relation: Vertex::Variable(relation),
            player: Vertex::Variable(player),
            role_type: Vertex::Variable(role_type),
            source_span,
        }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> Links<ID> {
    pub fn relation(&self) -> &Vertex<ID> {
        &self.relation
    }

    pub fn player(&self) -> &Vertex<ID> {
        &self.player
    }

    pub fn role_type(&self) -> &Vertex<ID> {
        &self.role_type
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> {
        [&self.relation, &self.player, &self.role_type].map(Vertex::as_variable).into_iter().flatten()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &Vertex<ID>> {
        [&self.relation, &self.player, &self.role_type].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.relation.as_variable().inspect(|&id| function(id));
        self.player.as_variable().inspect(|&id| function(id));
        self.role_type.as_variable().inspect(|&id| function(id));
    }

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> Links<T> {
        Links {
            relation: self.relation.map(mapping),
            player: self.player.map(mapping),
            role_type: self.role_type.map(mapping),
            source_span: self.source_span,
        }
    }
}

impl<ID: IrID> From<Links<ID>> for Constraint<ID> {
    fn from(links: Links<ID>) -> Self {
        Constraint::Links(links)
    }
}

impl<ID: Hash> Hash for Links<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.relation, state);
        Hash::hash(&self.player, state);
        Hash::hash(&self.role_type, state);
    }
}

impl<ID: PartialEq> Eq for Links<ID> {}

impl<ID: PartialEq> PartialEq for Links<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.relation.eq(&other.relation) && self.player.eq(&other.player) && self.role_type.eq(&other.role_type)
    }
}

impl<ID: StructuralEquality> StructuralEquality for Links<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.relation.hash_into(&mut hasher);
        self.player.hash_into(&mut hasher);
        self.role_type.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.relation.equals(&other.relation)
            && self.player.equals(&other.player)
            && self.role_type.equals(&other.role_type)
    }
}

impl<ID: IrID> fmt::Display for Links<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} links {} (role: {})", self.relation, self.player, self.role_type)
    }
}

#[derive(Debug, Clone)]
pub struct IndexedRelation<ID> {
    player_1: Vertex<ID>,
    player_2: Vertex<ID>,
    relation: Vertex<ID>,
    role_type_1: Vertex<ID>,
    role_type_2: Vertex<ID>,
    source_span: Option<Span>,
}

impl<ID> IndexedRelation<ID> {
    pub fn new(
        player_1: ID,
        player_2: ID,
        relation: ID,
        role_type_1: ID,
        role_type_2: ID,
        source_span: Option<Span>,
    ) -> Self {
        Self {
            player_1: Vertex::Variable(player_1),
            player_2: Vertex::Variable(player_2),
            relation: Vertex::Variable(relation),
            role_type_1: Vertex::Variable(role_type_1),
            role_type_2: Vertex::Variable(role_type_2),
            source_span,
        }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> IndexedRelation<ID> {
    pub fn player_1(&self) -> &Vertex<ID> {
        &self.player_1
    }

    pub fn player_2(&self) -> &Vertex<ID> {
        &self.player_2
    }

    pub fn relation(&self) -> &Vertex<ID> {
        &self.relation
    }

    pub fn role_type_1(&self) -> &Vertex<ID> {
        &self.role_type_1
    }

    pub fn role_type_2(&self) -> &Vertex<ID> {
        &self.role_type_2
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> {
        [&self.relation, &self.player_1, &self.player_2, &self.role_type_1, &self.role_type_2]
            .map(Vertex::as_variable)
            .into_iter()
            .flatten()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &Vertex<ID>> {
        [&self.relation, &self.player_1, &self.player_2, &self.role_type_1, &self.role_type_2].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.player_1.as_variable().inspect(|&id| function(id));
        self.player_2.as_variable().inspect(|&id| function(id));
        self.relation.as_variable().inspect(|&id| function(id));
        self.role_type_1.as_variable().inspect(|&id| function(id));
        self.role_type_2.as_variable().inspect(|&id| function(id));
    }

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> IndexedRelation<T> {
        IndexedRelation {
            player_1: self.player_1.map(mapping),
            player_2: self.player_2.map(mapping),
            relation: self.relation.map(mapping),
            role_type_1: self.role_type_1.map(mapping),
            role_type_2: self.role_type_2.map(mapping),
            source_span: self.source_span,
        }
    }
}

impl<ID: Hash> Hash for IndexedRelation<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.player_1, state);
        Hash::hash(&self.player_2, state);
        Hash::hash(&self.relation, state);
        Hash::hash(&self.role_type_1, state);
        Hash::hash(&self.role_type_2, state);
    }
}

impl<ID: PartialEq> PartialEq for IndexedRelation<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.player_1.eq(&other.player_1)
            && self.player_2.eq(&other.player_2)
            && self.relation.eq(&other.relation)
            && self.role_type_1.eq(&other.role_type_1)
            && self.role_type_2.eq(&other.role_type_2)
    }
}

impl<ID: Eq> Eq for IndexedRelation<ID> {}

impl<ID: StructuralEquality> StructuralEquality for IndexedRelation<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.player_1.hash_into(&mut hasher);
        self.player_2.hash_into(&mut hasher);
        self.relation.hash_into(&mut hasher);
        self.role_type_1.hash_into(&mut hasher);
        self.role_type_2.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        // TODO: should we care about which one is P1 and which one is P2?
        self.player_1.equals(&other.player_1)
            && self.player_2.equals(&other.player_2)
            && self.relation.equals(&other.relation)
            && self.role_type_1.equals(&other.role_type_1)
            && self.role_type_2.equals(&other.role_type_2)
    }
}

impl<ID: IrID> fmt::Display for IndexedRelation<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} connected-to {} via {} using role {} and role {}",
            self.player_1, self.player_2, self.relation, self.role_type_1, self.role_type_2
        )
    }
}

impl<ID: IrID> From<IndexedRelation<ID>> for Constraint<ID> {
    fn from(val: IndexedRelation<ID>) -> Self {
        Constraint::IndexedRelation(val)
    }
}

#[derive(Debug, Clone)]
pub struct Has<ID> {
    owner: Vertex<ID>,
    attribute: Vertex<ID>,
    source_span: Option<Span>,
}

impl<ID> Has<ID> {
    pub fn new(owner: ID, attribute: ID, source_span: Option<Span>) -> Self {
        Has { owner: Vertex::Variable(owner), attribute: Vertex::Variable(attribute), source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> Has<ID> {
    pub fn owner(&self) -> &Vertex<ID> {
        &self.owner
    }

    pub fn attribute(&self) -> &Vertex<ID> {
        &self.attribute
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> {
        [&self.owner, &self.attribute].map(Vertex::as_variable).into_iter().flatten()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &Vertex<ID>> {
        [&self.owner, &self.attribute].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.owner.as_variable().inspect(|&id| function(id));
        self.attribute.as_variable().inspect(|&id| function(id));
    }

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> Has<T> {
        Has { owner: self.owner.map(mapping), attribute: self.attribute.map(mapping), source_span: self.source_span }
    }
}

impl<ID: IrID> From<Has<ID>> for Constraint<ID> {
    fn from(has: Has<ID>) -> Self {
        Constraint::Has(has)
    }
}

impl<ID: Hash> Hash for Has<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.owner, state);
        Hash::hash(&self.attribute, state);
    }
}
impl<ID: PartialEq> PartialEq for Has<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.owner.eq(&other.owner) && self.attribute.eq(&other.attribute)
    }
}

impl<ID: PartialEq> Eq for Has<ID> {}

impl<ID: StructuralEquality> StructuralEquality for Has<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.owner.hash_into(&mut hasher);
        self.attribute.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.owner.equals(&other.owner) && self.attribute.equals(&other.attribute)
    }
}

impl<ID: IrID> fmt::Display for Has<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} has {}", self.owner, self.attribute)
    }
}

#[derive(Debug, Clone)]
pub struct ExpressionBinding<ID> {
    left: Vertex<ID>,
    expression: ExpressionTree<ID>,
    source_span: Option<Span>,
}

impl<ID> ExpressionBinding<ID> {
    fn new(left: ID, expression: ExpressionTree<ID>, source_span: Option<Span>) -> Self {
        Self { left: Vertex::Variable(left), expression, source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> ExpressionBinding<ID> {
    pub fn left(&self) -> &Vertex<ID> {
        &self.left
    }

    pub fn expression(&self) -> &ExpressionTree<ID> {
        &self.expression
    }

    pub fn vertices_assigned(&self) -> impl Iterator<Item = &Vertex<ID>> {
        [&self.left].into_iter()
    }

    pub fn required_ids(&self) -> impl Iterator<Item = ID> + '_ {
        self.expression.variables()
    }

    pub fn ids_assigned(&self) -> impl Iterator<Item = ID> {
        self.left.as_variable().into_iter()
    }

    pub(crate) fn ids(&self) -> impl Iterator<Item = ID> + '_ {
        self.ids_assigned().chain(self.expression().variables())
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.ids_assigned().for_each(&mut function);
        self.expression().variables().for_each(function);
    }

    pub(crate) fn validate(&self, context: &mut BlockBuilderContext<'_>) -> Result<(), ExpressionRepresentationError> {
        if self.expression().is_empty() {
            Err(ExpressionRepresentationError::EmptyExpressionTree {})
        } else {
            Ok(())
        }
    }

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> ExpressionBinding<T> {
        ExpressionBinding {
            left: self.left.map(mapping),
            expression: self.expression.map(mapping),
            source_span: self.source_span,
        }
    }
}

impl<ID: IrID> From<ExpressionBinding<ID>> for Constraint<ID> {
    fn from(val: ExpressionBinding<ID>) -> Self {
        Constraint::ExpressionBinding(val)
    }
}

impl<ID: Hash> Hash for ExpressionBinding<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.left, state);
        Hash::hash(&self.expression, state);
    }
}

impl<ID: PartialEq> PartialEq for ExpressionBinding<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.left.eq(&other.left) && self.expression.eq(&other.expression)
    }
}

impl<ID: PartialEq> Eq for ExpressionBinding<ID> {}

impl<ID: StructuralEquality> StructuralEquality for ExpressionBinding<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.left.hash_into(&mut hasher);
        self.expression.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.left.equals(&other.left) && self.expression.equals(&other.expression)
    }
}

impl<ID: IrID> fmt::Display for ExpressionBinding<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.left, self.expression)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct FunctionCallBinding<ID> {
    assigned: Vec<Vertex<ID>>,
    function_call: FunctionCall<ID>,
    is_stream: bool,
    source_span: Option<Span>,
}

impl<ID> FunctionCallBinding<ID> {
    fn new(left: Vec<ID>, function_call: FunctionCall<ID>, is_stream: bool, source_span: Option<Span>) -> Self {
        Self { assigned: left.into_iter().map(Vertex::Variable).collect(), function_call, is_stream, source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> FunctionCallBinding<ID> {
    pub fn assigned(&self) -> &[Vertex<ID>] {
        &self.assigned
    }

    pub fn function_call(&self) -> &FunctionCall<ID> {
        &self.function_call
    }

    pub fn is_stream(&self) -> bool {
        self.is_stream
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> + '_ {
        self.ids_assigned().chain(self.function_call.argument_ids())
    }

    pub fn required_ids(&self) -> impl Iterator<Item = ID> + '_ {
        self.function_call.argument_ids()
    }

    pub fn ids_assigned(&self) -> impl Iterator<Item = ID> + '_ {
        self.assigned.iter().filter_map(Vertex::as_variable)
    }

    pub fn vertices_assigned(&self) -> impl Iterator<Item = &Vertex<ID>> + '_ {
        self.assigned.iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.ids_assigned().for_each(|id| function(id));
        self.function_call.argument_ids().for_each(|id| function(id));
    }
    pub fn map<T: Clone + Ord>(self, mapping: &HashMap<ID, T>) -> FunctionCallBinding<T> {
        FunctionCallBinding {
            assigned: self.assigned.into_iter().map(|v| v.map(mapping)).collect(),
            function_call: self.function_call.map(mapping),
            is_stream: self.is_stream,
            source_span: self.source_span,
        }
    }
}

impl<ID: IrID> From<FunctionCallBinding<ID>> for Constraint<ID> {
    fn from(val: FunctionCallBinding<ID>) -> Self {
        Constraint::FunctionCallBinding(val)
    }
}

impl<ID: StructuralEquality + Ord> StructuralEquality for FunctionCallBinding<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.assigned.hash_into(&mut hasher);
        self.function_call.hash_into(&mut hasher);
        self.is_stream.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.assigned.equals(&other.assigned)
            && self.function_call.equals(&other.function_call)
            && self.is_stream.equals(&other.is_stream)
    }
}

impl<ID: IrID> fmt::Display for FunctionCallBinding<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_stream {
            write!(f, "{} in {}", self.ids_assigned().map(|i| i.to_string()).join(", "), self.function_call())
        } else {
            write!(f, "{} = {}", self.ids_assigned().map(|i| i.to_string()).join(", "), self.function_call())
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum Comparator {
    Equal,
    NotEqual,
    Less,
    Greater,
    LessOrEqual,
    GreaterOrEqual,
    Like,
    Contains,
}

impl Comparator {
    pub fn name(&self) -> &str {
        match self {
            Comparator::Equal => typeql::token::Comparator::Eq.as_str(),
            Comparator::NotEqual => typeql::token::Comparator::Neq.as_str(),
            Comparator::Less => typeql::token::Comparator::Lt.as_str(),
            Comparator::Greater => typeql::token::Comparator::Gt.as_str(),
            Comparator::LessOrEqual => typeql::token::Comparator::Lte.as_str(),
            Comparator::GreaterOrEqual => typeql::token::Comparator::Gte.as_str(),
            Comparator::Like => typeql::token::Comparator::Like.as_str(),
            Comparator::Contains => typeql::token::Comparator::Contains.as_str(),
        }
    }

    pub(crate) fn validate_arguments(
        &self,
        lhs: &Vertex<Variable>,
        rhs: &Vertex<Variable>,
        parameters: &mut ParameterRegistry,
        source_span: Option<Span>,
    ) -> Result<(), Box<RepresentationError>> {
        match self {
            Comparator::Equal
            | Comparator::NotEqual
            | Comparator::Less
            | Comparator::Greater
            | Comparator::LessOrEqual
            | Comparator::GreaterOrEqual
            | Comparator::Contains => Ok(()),
            Comparator::Like => match rhs.as_parameter().and_then(|pid| parameters.value(pid)) {
                Some(encoding::value::value::Value::String(value)) => {
                    regex::Regex::new(value).map(|_| ()).map_err(|source| {
                        Box::new(RepresentationError::RegexFailedCompilation {
                            value: (**value).to_owned(),
                            source,
                            source_span,
                        })
                    })
                }
                _ => Err(Box::new(RepresentationError::RegexExpectedStringLiteral { source_span })),
            },
        }
    }
}

impl TryFrom<typeql::token::Comparator> for Comparator {
    // TODO: Revert to From<> when we've implemented them all
    type Error = LiteralParseError;

    fn try_from(token: typeql::token::Comparator) -> Result<Self, Self::Error> {
        Ok(match token {
            typeql::token::Comparator::Eq => Self::Equal,
            typeql::token::Comparator::EqLegacy => Self::Equal,
            typeql::token::Comparator::Neq => Self::NotEqual,
            typeql::token::Comparator::Gt => Self::Greater,
            typeql::token::Comparator::Gte => Self::GreaterOrEqual,
            typeql::token::Comparator::Lt => Self::Less,
            typeql::token::Comparator::Lte => Self::LessOrEqual,
            typeql::token::Comparator::Contains => Self::Contains,
            typeql::token::Comparator::Like => Self::Like,
        })
    }
}

impl StructuralEquality for Comparator {
    fn hash(&self) -> u64 {
        StructuralEquality::hash(&mem::discriminant(self))
    }

    fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

impl fmt::Display for Comparator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Equal => write!(f, "{}", typeql::token::Comparator::Eq),
            Self::NotEqual => write!(f, "{}", typeql::token::Comparator::Neq),
            Self::Less => write!(f, "{}", typeql::token::Comparator::Lt),
            Self::Greater => write!(f, "{}", typeql::token::Comparator::Gt),
            Self::LessOrEqual => write!(f, "{}", typeql::token::Comparator::Lte),
            Self::GreaterOrEqual => write!(f, "{}", typeql::token::Comparator::Gte),
            Self::Like => write!(f, "{}", typeql::token::Comparator::Like),
            Self::Contains => write!(f, "{}", typeql::token::Comparator::Contains),
        }
    }
}
#[derive(Debug, Clone)]
pub struct Comparison<ID> {
    lhs: Vertex<ID>,
    rhs: Vertex<ID>,
    comparator: Comparator,
    source_span: Option<Span>,
}

impl<ID> Comparison<ID> {
    pub fn new(lhs: Vertex<ID>, rhs: Vertex<ID>, comparator: Comparator, source_span: Option<Span>) -> Self {
        Self { lhs, rhs, comparator, source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> Comparison<ID> {
    pub fn lhs(&self) -> &Vertex<ID> {
        &self.lhs
    }

    pub fn rhs(&self) -> &Vertex<ID> {
        &self.rhs
    }

    pub fn comparator(&self) -> Comparator {
        self.comparator
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> {
        [self.lhs.as_variable(), self.rhs.as_variable()].into_iter().flatten()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &Vertex<ID>> {
        [&self.lhs, &self.rhs].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.lhs.as_variable().inspect(|&id| function(id));
        self.rhs.as_variable().inspect(|&id| function(id));
    }

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> Comparison<T> {
        Comparison {
            lhs: self.lhs.map(mapping),
            rhs: self.rhs.map(mapping),
            comparator: self.comparator,
            source_span: self.source_span,
        }
    }
}

impl<ID: IrID> From<Comparison<ID>> for Constraint<ID> {
    fn from(comp: Comparison<ID>) -> Self {
        Constraint::Comparison(comp)
    }
}

impl<ID: Hash> Hash for Comparison<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.lhs, state);
        Hash::hash(&self.rhs, state);
        Hash::hash(&self.comparator, state);
    }
}

impl<ID: PartialEq> PartialEq for Comparison<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.lhs.eq(&other.lhs) && self.rhs.eq(&other.rhs) && self.comparator.eq(&other.comparator)
    }
}

impl<ID: PartialEq> Eq for Comparison<ID> {}

impl<ID: StructuralEquality> StructuralEquality for Comparison<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.lhs.hash_into(&mut hasher);
        self.rhs.hash_into(&mut hasher);
        self.comparator.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.lhs.equals(&other.lhs) && self.rhs.equals(&other.rhs) && self.comparator.equals(&other.comparator)
    }
}

impl<ID: IrID> fmt::Display for Comparison<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.lhs, self.comparator, self.rhs)
    }
}

#[derive(Debug, Clone)]
pub struct Owns<ID> {
    owner: Vertex<ID>,
    attribute: Vertex<ID>,
    source_span: Option<Span>,
}

impl<ID> Owns<ID> {
    fn new(owner: Vertex<ID>, attribute: Vertex<ID>, source_span: Option<Span>) -> Self {
        Self { owner, attribute, source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> Owns<ID> {
    pub fn owner(&self) -> &Vertex<ID> {
        &self.owner
    }

    pub fn attribute(&self) -> &Vertex<ID> {
        &self.attribute
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> {
        [self.owner.as_variable(), self.attribute.as_variable()].into_iter().flatten()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &Vertex<ID>> {
        [&self.owner, &self.attribute].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.owner.as_variable().inspect(|&id| function(id));
        self.attribute.as_variable().inspect(|&id| function(id));
    }

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> Owns<T> {
        Owns { owner: self.owner.map(mapping), attribute: self.attribute.map(mapping), source_span: self.source_span }
    }
}

impl<ID: IrID> From<Owns<ID>> for Constraint<ID> {
    fn from(val: Owns<ID>) -> Self {
        Constraint::Owns(val)
    }
}

impl<ID: Hash> Hash for Owns<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.owner, state);
        Hash::hash(&self.attribute, state);
    }
}
impl<ID: PartialEq> PartialEq for Owns<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.owner.eq(&other.owner) && self.attribute.eq(&other.attribute)
    }
}

impl<ID: PartialEq> Eq for Owns<ID> {}

impl<ID: StructuralEquality> StructuralEquality for Owns<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.owner.hash_into(&mut hasher);
        self.attribute.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.owner.equals(&other.owner) && self.attribute.equals(&other.attribute)
    }
}

impl<ID: IrID> fmt::Display for Owns<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} owns {}", self.owner, self.attribute)
    }
}

#[derive(Debug, Clone)]
pub struct Relates<ID> {
    relation: Vertex<ID>,
    role_type: Vertex<ID>,
    source_span: Option<Span>,
}

impl<ID> Relates<ID> {
    fn new(relation: Vertex<ID>, role: Vertex<ID>, source_span: Option<Span>) -> Self {
        Self { relation, role_type: role, source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> Relates<ID> {
    pub fn relation(&self) -> &Vertex<ID> {
        &self.relation
    }

    pub fn role_type(&self) -> &Vertex<ID> {
        &self.role_type
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> {
        [self.relation.as_variable(), self.role_type.as_variable()].into_iter().flatten()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &Vertex<ID>> {
        [&self.relation, &self.role_type].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.relation.as_variable().inspect(|&id| function(id));
        self.role_type.as_variable().inspect(|&id| function(id));
    }

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> Relates<T> {
        Relates {
            relation: self.relation.map(mapping),
            role_type: self.role_type.map(mapping),
            source_span: self.source_span,
        }
    }
}

impl<ID: IrID> From<Relates<ID>> for Constraint<ID> {
    fn from(val: Relates<ID>) -> Self {
        Constraint::Relates(val)
    }
}

impl<ID: Hash> Hash for Relates<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.relation, state);
        Hash::hash(&self.role_type, state);
    }
}
impl<ID: PartialEq> PartialEq for Relates<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.relation.eq(&other.relation) && self.role_type.eq(&other.role_type)
    }
}

impl<ID: PartialEq> Eq for Relates<ID> {}

impl<ID: StructuralEquality> StructuralEquality for Relates<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.relation.hash_into(&mut hasher);
        self.role_type.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.relation.equals(&other.relation) && self.role_type.equals(&other.role_type)
    }
}

impl<ID: IrID> fmt::Display for Relates<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} relates {}", self.relation, self.role_type)
    }
}

#[derive(Debug, Clone)]
pub struct Plays<ID> {
    player: Vertex<ID>,
    role_type: Vertex<ID>,
    source_span: Option<Span>,
}

impl<ID> Plays<ID> {
    fn new(player: Vertex<ID>, role: Vertex<ID>, source_span: Option<Span>) -> Self {
        Self { player, role_type: role, source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> Plays<ID> {
    pub fn player(&self) -> &Vertex<ID> {
        &self.player
    }

    pub fn role_type(&self) -> &Vertex<ID> {
        &self.role_type
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> {
        [self.player.as_variable(), self.role_type.as_variable()].into_iter().flatten()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &Vertex<ID>> {
        [&self.player, &self.role_type].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.player.as_variable().inspect(|&id| function(id));
        self.role_type.as_variable().inspect(|&id| function(id));
    }

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> Plays<T> {
        Plays {
            player: self.player.map(mapping),
            role_type: self.role_type.map(mapping),
            source_span: self.source_span,
        }
    }
}

impl<ID: IrID> From<Plays<ID>> for Constraint<ID> {
    fn from(val: Plays<ID>) -> Self {
        Constraint::Plays(val)
    }
}

impl<ID: Hash> Hash for Plays<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.player, state);
        Hash::hash(&self.role_type, state);
    }
}

impl<ID: PartialEq> PartialEq for Plays<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.player.eq(&other.player) && self.role_type.eq(&other.role_type)
    }
}

impl<ID: PartialEq> Eq for Plays<ID> {}

impl<ID: StructuralEquality> StructuralEquality for Plays<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.player.hash_into(&mut hasher);
        self.role_type.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.player.equals(&other.player) && self.role_type.equals(&other.role_type)
    }
}

impl<ID: IrID> fmt::Display for Plays<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} plays {}", self.player, self.role_type)
    }
}

#[derive(Debug, Clone)]
pub struct Value<ID> {
    attribute_type: Vertex<ID>,
    value_type: ValueType,
    source_span: Option<Span>,
}

impl<ID> Value<ID> {
    fn new(attribute_type: Vertex<ID>, value_type: ValueType, source_span: Option<Span>) -> Self {
        Self { attribute_type, value_type, source_span }
    }

    pub fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl<ID: IrID> Value<ID> {
    pub fn attribute_type(&self) -> &Vertex<ID> {
        &self.attribute_type
    }

    pub fn value_type(&self) -> &ValueType {
        &self.value_type
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> {
        [self.attribute_type.as_variable()].into_iter().flatten()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &Vertex<ID>> {
        [&self.attribute_type].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.attribute_type.as_variable().inspect(|&id| function(id));
    }

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> Value<T> {
        Value {
            attribute_type: self.attribute_type.map(mapping),
            value_type: self.value_type,
            source_span: self.source_span,
        }
    }
}

impl<ID: IrID> From<Value<ID>> for Constraint<ID> {
    fn from(val: Value<ID>) -> Self {
        Constraint::Value(val)
    }
}

impl<ID: Hash> Hash for Value<ID> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.attribute_type, state);
        Hash::hash(&self.value_type, state);
    }
}

impl<ID: PartialEq> PartialEq for Value<ID> {
    fn eq(&self, other: &Self) -> bool {
        self.attribute_type.eq(&other.attribute_type) && self.value_type.eq(&other.value_type)
    }
}

impl<ID: PartialEq> Eq for Value<ID> {}

impl<ID: StructuralEquality> StructuralEquality for Value<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.attribute_type.hash_into(&mut hasher);
        self.value_type.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.attribute_type.equals(&other.attribute_type) && self.value_type.equals(&other.value_type)
    }
}

impl<ID: IrID> fmt::Display for Value<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} value {}", self.attribute_type, self.value_type)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct LinksDeduplication<ID> {
    links1: Links<ID>,
    links2: Links<ID>,
}

impl<ID: IrID> LinksDeduplication<ID> {
    pub fn new(links1: Links<ID>, links2: Links<ID>) -> Self {
        Self { links1, links2 }
    }

    pub fn links1(&self) -> &Links<ID> {
        &self.links1
    }

    pub fn links2(&self) -> &Links<ID> {
        &self.links2
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> + Sized {
        [
            self.links1.role_type.as_variable(),
            self.links1.player.as_variable(),
            self.links2.role_type.as_variable(),
            self.links2.player.as_variable(),
        ]
        .into_iter()
        .flatten()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &Vertex<ID>> + Sized {
        [&self.links1.role_type, &self.links1.player, &self.links2.role_type, &self.links2.player].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
        self.ids().for_each(|id| function(id))
    }

    pub fn map<T: Clone>(self, mapping: &HashMap<ID, T>) -> LinksDeduplication<T> {
        LinksDeduplication { links1: self.links1.map(mapping), links2: self.links2.map(mapping) }
    }
}

impl<ID: IrID> From<LinksDeduplication<ID>> for Constraint<ID> {
    fn from(val: LinksDeduplication<ID>) -> Self {
        Constraint::LinksDeduplication(val)
    }
}

impl<ID: StructuralEquality> StructuralEquality for LinksDeduplication<ID> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.links1.hash_into(&mut hasher);
        self.links2.hash_into(&mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self.links1.equals(&other.links1) && self.links2.equals(&other.links2)
    }
}

impl<ID: IrID> fmt::Display for LinksDeduplication<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LinksDeduplication({}, {}))", self.links1, self.links2)
    }
}

#[derive(Debug, Clone)]
pub struct Unsatisfiable {
    conjunction: Conjunction,
}

impl Unsatisfiable {
    pub(crate) fn new(conjunction: Conjunction) -> Unsatisfiable {
        Self { conjunction }
    }

    pub fn ids<ID: IrID>(&self) -> impl Iterator<Item = ID> {
        [].into_iter()
    }

    pub fn vertices<ID: IrID>(&self) -> impl Iterator<Item = &Vertex<ID>> {
        [].into_iter()
    }

    pub fn ids_foreach<F, ID: IrID>(&self, mut function: F)
    where
        F: FnMut(ID),
    {
    }

    pub fn map<ID: IrID, T: Clone>(self, mapping: &HashMap<ID, T>) -> Unsatisfiable {
        self
    }
}

impl PartialEq<Self> for Unsatisfiable {
    fn eq(&self, other: &Self) -> bool {
        true
    }
}

impl Eq for Unsatisfiable {}

impl Hash for Unsatisfiable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&1, state)
    }
}

impl StructuralEquality for Unsatisfiable {
    fn hash(&self) -> u64 {
        1
    }

    fn equals(&self, other: &Self) -> bool {
        true
    }
}

impl fmt::Display for Unsatisfiable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unsatisfiable")
    }
}
