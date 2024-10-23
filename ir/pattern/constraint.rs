/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt};
use std::ops::Deref;

use answer::variable::Variable;
use itertools::Itertools;

use crate::{
    pattern::{
        expression::{ExpressionDefinitionError, ExpressionTree},
        function_call::FunctionCall,
        variable_category::VariableCategory,
        IrID, ScopeId, Vertex,
    },
    pipeline::{block::BlockBuilderContext, function_signature::FunctionSignature, ParameterRegistry},
    RepresentationError,
};

#[derive(Debug, Clone)]
pub struct Constraints {
    scope: ScopeId,
    constraints: Vec<Constraint<Variable>>,

    // TODO: could also store indexes into the Constraints vec? Depends how expensive Constraints are and if we delete
    left_constrained_index: HashMap<Variable, Vec<Constraint<Variable>>>,
    right_constrained_index: HashMap<Variable, Vec<Constraint<Variable>>>,
    filter_constrained_index: HashMap<Variable, Vec<Constraint<Variable>>>,
}

impl Deref for Constraints {
    type Target = [Constraint<Variable>];
    fn deref(&self) -> &Self::Target {
        self.constraints.as_slice()
    }
}

impl Constraints {
    pub(crate) fn new(scope: ScopeId) -> Self {
        Self {
            scope,
            constraints: Vec::new(),
            left_constrained_index: HashMap::new(),
            right_constrained_index: HashMap::new(),
            filter_constrained_index: HashMap::new(),
        }
    }

    pub(crate) fn scope(&self) -> ScopeId {
        self.scope
    }

    pub fn constraints(&self) -> &[Constraint<Variable>] {
        &self.constraints
    }

    fn add_constraint(&mut self, constraint: impl Into<Constraint<Variable>>) -> &Constraint<Variable> {
        let constraint = constraint.into();
        // TODO: ids_foreach is only used here, and ids is unused. Do we need these methods?
        constraint.ids_foreach(|var, side| match side {
            ConstraintIDSide::Left => self.left_constrained_index.entry(var).or_default().push(constraint.clone()),
            ConstraintIDSide::Right => self.right_constrained_index.entry(var).or_default().push(constraint.clone()),
            ConstraintIDSide::Filter => self.filter_constrained_index.entry(var).or_default().push(constraint.clone()),
        });
        self.constraints.push(constraint);
        self.constraints.last().unwrap()
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

    pub fn add_label(&mut self, variable: Variable, type_: &str) -> Result<&Label<Variable>, RepresentationError> {
        debug_assert!(self.context.is_variable_available(self.constraints.scope, variable));
        let type_ = Label::new(variable, type_.to_string());
        self.context.set_variable_category(variable, VariableCategory::Type, type_.clone().into())?;
        let as_ref = self.constraints.add_constraint(type_);
        Ok(as_ref.as_label().unwrap())
    }

    pub fn add_role_name(
        &mut self,
        variable: Variable,
        name: &str,
    ) -> Result<&RoleName<Variable>, RepresentationError> {
        debug_assert!(self.context.is_variable_available(self.constraints.scope, variable));
        let role_name = RoleName::new(variable, name.to_owned());
        self.context.set_variable_category(variable, VariableCategory::RoleType, role_name.clone().into())?;
        let as_ref = self.constraints.add_constraint(role_name);
        Ok(as_ref.as_role_name().unwrap())
    }

    pub(crate) fn add_kind(
        &mut self,
        kind: typeql::token::Kind,
        variable: Variable,
    ) -> Result<&Kind<Variable>, RepresentationError> {
        debug_assert!(self.context.is_variable_available(self.constraints.scope, variable));
        let category = match kind {
            typeql::token::Kind::Entity => VariableCategory::ThingType,
            typeql::token::Kind::Relation => VariableCategory::ThingType,
            typeql::token::Kind::Attribute => VariableCategory::ThingType,
            typeql::token::Kind::Role => VariableCategory::RoleType,
        };
        let kind = Kind::new(kind, variable);
        self.context.set_variable_category(variable, category, kind.clone().into())?;
        let as_ref = self.constraints.add_constraint(kind);
        Ok(as_ref.as_kind().unwrap())
    }

    pub fn add_sub(
        &mut self,
        kind: SubKind,
        subtype: Vertex<Variable>,
        supertype: Vertex<Variable>,
    ) -> Result<&Sub<Variable>, RepresentationError> {
        let subtype_var = subtype.as_variable();
        let supertype_var = supertype.as_variable();
        let sub = Sub::new(kind, subtype, supertype);

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

    pub fn add_isa(
        &mut self,
        kind: IsaKind,
        thing: Variable,
        type_: Vertex<Variable>,
    ) -> Result<&Isa<Variable>, RepresentationError> {
        let type_var = type_.as_variable();
        let isa = Isa::new(kind, thing, type_);

        debug_assert!(self.context.is_variable_available(self.constraints.scope, thing));
        self.context.set_variable_category(thing, VariableCategory::Thing, isa.clone().into())?;

        if let Some(type_) = type_var {
            debug_assert!(self.context.is_variable_available(self.constraints.scope, type_));
            self.context.set_variable_category(type_, VariableCategory::ThingType, isa.clone().into())?;
        };

        let constraint = self.constraints.add_constraint(isa);
        Ok(constraint.as_isa().unwrap())
    }

    pub fn add_has(&mut self, owner: Variable, attribute: Variable) -> Result<&Has<Variable>, RepresentationError> {
        let has = Has::new(owner, attribute);

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
    ) -> Result<&Links<Variable>, RepresentationError> {
        let links = Constraint::from(Links::new(relation, player, role_type));

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

    pub fn add_comparison(
        &mut self,
        lhs: Vertex<Variable>,
        rhs: Vertex<Variable>,
        comparator: Comparator,
    ) -> Result<&Comparison<Variable>, RepresentationError> {
        let lhs_var = lhs.as_variable();
        let rhs_var = rhs.as_variable();
        let comparison = Comparison::new(lhs, rhs, comparator);

        if let Some(lhs) = lhs_var {
            debug_assert!(self.context.is_variable_available(self.constraints.scope, lhs));
            self.context.set_variable_category(lhs, VariableCategory::Value, comparison.clone().into())?;
        }

        if let Some(rhs) = rhs_var {
            debug_assert!(self.context.is_variable_available(self.constraints.scope, rhs));
            self.context.set_variable_category(rhs, VariableCategory::Value, comparison.clone().into())?;
        }

        // TODO The above lines were the two lines below
        // self.context.set_variable_category(lhs, VariableCategory::AttributeOrValue, comparison.clone().into())?;
        // self.context.set_variable_category(rhs, VariableCategory::AttributeOrValue, comparison.clone().into())?;

        let as_ref = self.constraints.add_constraint(comparison);
        Ok(as_ref.as_comparison().unwrap())
    }

    pub fn add_function_binding(
        &mut self,
        assigned: Vec<Variable>,
        callee_signature: &FunctionSignature,
        arguments: Vec<Variable>,
        function_name: &str, // for errors
    ) -> Result<&FunctionCallBinding<Variable>, RepresentationError> {
        let function_call = self.create_function_call(&assigned, callee_signature, arguments, function_name)?;
        let binding = FunctionCallBinding::new(assigned, function_call, callee_signature.return_is_stream);
        for (index, var) in binding.ids_assigned().enumerate() {
            self.context.set_variable_category(var, callee_signature.returns[index].0, binding.clone().into())?;
        }
        for (caller_var, callee_arg_index) in binding.function_call.call_id_mapping() {
            self.context.set_variable_category(
                *caller_var,
                callee_signature.arguments[*callee_arg_index],
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
        function_name: &str, // for errors
    ) -> Result<FunctionCall<Variable>, RepresentationError> {
        use RepresentationError::{FunctionCallArgumentCountMismatch, FunctionCallReturnCountMismatch};
        debug_assert!(assigned.iter().all(|var| self.context.is_variable_available(self.constraints.scope, *var)));
        debug_assert!(arguments.iter().all(|var| self.context.is_variable_available(self.constraints.scope, *var)));

        // Validate
        if assigned.len() != callee_signature.returns.len() {
            Err(FunctionCallReturnCountMismatch {
                name: function_name.to_string(),
                assigned_var_count: assigned.len(),
                function_return_count: callee_signature.returns.len(),
            })?
        }
        if arguments.len() != callee_signature.arguments.len() {
            Err(FunctionCallArgumentCountMismatch {
                name: function_name.to_string(),
                expected: callee_signature.arguments.len(),
                actual: arguments.len(),
            })?
        }

        // Construct
        let call_variable_mapping = arguments.iter().enumerate().map(|(index, variable)| (*variable, index)).collect();
        Ok(FunctionCall::new(callee_signature.function_id.clone(), call_variable_mapping))
    }

    pub fn add_assignment(
        &mut self,
        variable: Variable,
        expression: ExpressionTree<Variable>,
    ) -> Result<&ExpressionBinding<Variable>, RepresentationError> {
        debug_assert!(self.context.is_variable_available(self.constraints.scope, variable));
        let binding = ExpressionBinding::new(variable, expression);
        binding.validate(self.context).map_err(|source| RepresentationError::ExpressionDefinitionError { source })?;
        // WARNING: we can't set a variable category here, since we don't know if the expression will produce a
        //          Value, a ValueList, or a ThingList! We will know this at compilation time
        let as_ref = self.constraints.add_constraint(binding);
        Ok(as_ref.as_expression_binding().unwrap())
    }

    pub fn add_owns(
        &mut self,
        owner_type: Vertex<Variable>,
        attribute_type: Vertex<Variable>,
    ) -> Result<&Owns<Variable>, RepresentationError> {
        let owner_type_var = owner_type.as_variable();
        let attribute_type_var = attribute_type.as_variable();
        let owns = Constraint::from(Owns::new(owner_type, attribute_type));

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
    ) -> Result<&Relates<Variable>, RepresentationError> {
        let relation_type_var = relation_type.as_variable();
        let role_type_var = role_type.as_variable();
        let relates = Constraint::from(Relates::new(relation_type, role_type));

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
    ) -> Result<&Plays<Variable>, RepresentationError> {
        let player_type_var = player_type.as_variable();
        let role_type_var = role_type.as_variable();
        let plays = Constraint::from(Plays::new(player_type, role_type));

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

    pub(crate) fn create_anonymous_variable(&mut self) -> Result<Variable, RepresentationError> {
        self.context.create_anonymous_variable(self.constraints.scope)
    }

    pub(crate) fn get_or_declare_variable(&mut self, name: &str) -> Result<Variable, RepresentationError> {
        self.context.get_or_declare_variable(name, self.constraints.scope)
    }

    pub(crate) fn set_variable_optionality(&mut self, variable: Variable, optional: bool) {
        self.context.set_variable_is_optional(variable, optional)
    }

    pub(crate) fn parameters(&mut self) -> &mut ParameterRegistry {
        self.context.parameters()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Constraint<ID: IrID> {
    Kind(Kind<ID>),
    Label(Label<ID>),
    RoleName(RoleName<ID>),
    Sub(Sub<ID>),
    Isa(Isa<ID>),
    Links(Links<ID>),
    Has(Has<ID>),
    ExpressionBinding(ExpressionBinding<ID>),
    FunctionCallBinding(FunctionCallBinding<ID>),
    Comparison(Comparison<ID>),
    Owns(Owns<ID>),
    Relates(Relates<ID>),
    Plays(Plays<ID>),
}

impl<ID: IrID> Constraint<ID> {
    pub fn name(&self) -> &str {
        match self {
            Constraint::Kind(kind) => kind.kind.as_str(),
            Constraint::Label(_) => typeql::token::Keyword::Label.as_str(),
            Constraint::RoleName(_) => "role-name",
            Constraint::Sub(_) => typeql::token::Keyword::Sub.as_str(),
            Constraint::Isa(_) => typeql::token::Keyword::Isa.as_str(),
            Constraint::Links(_) => typeql::token::Keyword::Links.as_str(),
            Constraint::Has(_) => typeql::token::Keyword::Has.as_str(),
            Constraint::ExpressionBinding(_) => typeql::token::Comparator::Eq.as_str(),
            Constraint::FunctionCallBinding(_) => "=/in",
            Constraint::Comparison(comp) => comp.comparator.name(),
            Constraint::Owns(_) => typeql::token::Keyword::Owns.as_str(),
            Constraint::Relates(_) => typeql::token::Keyword::Relates.as_str(),
            Constraint::Plays(_) => typeql::token::Keyword::Plays.as_str(),
        }
    }

    pub fn ids(&self) -> Box<dyn Iterator<Item = ID> + '_> {
        match self {
            Constraint::Kind(kind) => Box::new(kind.ids()),
            Constraint::Label(label) => Box::new(label.ids()),
            Constraint::RoleName(role_name) => Box::new(role_name.ids()),
            Constraint::Sub(sub) => Box::new(sub.ids()),
            Constraint::Isa(isa) => Box::new(isa.ids()),
            Constraint::Links(rp) => Box::new(rp.ids()),
            Constraint::Has(has) => Box::new(has.ids()),
            Constraint::ExpressionBinding(binding) => Box::new(binding.ids_assigned()),
            Constraint::FunctionCallBinding(binding) => Box::new(binding.ids_assigned()),
            Constraint::Comparison(comparison) => Box::new(comparison.ids()),
            Constraint::Owns(owns) => Box::new(owns.ids()),
            Constraint::Relates(relates) => Box::new(relates.ids()),
            Constraint::Plays(plays) => Box::new(plays.ids()),
        }
    }

    pub fn vertices(&self) -> Box<dyn Iterator<Item = &Vertex<ID>> + '_> {
        match self {
            Constraint::Kind(kind) => Box::new(kind.vertices()),
            Constraint::Label(label) => Box::new(label.vertices()),
            Constraint::RoleName(role_name) => Box::new(role_name.vertices()),
            Constraint::Sub(sub) => Box::new(sub.vertices()),
            Constraint::Isa(isa) => Box::new(isa.vertices()),
            Constraint::Links(rp) => Box::new(rp.vertices()),
            Constraint::Has(has) => Box::new(has.vertices()),
            Constraint::ExpressionBinding(binding) => Box::new(binding.vertices_assigned()),
            Constraint::FunctionCallBinding(binding) => Box::new(binding.vertices_assigned()),
            Constraint::Comparison(comparison) => Box::new(comparison.vertices()),
            Constraint::Owns(owns) => Box::new(owns.vertices()),
            Constraint::Relates(relates) => Box::new(relates.vertices()),
            Constraint::Plays(plays) => Box::new(plays.vertices()),
        }
    }

    pub fn ids_foreach<F>(&self, function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        match self {
            Self::Kind(kind) => kind.ids_foreach(function),
            Self::Label(label) => label.ids_foreach(function),
            Self::RoleName(role_name) => role_name.ids_foreach(function),
            Self::Sub(sub) => sub.ids_foreach(function),
            Self::Isa(isa) => isa.ids_foreach(function),
            Self::Links(rp) => rp.ids_foreach(function),
            Self::Has(has) => has.ids_foreach(function),
            Self::ExpressionBinding(binding) => binding.ids_foreach(function),
            Self::FunctionCallBinding(binding) => binding.ids_foreach(function),
            Self::Comparison(comparison) => comparison.ids_foreach(function),
            Self::Owns(owns) => owns.ids_foreach(function),
            Self::Relates(relates) => relates.ids_foreach(function),
            Self::Plays(plays) => plays.ids_foreach(function),
        }
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> Constraint<T> {
        match self {
            Self::Kind(inner) => Constraint::Kind(inner.map(mapping)),
            Self::Label(inner) => Constraint::Label(inner.map(mapping)),
            Self::RoleName(inner) => Constraint::RoleName(inner.map(mapping)),
            Self::Sub(inner) => Constraint::Sub(inner.map(mapping)),
            Self::Isa(inner) => Constraint::Isa(inner.map(mapping)),
            Self::Links(inner) => Constraint::Links(inner.map(mapping)),
            Self::Has(inner) => Constraint::Has(inner.map(mapping)),
            Self::ExpressionBinding(inner) => todo!(),
            Self::FunctionCallBinding(inner) => todo!(),
            Self::Comparison(inner) => todo!(),
            Self::Owns(inner) => Constraint::Owns(inner.map(mapping)),
            Self::Relates(inner) => Constraint::Relates(inner.map(mapping)),
            Self::Plays(inner) => Constraint::Plays(inner.map(mapping)),
        }
    }

    pub fn left_id(&self) -> Option<ID> {
        let mut id = None;
        self.ids_foreach(|constraint_id, side| {
            if side == ConstraintIDSide::Left {
                id = Some(constraint_id);
            }
        });
        id
    }

    pub fn right_id(&self) -> Option<ID> {
        let mut id = None;
        self.ids_foreach(|constraint_id, side| {
            if side == ConstraintIDSide::Right {
                id = Some(constraint_id);
            }
        });
        id
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

    pub(crate) fn as_isa(&self) -> Option<&Isa<ID>> {
        match self {
            Constraint::Isa(isa) => Some(isa),
            _ => None,
        }
    }

    pub(crate) fn as_links(&self) -> Option<&Links<ID>> {
        match self {
            Constraint::Links(rp) => Some(rp),
            _ => None,
        }
    }

    pub(crate) fn as_has(&self) -> Option<&Has<ID>> {
        match self {
            Constraint::Has(has) => Some(has),
            _ => None,
        }
    }

    pub(crate) fn as_comparison(&self) -> Option<&Comparison<ID>> {
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

    pub(crate) fn as_relates(&self) -> Option<&Relates<ID>> {
        match self {
            Constraint::Relates(relates) => Some(relates),
            _ => None,
        }
    }

    pub(crate) fn as_plays(&self) -> Option<&Plays<ID>> {
        match self {
            Constraint::Plays(plays) => Some(plays),
            _ => None,
        }
    }
}

impl<ID: IrID> fmt::Display for Constraint<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Constraint::Kind(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Label(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::RoleName(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Sub(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Isa(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Links(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Has(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::ExpressionBinding(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::FunctionCallBinding(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Comparison(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Owns(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Relates(constraint) => fmt::Display::fmt(constraint, f),
            Constraint::Plays(constraint) => fmt::Display::fmt(constraint, f),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum ConstraintIDSide {
    Left,
    Right,
    Filter,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Label<ID> {
    left: Vertex<ID>,
    type_label: String,
}

impl<ID: IrID> Label<ID> {
    fn new(left: ID, type_label: String) -> Self {
        Self { left: Vertex::Variable(left), type_label }
    }

    pub fn type_(&self) -> &Vertex<ID> {
        &self.left
    }

    pub fn type_label(&self) -> &str {
        &self.type_label
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> + Sized {
        self.left.as_variable().into_iter()
    }

    pub fn vertices(&self) -> impl Iterator<Item = &Vertex<ID>> + Sized {
        [&self.left].into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        self.left.as_variable().inspect(|&id| function(id, ConstraintIDSide::Left));
    }

    fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> Label<T> {
        Label { left: self.left.map(mapping), type_label: self.type_label }
    }
}

impl<ID: IrID> From<Label<ID>> for Constraint<ID> {
    fn from(val: Label<ID>) -> Self {
        Constraint::Label(val)
    }
}

impl<ID: IrID> fmt::Display for Label<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: implement indentation without rewriting it everywhere
        // write!(f, "{: >width$} {} type {}", "", self.left, self.type_, width=f.width().unwrap_or(0))
        write!(f, "{} label {}", self.left, self.type_label)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RoleName<ID> {
    left: Vertex<ID>,
    name: String,
}

impl<ID: IrID> RoleName<ID> {
    pub fn new(left: ID, name: String) -> Self {
        Self { left: Vertex::Variable(left), name }
    }

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
        F: FnMut(ID, ConstraintIDSide),
    {
        self.left.as_variable().inspect(|&id| function(id, ConstraintIDSide::Left));
    }

    fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> RoleName<T> {
        RoleName { left: self.left.map(mapping), name: self.name }
    }
}

impl<ID: IrID> From<RoleName<ID>> for Constraint<ID> {
    fn from(value: RoleName<ID>) -> Self {
        Constraint::RoleName(value)
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
}

impl<ID: IrID> Kind<ID> {
    pub fn new(kind: typeql::token::Kind, type_: ID) -> Self {
        Self { kind, type_: Vertex::Variable(type_) }
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
        F: FnMut(ID, ConstraintIDSide),
    {
        self.type_.as_variable().inspect(|&id| function(id, ConstraintIDSide::Left));
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> Kind<T> {
        Kind { kind: self.kind, type_: self.type_.map(mapping) }
    }
}

impl<ID: IrID> From<Kind<ID>> for Constraint<ID> {
    fn from(kind: Kind<ID>) -> Self {
        Constraint::Kind(kind)
    }
}

impl<ID: IrID> fmt::Display for Kind<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl<ID: IrID> fmt::Debug for Kind<ID> {
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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Sub<ID> {
    kind: SubKind,
    subtype: Vertex<ID>,
    supertype: Vertex<ID>,
}

impl<ID: IrID> Sub<ID> {
    fn new(kind: SubKind, subtype: Vertex<ID>, supertype: Vertex<ID>) -> Self {
        Sub { subtype, supertype, kind }
    }

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
        F: FnMut(ID, ConstraintIDSide),
    {
        self.subtype.as_variable().inspect(|&id| function(id, ConstraintIDSide::Left));
        self.supertype.as_variable().inspect(|&id| function(id, ConstraintIDSide::Right));
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> Sub<T> {
        Sub::new(self.kind, self.subtype.map(mapping), self.supertype.map(mapping))
    }
}

impl<ID: IrID> From<Sub<ID>> for Constraint<ID> {
    fn from(val: Sub<ID>) -> Self {
        Constraint::Sub(val)
    }
}

impl<ID: IrID> fmt::Display for Sub<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} sub {}", self.subtype, self.supertype)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Isa<ID> {
    kind: IsaKind,
    thing: Vertex<ID>,
    type_: Vertex<ID>,
}

impl<ID: IrID> Isa<ID> {
    fn new(kind: IsaKind, thing: ID, type_: Vertex<ID>) -> Self {
        Self { kind, thing: Vertex::Variable(thing), type_ }
    }

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
        F: FnMut(ID, ConstraintIDSide),
    {
        self.thing.as_variable().inspect(|&id| function(id, ConstraintIDSide::Left));
        self.type_.as_variable().inspect(|&id| function(id, ConstraintIDSide::Right));
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> Isa<T> {
        Isa { kind: self.kind, thing: self.thing.map(mapping), type_: self.type_.map(mapping) }
    }
}

impl<ID: IrID> From<Isa<ID>> for Constraint<ID> {
    fn from(val: Isa<ID>) -> Self {
        Constraint::Isa(val)
    }
}

impl<ID: IrID> fmt::Display for Isa<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} isa {}", self.thing, self.type_)
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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Links<ID> {
    pub(crate) relation: Vertex<ID>,
    pub(crate) player: Vertex<ID>,
    pub(crate) role_type: Vertex<ID>,
}

impl<ID: IrID> Links<ID> {
    pub fn new(relation: ID, player: ID, role_type: ID) -> Self {
        Self {
            relation: Vertex::Variable(relation),
            player: Vertex::Variable(player),
            role_type: Vertex::Variable(role_type),
        }
    }

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
        F: FnMut(ID, ConstraintIDSide),
    {
        self.relation.as_variable().inspect(|&id| function(id, ConstraintIDSide::Left));
        self.player.as_variable().inspect(|&id| function(id, ConstraintIDSide::Right));
        self.role_type.as_variable().inspect(|&id| function(id, ConstraintIDSide::Filter));
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> Links<T> {
        Links {
            relation: self.relation.map(mapping),
            player: self.player.map(mapping),
            role_type: self.role_type.map(mapping),
        }
    }
}

impl<ID: IrID> From<Links<ID>> for Constraint<ID> {
    fn from(links: Links<ID>) -> Self {
        Constraint::Links(links)
    }
}

impl<ID: IrID> fmt::Display for Links<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} rp {} (role: {})", self.relation, self.player, self.role_type)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Has<ID> {
    owner: Vertex<ID>,
    attribute: Vertex<ID>,
}

impl<ID: IrID> Has<ID> {
    pub fn new(owner: ID, attribute: ID) -> Self {
        Has { owner: Vertex::Variable(owner), attribute: Vertex::Variable(attribute) }
    }

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
        F: FnMut(ID, ConstraintIDSide),
    {
        self.owner.as_variable().inspect(|&id| function(id, ConstraintIDSide::Left));
        self.attribute.as_variable().inspect(|&id| function(id, ConstraintIDSide::Right));
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> Has<T> {
        Has { owner: self.owner.map(mapping), attribute: self.attribute.map(mapping) }
    }
}

impl<ID: IrID> From<Has<ID>> for Constraint<ID> {
    fn from(has: Has<ID>) -> Self {
        Constraint::Has(has)
    }
}

impl<ID: IrID> fmt::Display for Has<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} has {}", self.owner, self.attribute)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ExpressionBinding<ID> {
    left: Vertex<ID>,
    expression: ExpressionTree<ID>,
}

impl<ID: IrID> ExpressionBinding<ID> {
    fn new(left: ID, expression: ExpressionTree<ID>) -> Self {
        Self { left: Vertex::Variable(left), expression }
    }

    pub fn left(&self) -> &Vertex<ID> {
        &self.left
    }

    pub fn expression(&self) -> &ExpressionTree<ID> {
        &self.expression
    }

    pub fn vertices_assigned(&self) -> impl Iterator<Item = &Vertex<ID>> {
        [&self.left].into_iter()
    }

    pub fn ids_assigned(&self) -> impl Iterator<Item = ID> {
        self.left.as_variable().into_iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        self.ids_assigned().for_each(|id| function(id, ConstraintIDSide::Left));
        // TODO
        // todo!("Do we really need positions here?")
        // self.expression().ids().for_each(|id| function(id, ConstraintIDSide::Right));
    }

    pub(crate) fn validate(&self, context: &mut BlockBuilderContext<'_>) -> Result<(), ExpressionDefinitionError> {
        if self.expression().is_empty() {
            Err(ExpressionDefinitionError::EmptyExpressionTree {})
        } else {
            Ok(())
        }
    }
}

impl<ID: IrID> From<ExpressionBinding<ID>> for Constraint<ID> {
    fn from(val: ExpressionBinding<ID>) -> Self {
        Constraint::ExpressionBinding(val)
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
}

impl<ID: IrID> FunctionCallBinding<ID> {
    fn new(left: Vec<ID>, function_call: FunctionCall<ID>, is_stream: bool) -> Self {
        Self { assigned: left.into_iter().map(Vertex::Variable).collect(), function_call, is_stream }
    }

    pub fn assigned(&self) -> &[Vertex<ID>] {
        &self.assigned
    }

    pub fn function_call(&self) -> &FunctionCall<ID> {
        &self.function_call
    }

    pub fn ids(&self) -> impl Iterator<Item = ID> + '_ {
        self.ids_assigned().chain(self.function_call.argument_ids())
    }

    pub fn ids_assigned(&self) -> impl Iterator<Item = ID> + '_ {
        self.assigned.iter().filter_map(Vertex::as_variable)
    }

    pub fn vertices_assigned(&self) -> impl Iterator<Item = &Vertex<ID>> + '_ {
        self.assigned.iter()
    }

    pub fn ids_foreach<F>(&self, mut function: F)
    where
        F: FnMut(ID, ConstraintIDSide),
    {
        for id in self.ids_assigned() {
            function(id, ConstraintIDSide::Left)
        }
        for id in self.function_call.argument_ids() {
            function(id, ConstraintIDSide::Right)
        }
    }
}

impl<ID: IrID> From<FunctionCallBinding<ID>> for Constraint<ID> {
    fn from(val: FunctionCallBinding<ID>) -> Self {
        Constraint::FunctionCallBinding(val)
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
}

impl From<typeql::token::Comparator> for Comparator {
    fn from(token: typeql::token::Comparator) -> Self {
        match token {
            typeql::token::Comparator::Eq => Self::Equal,
            typeql::token::Comparator::EqLegacy => Self::Equal,
            typeql::token::Comparator::Neq => Self::NotEqual,
            typeql::token::Comparator::Gt => Self::Greater,
            typeql::token::Comparator::Gte => Self::GreaterOrEqual,
            typeql::token::Comparator::Lt => Self::Less,
            typeql::token::Comparator::Lte => Self::LessOrEqual,
            typeql::token::Comparator::Contains => Self::Contains,
            typeql::token::Comparator::Like => Self::Like,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Comparison<ID> {
    lhs: Vertex<ID>,
    rhs: Vertex<ID>,
    comparator: Comparator,
}

impl<ID: IrID> Comparison<ID> {
    fn new(lhs: Vertex<ID>, rhs: Vertex<ID>, comparator: Comparator) -> Self {
        Self { lhs, rhs, comparator }
    }

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
        F: FnMut(ID, ConstraintIDSide),
    {
        self.lhs.as_variable().inspect(|&id| function(id, ConstraintIDSide::Left));
        self.rhs.as_variable().inspect(|&id| function(id, ConstraintIDSide::Right));
    }
}

impl<ID: IrID> From<Comparison<ID>> for Constraint<ID> {
    fn from(comp: Comparison<ID>) -> Self {
        Constraint::Comparison(comp)
    }
}

impl<ID: IrID> fmt::Display for Comparison<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Owns<ID> {
    owner: Vertex<ID>,
    attribute: Vertex<ID>,
}

impl<ID: IrID> Owns<ID> {
    fn new(owner: Vertex<ID>, attribute: Vertex<ID>) -> Self {
        Self { owner, attribute }
    }

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
        F: FnMut(ID, ConstraintIDSide),
    {
        self.owner.as_variable().inspect(|&id| function(id, ConstraintIDSide::Left));
        self.attribute.as_variable().inspect(|&id| function(id, ConstraintIDSide::Right));
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> Owns<T> {
        Owns::new(self.owner.map(mapping), self.attribute.map(mapping))
    }
}

impl<ID: IrID> From<Owns<ID>> for Constraint<ID> {
    fn from(val: Owns<ID>) -> Self {
        Constraint::Owns(val)
    }
}

impl<ID: IrID> fmt::Display for Owns<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Relates<ID> {
    relation: Vertex<ID>,
    role_type: Vertex<ID>,
}

impl<ID: IrID> Relates<ID> {
    fn new(relation: Vertex<ID>, role: Vertex<ID>) -> Self {
        Self { relation, role_type: role }
    }

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
        F: FnMut(ID, ConstraintIDSide),
    {
        self.relation.as_variable().inspect(|&id| function(id, ConstraintIDSide::Left));
        self.role_type.as_variable().inspect(|&id| function(id, ConstraintIDSide::Right));
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> Relates<T> {
        Relates::new(self.relation.map(mapping), self.role_type.map(mapping))
    }
}

impl<ID: IrID> From<Relates<ID>> for Constraint<ID> {
    fn from(val: Relates<ID>) -> Self {
        Constraint::Relates(val)
    }
}

impl<ID: IrID> fmt::Display for Relates<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Plays<ID> {
    player: Vertex<ID>,
    role_type: Vertex<ID>,
}

impl<ID: IrID> Plays<ID> {
    fn new(player: Vertex<ID>, role: Vertex<ID>) -> Self {
        Self { player, role_type: role }
    }

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
        F: FnMut(ID, ConstraintIDSide),
    {
        self.player.as_variable().inspect(|&id| function(id, ConstraintIDSide::Left));
        self.role_type.as_variable().inspect(|&id| function(id, ConstraintIDSide::Right));
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> Plays<T> {
        Plays::new(self.player.map(mapping), self.role_type.map(mapping))
    }
}

impl<ID: IrID> From<Plays<ID>> for Constraint<ID> {
    fn from(val: Plays<ID>) -> Self {
        Constraint::Plays(val)
    }
}

impl<ID: IrID> fmt::Display for Plays<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
