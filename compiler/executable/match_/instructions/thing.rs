/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    sync::Arc,
};

use answer::{variable::Variable, Type};
use concept::type_::role_type::RoleType;
use ir::pattern::{
    constraint::{Has, Iid, Isa, Links},
    IrID,
};
use ir::pattern::constraint::IndexedRelation;
use storage::MVCCKey;

use crate::{
    annotation::type_annotations::TypeAnnotations,
    executable::match_::instructions::{CheckInstruction, DisplayVec, Inputs},
};

#[derive(Debug, Clone)]
pub struct IidInstruction<ID> {
    pub iid: Iid<ID>,
    pub types: Arc<BTreeSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl IidInstruction<Variable> {
    pub fn new(iid: Iid<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let types = type_annotations.vertex_annotations_of(iid.var()).unwrap().clone();
        Self { iid, types, checks: Vec::new() }
    }
}

impl<ID> IidInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }
}

impl<ID: IrID> IidInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> IidInstruction<T> {
        let Self { iid, types, checks } = self;
        IidInstruction {
            iid: iid.map(mapping),
            types,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

impl<ID: IrID> fmt::Display for IidInstruction<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] filter {}", &self.iid, DisplayVec::new(&self.checks))
    }
}

#[derive(Debug, Clone)]
pub struct IsaInstruction<ID> {
    pub isa: Isa<ID>,
    pub inputs: Inputs<ID>,
    pub instance_type_to_types: Arc<BTreeMap<Type, Vec<Type>>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl IsaInstruction<Variable> {
    pub fn new(isa: Isa<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let isa_annotations = type_annotations.constraint_annotations_of(isa.clone().into()).unwrap();
        let instance_to_types = isa_annotations.as_left_right().left_to_right().clone();
        Self { isa, inputs, instance_type_to_types: instance_to_types, checks: Vec::new() }
    }
}

impl<ID> IsaInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }
}

impl<ID: IrID> IsaInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> IsaInstruction<T> {
        let Self { isa, inputs, instance_type_to_types: instance_to_types, checks } = self;
        IsaInstruction {
            isa: isa.map(mapping),
            inputs: inputs.map(mapping),
            instance_type_to_types: instance_to_types,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

impl<ID: IrID> fmt::Display for IsaInstruction<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] filter {}", &self.isa, DisplayVec::new(&self.checks))
    }
}

#[derive(Debug, Clone)]
pub struct IsaReverseInstruction<ID> {
    pub isa: Isa<ID>,
    pub inputs: Inputs<ID>,
    pub type_to_instance_types: Arc<BTreeMap<Type, Vec<Type>>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl IsaReverseInstruction<Variable> {
    pub fn new(isa: Isa<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let isa_annotations = type_annotations.constraint_annotations_of(isa.clone().into()).unwrap();
        let type_to_instance_types = isa_annotations.as_left_right().right_to_left();
        Self { isa, inputs, type_to_instance_types, checks: Vec::new() }
    }
}

impl<ID> IsaReverseInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }
}

impl<ID: IrID> IsaReverseInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> IsaReverseInstruction<T> {
        let Self { isa, inputs, type_to_instance_types, checks } = self;
        IsaReverseInstruction {
            isa: isa.map(mapping),
            inputs: inputs.map(mapping),
            type_to_instance_types,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

impl<ID: IrID> fmt::Display for IsaReverseInstruction<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Reverse[{}] filter {}", &self.isa, DisplayVec::new(&self.checks))
    }
}

#[derive(Debug, Clone)]
pub struct HasInstruction<ID> {
    pub has: Has<ID>,
    pub inputs: Inputs<ID>,
    owner_to_attribute_types: Arc<BTreeMap<Type, Vec<Type>>>,
    attribute_types: Arc<BTreeSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl HasInstruction<Variable> {
    pub fn new(has: Has<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let constraint_annotations =
            type_annotations.constraint_annotations_of(has.clone().into()).unwrap().as_left_right();
        let owner_to_attribute_types = constraint_annotations.left_to_right();
        let attribute_types = type_annotations.vertex_annotations_of(has.attribute()).unwrap().clone();
        Self { has, inputs, owner_to_attribute_types, attribute_types, checks: Vec::new() }
    }
}

impl<ID> HasInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }

    pub fn owner_to_attribute_types(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.owner_to_attribute_types
    }

    pub fn attribute_types(&self) -> &Arc<BTreeSet<Type>> {
        &self.attribute_types
    }
}

impl<ID: IrID> HasInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> HasInstruction<T> {
        let Self { has, inputs, owner_to_attribute_types, attribute_types, checks } = self;
        HasInstruction {
            has: has.map(mapping),
            inputs: inputs.map(mapping),
            owner_to_attribute_types,
            attribute_types,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

impl<ID: IrID> fmt::Display for HasInstruction<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] filter {}", &self.has, DisplayVec::new(&self.checks))
    }
}

#[derive(Debug, Clone)]
pub struct HasReverseInstruction<ID> {
    pub has: Has<ID>,
    pub inputs: Inputs<ID>,
    attribute_to_owner_types: Arc<BTreeMap<Type, Vec<Type>>>,
    owner_types: Arc<BTreeSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl HasReverseInstruction<Variable> {
    pub fn new(has: Has<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let edge_annotations = &type_annotations.constraint_annotations_of(has.clone().into()).unwrap().as_left_right();
        let attribute_to_owner_types = edge_annotations.right_to_left().clone();
        let owner_types = type_annotations.vertex_annotations_of(has.owner()).unwrap().clone();
        Self { has, inputs, attribute_to_owner_types, owner_types, checks: Vec::new() }
    }
}

impl<ID> HasReverseInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }

    pub fn attribute_to_owner_types(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.attribute_to_owner_types
    }

    pub fn owner_types(&self) -> &Arc<BTreeSet<Type>> {
        &self.owner_types
    }
}

impl<ID: IrID> HasReverseInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> HasReverseInstruction<T> {
        let Self { has, inputs, attribute_to_owner_types, owner_types, checks } = self;
        HasReverseInstruction {
            has: has.map(mapping),
            inputs: inputs.map(mapping),
            attribute_to_owner_types,
            owner_types,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

impl<ID: IrID> fmt::Display for HasReverseInstruction<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Reverse[{}] filter {}", &self.has, DisplayVec::new(&self.checks))
    }
}

#[derive(Debug, Clone)]
pub struct LinksInstruction<ID> {
    pub links: Links<ID>,
    pub inputs: Inputs<ID>,
    relation_to_player_types: Arc<BTreeMap<Type, Vec<Type>>>,
    player_to_role_types: Arc<BTreeMap<Type, BTreeSet<Type>>>,
    player_types: Arc<BTreeSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl LinksInstruction<Variable> {
    pub fn new(links: Links<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let edge_annotations =
            type_annotations.constraint_annotations_of(links.clone().into()).unwrap().as_links();
        let player_to_role_types = edge_annotations.player_to_role();
        let relation_to_player_types = edge_annotations.relation_to_player();
        let player_types = type_annotations.vertex_annotations_of(links.player()).unwrap().clone();
        Self { links, inputs, relation_to_player_types, player_types, player_to_role_types, checks: Vec::new() }
    }
}

impl<ID> LinksInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }

    pub fn relation_to_player_types(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.relation_to_player_types
    }

    pub fn player_types(&self) -> &Arc<BTreeSet<Type>> {
        &self.player_types
    }

    pub fn relation_to_role_types(&self) -> &Arc<BTreeMap<Type, BTreeSet<Type>>> {
        &self.player_to_role_types
    }
}

impl<ID: IrID> LinksInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> LinksInstruction<T> {
        let Self { links, inputs, relation_to_player_types, player_types, player_to_role_types, checks } = self;
        LinksInstruction {
            links: links.map(mapping),
            inputs: inputs.map(mapping),
            relation_to_player_types,
            player_types,
            player_to_role_types,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

impl<ID: IrID> fmt::Display for LinksInstruction<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] filter {}", &self.links, DisplayVec::new(&self.checks))
    }
}

#[derive(Debug, Clone)]
pub struct LinksReverseInstruction<ID> {
    pub links: Links<ID>,
    pub inputs: Inputs<ID>,
    player_to_relation_types: Arc<BTreeMap<Type, Vec<Type>>>,
    relation_to_role_types: Arc<BTreeMap<Type, BTreeSet<Type>>>,
    relation_types: Arc<BTreeSet<Type>>,
    pub checks: Vec<CheckInstruction<ID>>,
}

impl LinksReverseInstruction<Variable> {
    pub fn new(links: Links<Variable>, inputs: Inputs<Variable>, type_annotations: &TypeAnnotations) -> Self {
        let edge_annotations =
            type_annotations.constraint_annotations_of(links.clone().into()).unwrap().as_links().clone();
        let relation_to_role_types = edge_annotations.relation_to_role();
        let player_to_relation_types = edge_annotations.player_to_relation();
        let relation_types = type_annotations.vertex_annotations_of(links.relation()).unwrap().clone();
        Self { links, inputs, player_to_relation_types, relation_types, relation_to_role_types, checks: Vec::new() }
    }
}

impl<ID> LinksReverseInstruction<ID> {
    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }

    pub fn player_to_relation_types(&self) -> &Arc<BTreeMap<Type, Vec<Type>>> {
        &self.player_to_relation_types
    }

    pub fn relation_types(&self) -> &Arc<BTreeSet<Type>> {
        &self.relation_types
    }

    pub fn relation_to_role_types(&self) -> &Arc<BTreeMap<Type, BTreeSet<Type>>> {
        &self.relation_to_role_types
    }
}

impl<ID: IrID> LinksReverseInstruction<ID> {
    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> LinksReverseInstruction<T> {
        let Self { links, inputs, player_to_relation_types, relation_to_role_types, relation_types, checks } = self;
        LinksReverseInstruction {
            links: links.map(mapping),
            inputs: inputs.map(mapping),
            player_to_relation_types,
            relation_to_role_types,
            relation_types,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }
}

impl<ID: IrID> fmt::Display for LinksReverseInstruction<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Reverse[{}] filter {}", &self.links, DisplayVec::new(&self.checks))
    }
}

// We use a lowered form of the IndexedRelation, since it is fully symmetric otherwise
#[derive(Debug, Clone)]
pub struct IndexedRelationInstruction<ID> {
    pub player_start: ID,
    pub player_end: ID,
    pub relation: ID,
    pub role_start: ID,
    pub role_end: ID,

    pub inputs: Inputs<ID>,
    pub checks: Vec<CheckInstruction<ID>>,

    // the prefixes we will generally want to construct are [rel type][from][to type]
    pub relation_to_player_start_types: Arc<BTreeMap<Type, Vec<Type>>>,
    pub player_start_to_player_end_types: Arc<BTreeMap<Type, BTreeSet<Type>>>,
    pub role_start_types: Arc<BTreeSet<RoleType>>,
    pub role_end_types: Arc<BTreeSet<RoleType>>,
}

impl IndexedRelationInstruction<Variable> {
    pub fn new(
        player_start: Variable,
        player_end: Variable,
        relation: Variable,
        role_start: Variable,
        role_end: Variable,

        inputs: Inputs<Variable>,

        relation_to_player_start_types: Arc<BTreeMap<Type, Vec<Type>>>,
        player_start_to_relation_types: &BTreeMap<Type, Vec<Type>>,
        relation_to_player_end_types: &BTreeMap<Type, Vec<Type>>,
        role_start_types: Arc<BTreeSet<RoleType>>,
        role_end_types: Arc<BTreeSet<RoleType>>,
    ) -> Self {
        let mut player_start_to_player_end_types = BTreeMap::new();
        for (player_start_type, relation_types) in player_start_to_relation_types {
            let player_end_types = player_start_to_player_end_types.entry(*player_start_type)
                .or_insert(BTreeSet::new());
            for relation_type in relation_types {
                player_end_types.extend(relation_to_player_end_types.get(&relation_type).iter().flat_map(|vec| vec.iter()));
            }
        }

        Self {
            player_start,
            player_end,
            relation,
            role_start,
            role_end,
            inputs,
            relation_to_player_start_types,
            player_start_to_player_end_types: Arc::new(player_start_to_player_end_types),
            role_start_types,
            role_end_types,
            checks: Vec::new()
        }
    }
}

impl<ID: IrID> IndexedRelationInstruction<ID> {
    pub fn player_start(&self) -> &ID {
        &self.player_start
    }
    pub fn player_end(&self) -> &ID {
        &self.player_end
    }
    pub fn relation(&self) -> &ID {
        &self.relation
    }
    pub fn role_start(&self) -> &ID {
        &self.role_start
    }
    pub fn role_end(&self) -> &ID {
        &self.role_end
    }

    pub fn map<T: IrID>(self, mapping: &HashMap<ID, T>) -> IndexedRelationInstruction<T> {
        let Self {
            player_start,
            player_end,
            relation,
            role_start,
            role_end,
            inputs,
            relation_to_player_start_types,
            player_start_to_player_end_types,
            role_start_types,
            role_end_types,
            checks,
        } = self;
        IndexedRelationInstruction {
            player_start: mapping[&player_start],
            player_end: mapping[&player_end],
            relation: mapping[&relation],
            role_start: mapping[&role_start],
            role_end: mapping[&role_end],
            inputs: inputs.map(mapping),
            relation_to_player_start_types,
            player_start_to_player_end_types,
            role_start_types,
            role_end_types,
            checks: checks.into_iter().map(|check| check.map(mapping)).collect(),
        }
    }

    pub(crate) fn add_check(&mut self, check: CheckInstruction<ID>) {
        self.checks.push(check)
    }
}

impl<ID: IrID> fmt::Display for IndexedRelationInstruction<ID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,
               "{} indexed_relation(role: {} -> relation: {} -> role: {}) to {}",
               self.player_start,
               self.role_start,
               self.relation,
               self.role_end,
               self.player_end
        )
    }
}
