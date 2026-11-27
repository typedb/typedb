/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use core::cmp::{Eq, Ord, PartialOrd};
use std::{fmt, sync::Arc};

use concept::type_::{TypeAPI, attribute_type::AttributeType, object_type::ObjectType};
use encoding::{
    error::EncodingError,
    graph::{
        rbac::{RBACAccessKind, RBACRoleVertex, RBACTypeAccessEdge},
        type_::vertex::{TypeVertex, TypeVertexEncoding},
    },
};
use error::typedb_error;
use storage::snapshot::{SnapshotGetError, iterator::SnapshotIteratorError};

pub mod rbac_cache;
pub mod rbac_manager;

typedb_error! {
    pub RBACError(component = "RBAC", prefix = "RBC") {
        SnapshotGet(1, "Failed to read key from storage snapshot.", source: SnapshotGetError),
        SnapshotIterate(2, "Failed to open iterator on storage snapshot.", source: Arc<SnapshotIteratorError>),

        RBACRoleAlreadyExists(3, "An RBAC role with name '{name}' already exists.", name: String),
        CreateRBACRoleEncoding(4, "Encoding error while trying to create RBAC role.", source: EncodingError),
        RBACRoleNotFound(5, "RBAC role was not found."),
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum RBACAccess {
    ReadObject(ObjectType),
    WriteObject(ObjectType),
    ReadAttribute(AttributeType),
    WriteAttribute(AttributeType),
}

impl RBACAccess {
    pub fn new_type_access(access_kind: RBACAccessKind, type_: TypeVertex) -> Self {
        match access_kind {
            RBACAccessKind::ReadObjectVertex => Self::ReadObject(ObjectType::new(type_)),
            RBACAccessKind::WriteObjectVertex => Self::WriteObject(ObjectType::new(type_)),
            RBACAccessKind::ReadAttributeVertex => Self::ReadAttribute(AttributeType::new(type_)),
            RBACAccessKind::WriteAttributeVertex => Self::WriteAttribute(AttributeType::new(type_)),
        }
    }

    pub fn to_type_access_for_role(&self, role: &RBACRole) -> RBACTypeAccessEdge {
        match self {
            RBACAccess::ReadObject(type_) => {
                RBACTypeAccessEdge::new(role.vertex(), RBACAccessKind::ReadObjectVertex, type_.vertex())
            }
            RBACAccess::WriteObject(type_) => {
                RBACTypeAccessEdge::new(role.vertex(), RBACAccessKind::WriteObjectVertex, type_.vertex())
            }
            RBACAccess::ReadAttribute(type_) => {
                RBACTypeAccessEdge::new(role.vertex(), RBACAccessKind::ReadAttributeVertex, type_.vertex())
            }
            RBACAccess::WriteAttribute(type_) => {
                RBACTypeAccessEdge::new(role.vertex(), RBACAccessKind::WriteAttributeVertex, type_.vertex())
            }
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RBACRole {
    vertex: RBACRoleVertex,
}

impl fmt::Debug for RBACRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RBACRole[{:?}]", self.vertex.role_id())
    }
}

impl RBACRole {
    pub fn new(vertex: RBACRoleVertex) -> Self {
        Self { vertex }
    }

    pub fn vertex(&self) -> RBACRoleVertex {
        self.vertex
    }
}
