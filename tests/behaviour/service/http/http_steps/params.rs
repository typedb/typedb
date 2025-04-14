/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{borrow::Borrow, convert::Infallible, fmt, str::FromStr};

use cucumber::Parameter;

use crate::message::ConceptResponse;

#[derive(Clone, Debug, Default, Parameter)]
#[param(name = "var", regex = r".*")]
pub struct Var {
    pub name: String,
}

impl FromStr for Var {
    type Err = Infallible;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Ok(Self { name: name.to_owned() })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "is_or_not", regex = "(is|is not)")]
pub(crate) enum IsOrNot {
    Is,
    IsNot,
}

impl IsOrNot {
    pub fn check(&self, real_is: bool) {
        match self {
            Self::Is => {
                assert!(real_is)
            }
            Self::IsNot => {
                assert!(!real_is)
            }
        };
    }

    pub fn check_none<T: fmt::Debug>(&self, value: &Option<T>) {
        match self {
            Self::Is => {
                assert!(matches!(value, None), "expected to be none")
            }
            Self::IsNot => {
                assert!(matches!(value, Some(_)), "expected to be NOT none")
            }
        };
    }

    pub fn compare<T: PartialEq + fmt::Debug>(&self, left: T, right: T) {
        match self {
            Self::Is => {
                assert_eq!(left, right, "Expected '{left:?}' is '{right:?}'")
            }
            Self::IsNot => {
                assert_ne!(left, right, "Expected '{left:?}' is not '{right:?}'")
            }
        };
    }

    pub fn is(&self) -> bool {
        match self {
            IsOrNot::Is => true,
            IsOrNot::IsNot => false,
        }
    }
}

impl FromStr for IsOrNot {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "is" => Self::Is,
            "is not" => Self::IsNot,
            invalid => return Err(format!("Invalid `IsOrNot`: {invalid}")),
        })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "is_by_var_index", regex = "(| by index of variable)")]
pub(crate) enum IsByVarIndex {
    Is,
    IsNot,
}

impl FromStr for IsByVarIndex {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            " by index of variable" => Self::Is,
            "" => Self::IsNot,
            invalid => return Err(format!("Invalid `IsByVarIndex`: {invalid}")),
        })
    }
}

#[derive(Debug, Clone, Copy, Parameter)]
#[param(name = "query_answer_type", regex = "(ok|concept rows|concept documents)")]
pub(crate) enum QueryAnswerType {
    Ok,
    ConceptRows,
    ConceptDocuments,
}

impl FromStr for QueryAnswerType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "ok" => Self::Ok,
            "concept rows" => Self::ConceptRows,
            "concept documents" => Self::ConceptDocuments,
            invalid => return Err(format!("Invalid `QueryAnswerType`: {invalid}")),
        })
    }
}

impl fmt::Display for QueryAnswerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryAnswerType::Ok => write!(f, "Ok"),
            QueryAnswerType::ConceptRows => write!(f, "ConceptRows"),
            QueryAnswerType::ConceptDocuments => write!(f, "ConceptTrees"),
        }
    }
}

#[derive(Debug, Clone, Copy, Parameter)]
#[param(name = "query_type", regex = "(read|write|schema)")]
pub(crate) enum QueryType {
    Read,
    Write,
    Schema,
}

impl FromStr for QueryType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "read" => Self::Read,
            "write" => Self::Write,
            "schema" => Self::Schema,
            invalid => return Err(format!("Invalid `QueryType`: {invalid}")),
        })
    }
}

impl fmt::Display for QueryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryType::Read => write!(f, "Read"),
            QueryType::Write => write!(f, "Write"),
            QueryType::Schema => write!(f, "Schema"),
        }
    }
}

impl Into<server::service::QueryType> for QueryType {
    fn into(self) -> server::service::QueryType {
        match self {
            QueryType::Read => server::service::QueryType::Read,
            QueryType::Write => server::service::QueryType::Write,
            QueryType::Schema => server::service::QueryType::Schema,
        }
    }
}

#[derive(Debug, Parameter)]
#[param(
    name = "concept_kind",
    regex = "(concept|variable|type|instance|entity type|relation type|attribute type|role type|entity|relation|attribute|value)"
)]
pub(crate) enum ConceptKind {
    Concept,
    Type,
    Instance,
    EntityType,
    RelationType,
    AttributeType,
    RoleType,
    Entity,
    Relation,
    Attribute,
    Value,
}

impl ConceptKind {
    pub(crate) fn matches_concept(&self, concept: &ConceptResponse) -> bool {
        match self {
            ConceptKind::Concept => true,
            ConceptKind::Type => match concept {
                ConceptResponse::EntityType(_)
                | ConceptResponse::RelationType(_)
                | ConceptResponse::AttributeType(_)
                | ConceptResponse::RoleType(_) => true,
                _ => false,
            },
            ConceptKind::Instance => match concept {
                ConceptResponse::Entity(_) | ConceptResponse::Relation(_) | ConceptResponse::Attribute(_) => true,
                _ => false,
            },
            ConceptKind::EntityType => matches!(concept, ConceptResponse::EntityType(_)),
            ConceptKind::RelationType => matches!(concept, ConceptResponse::RelationType(_)),
            ConceptKind::AttributeType => matches!(concept, ConceptResponse::AttributeType(_)),
            ConceptKind::RoleType => matches!(concept, ConceptResponse::RoleType(_)),
            ConceptKind::Entity => matches!(concept, ConceptResponse::Entity(_)),
            ConceptKind::Relation => matches!(concept, ConceptResponse::Relation(_)),
            ConceptKind::Attribute => matches!(concept, ConceptResponse::Attribute(_)),
            ConceptKind::Value => matches!(concept, ConceptResponse::Value(_)),
        }
    }
}

impl FromStr for ConceptKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "concept" | "variable" => Self::Concept,
            "type" => Self::Type,
            "instance" => Self::Instance,
            "entity type" => Self::EntityType,
            "relation type" => Self::RelationType,
            "attribute type" => Self::AttributeType,
            "role type" => Self::RoleType,
            "entity" => Self::Entity,
            "relation" => Self::Relation,
            "attribute" => Self::Attribute,
            "value" => Self::Value,
            invalid => return Err(format!("Invalid `ConceptKind`: {invalid}")),
        })
    }
}

impl fmt::Display for ConceptKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Concept => write!(f, "Concept"),
            Self::Type => write!(f, "Type"),
            Self::Instance => write!(f, "Instance"),
            Self::EntityType => write!(f, "EntityType"),
            Self::RelationType => write!(f, "RelationType"),
            Self::AttributeType => write!(f, "AttributeType"),
            Self::RoleType => write!(f, "RoleType"),
            Self::Entity => write!(f, "Entity"),
            Self::Relation => write!(f, "Relation"),
            Self::Attribute => write!(f, "Attribute"),
            Self::Value => write!(f, "Value"),
        }
    }
}
