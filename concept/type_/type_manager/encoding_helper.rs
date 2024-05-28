use bytes::Bytes;
use encoding::graph::type_::edge::{EdgeOwnsEncoder, EdgeOwnsReverseEncoder, EdgePlaysEncoder, EdgePlaysReverseEncoder, EdgeRelatesEncoder, EdgeRelatesReverseEncoder, EncodableParametrisedTypeEdge, TypeEdge, TypeEdgeEncoder};
use encoding::graph::type_::property::TypeEdgeProperty;
use encoding::layout::prefix::Prefix;
use storage::key_value::StorageKey;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use crate::type_::{InterfaceImplementation, IntoCanonicalTypeEdge, TypeAPI};
use crate::type_::object_type::ObjectType;
use crate::type_::owns::Owns;
use crate::type_::plays::Plays;
use crate::type_::relates::Relates;
use crate::type_::role_type::RoleType;

pub struct OwnsEncoder { }
impl<'a> EdgeEncoder<'a, Owns<'a>> for OwnsEncoder {
    type ForwardEncoder = EdgeOwnsEncoder;
    type ReverseEncoder = EdgeOwnsReverseEncoder;
}

// pub trait EdgeAnnotationEncoder<'a, E> where
//     E: InterfaceEdge<'a>
// {
//     fn to_type_edge_property_key(edge: E, annotation: E::AnnotationType) -> TypeEdgeProperty<'static> {
//         TypeEdgeProperty::build(edge.clone().into_type_edge(), annotation)
//     }
// }