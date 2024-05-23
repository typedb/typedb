use bytes::Bytes;
use encoding::graph::type_::edge::{EdgeOwnsEncoder, EdgeOwnsReverseEncoder, EdgePlaysEncoder, EdgePlaysReverseEncoder, EdgeRelatesEncoder, EdgeRelatesReverseEncoder, TypeEdge, TypeEdgeEncoder};
use encoding::graph::type_::property::TypeEdgeProperty;
use storage::key_value::StorageKey;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use crate::type_::{InterfaceImplementation, IntoCanonicalTypeEdge, TypeAPI};
use crate::type_::owns::Owns;
use crate::type_::plays::Plays;
use crate::type_::relates::Relates;

pub trait EdgeEncoder<'a, E>
where
    E: InterfaceImplementation<'a>,
{
    type ForwardEncoder: TypeEdgeEncoder;
    type ReverseEncoder: TypeEdgeEncoder;

    fn read_from<'b>(bytes: Bytes<'b, BUFFER_KEY_INLINE>) -> E
    {
        let edge = TypeEdge::new(bytes);
        let ret = if edge.prefix() == Self::ForwardEncoder::PREFIX {
            E::new(
                E::ObjectType::new(edge.from().into_owned()),
                E::InterfaceType::new(edge.to().into_owned())
            )
        } else if edge.prefix() == Self::ReverseEncoder::PREFIX {
            E::new(
                E::ObjectType::new(edge.to().into_owned()),
                E::InterfaceType::new(edge.from().into_owned())
            )
        } else {
            panic!("Unrecognised prefix for type_edge: {:?}. Expected {:?} or {:?}",
                   edge.prefix(),
                   Self::ForwardEncoder::PREFIX,
                   Self::ReverseEncoder::PREFIX
            )
        };
        ret
    }

    fn forward_edge(edge: E) -> TypeEdge<'a> {
        Self::ForwardEncoder::build_edge(edge.clone().object().into_vertex(), edge.clone().interface().into_vertex())
    }

    fn reverse_edge(edge: E) -> TypeEdge<'a> {
        Self::ReverseEncoder::build_edge(edge.clone().interface().into_vertex(), edge.clone().object().into_vertex())
    }

    fn forward_seek_prefix(from: E::ObjectType) -> StorageKey<'a, { TypeEdge::LENGTH_PREFIX_FROM }> {
        Self::ForwardEncoder::build_edge_prefix_from(from.into_vertex())
    }

    fn reverse_seek_prefix(from: E::InterfaceType) -> StorageKey<'a, { TypeEdge::LENGTH_PREFIX_FROM }> {
        Self::ReverseEncoder::build_edge_prefix_from(from.into_vertex())
    }
}

pub struct PlaysEncoder { }
impl<'a> EdgeEncoder<'a, Plays<'a>> for PlaysEncoder {
    type ForwardEncoder = EdgePlaysEncoder;
    type ReverseEncoder = EdgePlaysReverseEncoder;
}

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