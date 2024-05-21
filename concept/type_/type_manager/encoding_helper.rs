use bytes::Bytes;
use encoding::graph::type_::edge::{TypeEdge, edge_constructors::TypeEdgeConstructor, edge_constructors};
use encoding::graph::type_::property::TypeEdgeProperty;
use storage::key_value::StorageKey;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use crate::type_::{InterfaceEdge, IntoCanonicalTypeEdge, TypeAPI};
use crate::type_::owns::Owns;
use crate::type_::plays::Plays;

pub trait EdgeEncoder<'a, E>
where
    E: InterfaceEdge<'a>,
{
    type ForwardFactory: TypeEdgeConstructor;
    type ReverseFactory: TypeEdgeConstructor;

    fn read_from<'b>(bytes: Bytes<'b, BUFFER_KEY_INLINE>) -> E
    {
        let edge = TypeEdge::new(bytes);
        let ret = if edge.prefix() == Self::ForwardFactory::PREFIX {
            E::new(
                E::ObjectType::new(edge.from().into_owned()),
                E::InterfaceType::new(edge.to().into_owned())
            )
        } else if edge.prefix() == Self::ReverseFactory::PREFIX {
            E::new(
                E::ObjectType::new(edge.to().into_owned()),
                E::InterfaceType::new(edge.from().into_owned())
            )
        } else {
            panic!("Unrecognised prefix for type_edge: {:?}. Expected {:?} or {:?}",
                    edge.prefix(),
                    Self::ForwardFactory::PREFIX,
                    Self::ReverseFactory::PREFIX
            )
        };
        ret
    }

    fn forward_edge(edge: E) -> TypeEdge<'a> {
        Self::ForwardFactory::build_edge(edge.clone().object().into_vertex(), edge.clone().interface().into_vertex())
    }

    fn reverse_edge(edge: E) -> TypeEdge<'a> {
        Self::ReverseFactory::build_edge(edge.clone().interface().into_vertex(), edge.clone().object().into_vertex())
    }

    fn forward_seek_prefix(from: E::ObjectType) -> StorageKey<'a, { TypeEdge::LENGTH_PREFIX_FROM }> {
        Self::ForwardFactory::build_edge_prefix_from(from.into_vertex())
    }

    fn reverse_seek_prefix(from: E::InterfaceType) -> StorageKey<'a, { TypeEdge::LENGTH_PREFIX_FROM }> {
        Self::ReverseFactory::build_edge_prefix_from(from.into_vertex())
    }
}

pub struct PlaysEncoder { }
impl<'a> EdgeEncoder<'a, Plays<'a>> for PlaysEncoder {
    type ForwardFactory = edge_constructors::EdgePlays;
    type ReverseFactory = edge_constructors::EdgePlaysReverse;
}

pub struct OwnsEncoder { }
impl<'a> EdgeEncoder<'a, Owns<'a>> for OwnsEncoder {
    type ForwardFactory = edge_constructors::EdgeOwns;
    type ReverseFactory = edge_constructors::EdgeOwnsReverse;
}

pub trait EdgeAnnotationEncoder<'a, E> where
    E: InterfaceEdge<'a>
{
    fn to_type_edge_property_key(edge: E, annotation: E::AnnotationType) -> TypeEdgeProperty<'static> {
        TypeEdgeProperty::build(edge.clone().into_type_edge(), annotation)
    }
}