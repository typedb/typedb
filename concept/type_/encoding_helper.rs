use bytes::Bytes;
use encoding::graph::type_::edge::{TypeEdge, edge_constructors::TypeEdgeConstructor, edge_constructors};
use encoding::graph::type_::vertex::TypeVertex;
use encoding::layout::prefix::Prefix;
use storage::key_value::StorageKey;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use crate::type_::{InterfaceEdge, TypeAPI};
use crate::type_::attribute_type::AttributeType;
use crate::type_::object_type::ObjectType;
use crate::type_::owns::Owns;
use crate::type_::plays::Plays;
use crate::type_::role_type::RoleType;
use crate::type_::type_manager::{KindAPI, ReadableType};

pub trait EdgeEncoder<'a, OBJ, ITF, E>
where
    OBJ: TypeAPI<'a> + ReadableType,
    ITF: KindAPI<'a> + ReadableType,
    E: InterfaceEdge<'a, OBJ, ITF>,
{
    type ForwardFactory: TypeEdgeConstructor;
    type ReverseFactory: TypeEdgeConstructor;

    fn read_from<'b>(bytes: Bytes<'b, BUFFER_KEY_INLINE>) -> E
    where 'b : 'a
    {
        let edge = TypeEdge::new(bytes);
        let ret = if edge.prefix() == Self::ForwardFactory::PREFIX {
            let obj = OBJ::new(edge.from().into_owned());
            let itf = ITF::new(edge.to().into_owned());
            E::new(obj, itf)
        } else if edge.prefix() == Self::ReverseFactory::PREFIX {
            let obj = OBJ::new(edge.to().into_owned());
            let itf = ITF::new(edge.from().into_owned());
            E::new(obj, itf)
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

    fn forward_seek_prefix(from: OBJ) -> StorageKey<'a, { TypeEdge::LENGTH_PREFIX_FROM }> {
        Self::ForwardFactory::build_edge_prefix_from(from.into_vertex())
    }

    fn reverse_seek_prefix(from: ITF) -> StorageKey<'a, { TypeEdge::LENGTH_PREFIX_FROM }> {
        Self::ReverseFactory::build_edge_prefix_from(from.into_vertex())
    }
}

pub struct PlaysEncoder { }
impl<'a> EdgeEncoder<'a, ObjectType<'a>, RoleType<'a>, Plays<'a>> for PlaysEncoder {
    type ForwardFactory = edge_constructors::EdgePlays;
    type ReverseFactory = edge_constructors::EdgePlaysReverse;
}

pub struct OwnsEncoder { }
impl<'a> EdgeEncoder<'a, ObjectType<'a>, AttributeType<'a>, Owns<'a>> for OwnsEncoder {
    type ForwardFactory = edge_constructors::EdgeOwns;
    type ReverseFactory = edge_constructors::EdgeOwnsReverse;
}
