use concept::thing::thing_manager::ThingManager;
use storage::snapshot::WritableSnapshot;
use crate::planner::insert_planner::VariablePosition;

#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub enum VariableSource {
    TypeSource(TypeSource),
    ValueSource(ValueSource),
    ThingSource(ThingSource),
}

#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub enum TypeSource {
    Input(VariablePosition),
    TypeConstant(usize),
}

#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub enum ValueSource {
    Input(VariablePosition),
    ValueConstant(usize),
}

#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub enum ThingSource {
    Input(VariablePosition),
    Inserted(usize),
}

// We aim to generalise Insert, Delete, Update and Put using this trait.
// ConceptSource is for injecting ExecutionConcepts.
pub trait WriteInstruction<ConceptSource> {
    type CheckError;
    type InsertError;
    type DeleteError;
    fn check(snapshot: &impl WritableSnapshot, thing_manager: &ThingManager, concepts: &mut ConceptSource) -> Result<(), Self::CheckError>;
    fn insert(snapshot: &impl WritableSnapshot, thing_manager: &ThingManager, concepts: &mut ConceptSource) -> Result<(), Self::InsertError>;
    fn delete(snapshot: &impl WritableSnapshot, thing_manager: &ThingManager, concepts: &ConceptSource) -> Result<(), Self::DeleteError>;
}

#[derive(Debug)]
pub struct IsaEntity {
    pub type_: TypeSource
}

#[derive(Debug)]
pub struct IsaAttribute {
    pub type_: TypeSource,
    pub value: ValueSource
}

#[derive(Debug)]
pub struct IsaRelation {
    pub type_: TypeSource
}

#[derive(Debug)]
pub struct Has {
    pub owner: ThingSource,
    pub attribute: ThingSource
}

#[derive(Debug)]
pub struct RolePlayer {
    pub relation: ThingSource,
    pub player: ThingSource,
    pub role: TypeSource
}
