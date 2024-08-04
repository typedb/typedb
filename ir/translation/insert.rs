use crate::{
    program::{
        block::{FunctionalBlock, FunctionalBlockBuilder},
        function_signature::FunctionSignatureIndex,
    },
    translator::constraints::add_statement,
    PatternDefinitionError,
};

pub fn translate_insert(
    function_index: &impl FunctionSignatureIndex,
    insert: &typeql::query::stage::Insert,
) -> Result<FunctionalBlockBuilder, PatternDefinitionError> {
    let mut builder = FunctionalBlock::builder();
    for statement in &insert.statements {
        add_statement(function_index, &mut builder.conjunction_mut().constraints_mut(), statement)?;
    }
    Ok(builder)
}
