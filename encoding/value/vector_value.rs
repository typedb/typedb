/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::borrow::Cow;

use crate::value::value_type::{VectorPrecision, VectorTypeParameters};

#[derive(Debug, Clone, PartialEq)]
pub struct VectorValue<'a> {
    parameters: VectorTypeParameters,
    elements: Cow<'a, [f32]>,
}

impl<'a> VectorValue<'a> {
    pub fn new(elements: Cow<'a, [f32]>, precision: VectorPrecision) -> Option<Self> {
        let length = u16::try_from(elements.len()).ok()?;
        Some(Self { parameters: VectorTypeParameters::new(length, precision), elements })
    }

    pub fn parameters(&self) -> VectorTypeParameters {
        self.parameters
    }

    pub fn elements(&self) -> &[f32] {
        &self.elements
    }

    pub fn into_elements(self) -> Cow<'a, [f32]> {
        self.elements
    }

    pub fn as_reference(&self) -> VectorValue<'_> {
        VectorValue { parameters: self.parameters, elements: Cow::Borrowed(&self.elements) }
    }

    pub fn into_owned(self) -> VectorValue<'static> {
        VectorValue { parameters: self.parameters, elements: Cow::Owned(self.elements.into_owned()) }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::VectorValue;
    use crate::value::value_type::{VectorPrecision, VectorTypeParameters};

    #[test]
    fn parameters_follow_the_elements() {
        let vector = VectorValue::new(Cow::Owned(vec![1.0, 2.0, 3.0]), VectorPrecision::Float32).unwrap();
        assert_eq!(vector.parameters(), VectorTypeParameters::new(3, VectorPrecision::Float32));
    }

    #[test]
    fn vector_longer_than_the_maximum_length_is_rejected() {
        let max_length = VectorTypeParameters::MAX_LENGTH as usize;
        assert!(VectorValue::new(Cow::Owned(vec![1.0; max_length]), VectorPrecision::Float32).is_some());
        assert!(VectorValue::new(Cow::Owned(vec![1.0; max_length + 1]), VectorPrecision::Float32).is_none());
    }
}
