use std::str::FromStr;
use cucumber::Parameter;
use concept::type_::annotation::AnnotationDistinct;
use concept::type_::owns::OwnsAnnotation;
use encoding::{
    graph::type_::Kind as TypeDBTypeKind,
    value::{
        label::Label as TypeDBLabel,
        value_type::ValueType as TypeDBValueType,
    },
};

#[derive(Debug, Default, Parameter)]
#[param(name = "may_error", regex = "(throws exception|)")]
pub(crate) enum MayError {
    #[default]
    False,
    True,
}

impl MayError {
    pub fn check<T,E>(&self, res: &Result<T,E>) {
        match self {
            MayError::False => assert!(res.is_ok()),
            MayError::True => assert!(res.is_err()),
        };
    }
}

impl FromStr for MayError {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "throws exception" => Self::True,
            "" => Self::False,
            invalid => return Err(format!("Invalid `MayError`: {invalid}")),
        })
    }
}


#[derive(Debug, Default, Parameter)]
#[param(name = "boolean", regex = "(true|false)")]
pub(crate) enum Boolean {
    #[default]
    False,
    True,
}

impl Boolean {
    pub fn check(&self, res: bool) {
        match self {
            Boolean::False => assert_eq!(false, res),
            Boolean::True => assert_eq!(true, res),
        };
    }
}

impl FromStr for Boolean {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "true" => Self::True,
            "false" => Self::False,
            invalid => return Err(format!("Invalid `Boolean`: {invalid}")),
        })
    }
}

#[derive(Debug, Default, Parameter)]
#[param(name = "contains_or_doesnt", regex = "(contain|do not contain)")]
pub(crate) enum ContainsOrDoesnt {
    #[default]
    Contains,
    DoesNotContain,
}

impl ContainsOrDoesnt {
    pub fn check<T: PartialEq + std::fmt::Display>(&self, expected: Vec<T>, actual: Vec<T>) {
        let expected_contains: bool = match self {
            ContainsOrDoesnt::Contains => true,
            ContainsOrDoesnt::DoesNotContain => false,
        };

        expected.iter().for_each(|expected_item| {
            println!("Check contains({}) : {} = {}", expected_item, expected_contains, actual.contains(&expected_item));
            assert_eq!(expected_contains, actual.contains(&expected_item));
        });
    }
}

impl FromStr for ContainsOrDoesnt {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "contain" => Self::Contains,
            "do not contain" => Self::DoesNotContain,
            invalid => return Err(format!("Invalid `ContainsOrDoesnt`: {invalid}")),
        })
    }
}


#[derive(Debug, Parameter)]
#[param(name = "type_label", regex = r"[A-Za-z0-9_\-]+")]
pub(crate) struct Label {
    label_string: String,
}

impl Default for Label {
    fn default() -> Self {
        unreachable!("Why is default called?");
    }
}

impl Label {
    pub fn to_typedb(&self) -> TypeDBLabel<'static> {
        TypeDBLabel::build(&self.label_string)
    }
}

impl FromStr for Label {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok( Self { label_string: s.to_string() })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "root_label", regex = r"(attribute|entity|relation)")]
pub(crate) struct RootLabel {
    kind: TypeDBTypeKind,
}

impl RootLabel {
    pub fn to_typedb(&self) -> TypeDBTypeKind {
         self.kind
    }
}

impl FromStr for RootLabel {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let kind = match s {
            "attribute" => TypeDBTypeKind::Attribute,
            "entity" => TypeDBTypeKind::Entity,
            "relation" => TypeDBTypeKind::Relation,
            _ => unreachable!()
        };
        Ok(RootLabel { kind })
    }
}


#[derive(Debug, Default, Parameter)]
#[param(name = "optional_override", regex = r"(| as [A-Za-z0-9_\-]+)")]
pub(crate) struct OptionalOverride {
    optional_override: Option<TypeDBLabel<'static>>
}

impl FromStr for OptionalOverride {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with(" as ") {
            let label_str = TypeDBLabel::build(s.strip_prefix(" as ").unwrap());
            Ok(Self { optional_override: Some( label_str ) })
        } else if s.is_empty() {
            Ok(Self { optional_override: None })
        } else {
            return Err(format!("Invalid `OptionalOverride`: {}", s))
        }
    }
}



#[derive(Debug, Default, Parameter)]
#[param(name = "value_type", regex = "(boolean|long|double|string|datetime)")]
pub(crate) enum ValueType {
    #[default]
    Boolean,
    Long,
    Double,
    String,
    DateTime
}

impl ValueType {
    pub fn to_typedb(&self) -> TypeDBValueType {
        match self {
            ValueType::Boolean => TypeDBValueType::Boolean,
            ValueType::Long => TypeDBValueType::Long,
            ValueType::Double => TypeDBValueType::Double,
            ValueType::String => TypeDBValueType::String,
            ValueType::DateTime => todo!(),
        }
    }
}

impl FromStr for ValueType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "long" => Self::Long,
            "string" => Self::String,
            "boolean" => Self::Boolean,
            "double" => Self::Double,
            "datetime" => Self::DateTime,
            _ => panic!("Unrecognised value type")
        })
    }
}


#[derive(Debug, Default, Parameter)]
#[param(name = "with_annotations", regex = r"(with annotations: [a-z\\,\\s]+|)")]
pub(crate) struct WithAnnotations {
    annotation_list : Vec<String>,
}

impl WithAnnotations {
    pub(crate) fn to_owns(&self) -> Vec<OwnsAnnotation> {
        self.annotation_list.iter().map(|annotation_str| {
            todo!()
        }).collect::<Vec<OwnsAnnotation>>()
    }
}
impl FromStr for WithAnnotations {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("with annotations:") {
            let (_, annotations_str) = s.split_once(":").unwrap();
            let annotation_list = annotations_str.split(",").map(|s| { s.trim().to_string() }).collect::<Vec<String>>();
            Ok(Self { annotation_list  })
        } else {
            Ok(Self { annotation_list : vec!() })
        }
    }
}
