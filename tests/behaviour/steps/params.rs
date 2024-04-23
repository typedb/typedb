use std::str::FromStr;
use cucumber::Parameter;

#[derive(Debug, Default, Parameter)]
#[param(name = "may_error", regex = "(; throws exception|)")]
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
            "; throws exception" => Self::True,
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
            invalid => return Err(format!("Invalid `MayError`: {invalid}")),
        })
    }
}
