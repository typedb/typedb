/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

use ::typeql::common::Spannable;
use resource::constants::common::{ERROR_QUERY_POINTER_LINES_AFTER, ERROR_QUERY_POINTER_LINES_BEFORE};

mod typeql;

pub trait TypeDBError {
    fn variant_name(&self) -> &'static str;

    fn component(&self) -> &'static str;

    fn code(&self) -> &'static str;

    fn code_prefix(&self) -> &'static str;

    fn code_number(&self) -> usize;

    fn format_description(&self) -> String;

    fn source_error(&self) -> Option<&(dyn Error + Sync)>;

    fn source_typedb_error(&self) -> Option<&(dyn TypeDBError + Sync)>;

    fn root_source_typedb_error(&self) -> &(dyn TypeDBError + Sync)
    where
        Self: Sized + Sync,
    {
        let mut error: &(dyn TypeDBError + Sync) = self;
        while let Some(source) = error.source_typedb_error() {
            error = source;
        }
        error
    }

    fn source_query(&self) -> Option<&str>;

    fn source_span(&self) -> Option<::typeql::common::Span>;

    fn format_code_and_description(&self) -> String {
        if let Some(query) = self.source_query() {
            if let Some((line_col, _)) = self.bottom_source_span().map(|span| query.line_col(span)).flatten() {
                if let Some(excerpt) = query.extract_annotated_line_col(
                    // note: span line and col are 1-indexed,must adjust to 0-offset
                    line_col.line as usize - 1,
                    line_col.column as usize - 1,
                    ERROR_QUERY_POINTER_LINES_BEFORE,
                    ERROR_QUERY_POINTER_LINES_AFTER,
                ) {
                    return format!(
                        "[{}] {}\nNear {}:{}\n-----\n{}\n-----",
                        self.code(),
                        self.format_description(),
                        line_col.line,
                        line_col.column,
                        excerpt
                    );
                }
            }
        }
        format!("[{}] {}", self.code(), self.format_description())
    }

    // return most-specific span available
    fn bottom_source_span(&self) -> Option<::typeql::common::Span> {
        self.source_typedb_error().map(|err| err.bottom_source_span()).flatten().or_else(|| self.source_span())
    }
}

impl PartialEq for dyn TypeDBError {
    fn eq(&self, other: &Self) -> bool {
        self.code() == other.code()
    }
}

impl Eq for dyn TypeDBError {}

impl fmt::Debug for dyn TypeDBError + '_ {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for dyn TypeDBError + '_ {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(source) = self.source_typedb_error() {
            write!(f, "{}\nCause: \n      {:?}", self.format_code_and_description(), source as &dyn TypeDBError)
        } else if let Some(source) = self.source_error() {
            write!(f, "{}\nCause: \n      {:?}", self.format_code_and_description(), source)
        } else {
            write!(f, "{}", self.format_code_and_description())
        }
    }
}

impl<T: TypeDBError> TypeDBError for Box<T> {
    fn variant_name(&self) -> &'static str {
        (**self).variant_name()
    }

    fn component(&self) -> &'static str {
        (**self).component()
    }

    fn code(&self) -> &'static str {
        (**self).code()
    }

    fn code_prefix(&self) -> &'static str {
        (**self).code_prefix()
    }

    fn code_number(&self) -> usize {
        (**self).code_number()
    }

    fn format_description(&self) -> String {
        (**self).format_description()
    }

    fn source_error(&self) -> Option<&(dyn Error + Sync)> {
        (**self).source_error()
    }

    fn source_typedb_error(&self) -> Option<&(dyn TypeDBError + Sync)> {
        (**self).source_typedb_error()
    }

    fn source_query(&self) -> Option<&str> {
        (**self).source_query()
    }

    fn source_span(&self) -> Option<::typeql::common::Span> {
        (**self).source_span()
    }
}

// ***USAGE WARNING***: We should not set both Source and TypeDBSource, TypeDBSource has precedence!
#[macro_export]
macro_rules! typedb_error {
    ($vis:vis $name:ident(component = $component:literal, prefix = $prefix:literal) { $(
        $variant:ident($number:literal, $description:literal $(, $($arg:tt)*)?),
    )*}) => {
        #[derive(Clone)]
        $vis enum $name {
            $($variant { $($($arg)*)? }),*
        }

        const _: () = {
            // fail to compile if any Numbers are the same
            trait Assert {}
            $(impl Assert for [(); $number ] {})*
        };

        impl $crate::TypeDBError for $name {
            fn variant_name(&self) -> &'static str {
                match self {
                    $(Self::$variant { .. } => stringify!($variant),)*
                }
            }

            fn component(&self) -> &'static str {
                &$component
            }

            fn code(&self) -> &'static str {
                match self {
                    $(Self::$variant { .. } => concat!($prefix, stringify!($number)),)*
                }
            }

            fn code_prefix(&self) -> &'static str {
                $prefix
            }

            fn code_number(&self) -> usize {
                match self {
                    $(Self::$variant { .. } => $number,)*
                }
            }

            fn format_description(&self) -> String {
                match self {
                    $(typedb_error!(@args $variant { $($($arg)*)? }) => format!($description),)*
                }
            }

            fn source_error(&self) -> Option<&(dyn ::std::error::Error + Sync + 'static)> {
                match self {
                    $(typedb_error!(@source source from $variant { $($($arg)*)? })=> {
                        typedb_error!(@source source { $($($arg)*)? })
                    })*
                }
            }

            fn source_typedb_error(&self) -> Option<&(dyn $crate::TypeDBError + Sync + 'static)> {
                match self {
                    $(typedb_error!(@typedb_source typedb_source from $variant { $($($arg)*)? })=> {
                        typedb_error!(@typedb_source typedb_source { $($($arg)*)? })
                    })*
                }
            }

            fn source_query(&self) -> Option<&str> {
                match self {
                    $(typedb_error!(@source_query source_query from $variant { $($($arg)*)? })=> {
                        typedb_error!(@source_query source_query { $($($arg)*)? })
                    })*
                }
            }

            fn source_span(&self) -> Option<::typeql::common::Span> {
                match self {
                    $(typedb_error!(@source_span source_span from $variant { $($($arg)*)? })=> {
                        typedb_error!(@source_span source_span { $($($arg)*)? })
                    })*
                }
            }
        }

        impl ::std::fmt::Debug for $name {
        fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                ::std::fmt::Debug::fmt(self as &dyn $crate::TypeDBError, f)
            }
        }
    };

    (@args $variant:ident { $($arg:ident : $ty:ty),* $(,)? }) => {
        Self::$variant { $($arg),* }
    };

    (@source $ts:ident from $variant:ident { source : $argty:ty $(, $($rest:tt)*)? }) => {
        Self::$variant { source: $ts, .. }
    };
    (@source $ts:ident from $variant:ident { $arg:ident : $argty:ty $(, $($rest:tt)*)? }) => {
        typedb_error!(@source $ts from $variant { $($($rest)*)? })
    };
    (@source $ts:ident from $variant:ident { $(,)? }) => {
        Self::$variant { .. }
    };

    (@source $ts:ident { source: $_:ty $(, $($rest:tt)*)? }) => {
        Some($ts as &(dyn ::std::error::Error + Sync + 'static))
    };
    (@source $ts:ident { $arg:ident : $argty:ty $(, $($rest:tt)*)? }) => {
        typedb_error!(@source $ts { $($($rest)*)? })
    };
    (@source $ts:ident { $(,)? }) => {
        None
    };

    (@typedb_source $ts:ident from $variant:ident { typedb_source : $argty:ty $(, $($rest:tt)*)? }) => {
        Self::$variant { typedb_source: $ts, .. }
    };
    (@typedb_source $ts:ident from $variant:ident { $arg:ident : $argty:ty $(, $($rest:tt)*)? }) => {
        typedb_error!(@typedb_source $ts from $variant { $($($rest)*)? })
    };
    (@typedb_source $ts:ident from $variant:ident { $(,)? }) => {
        Self::$variant { .. }
    };

    (@typedb_source $ts:ident { typedb_source: $_:ty $(, $($rest:tt)*)? }) => {
        Some($ts as &(dyn $crate::TypeDBError + Sync + 'static))
    };
    (@typedb_source $ts:ident { $arg:ident : $argty:ty $(, $($rest:tt)*)? }) => {
        typedb_error!(@typedb_source $ts { $($($rest)*)? })
    };
    (@typedb_source $ts:ident { $(,)? }) => {
        None
    };

    (@source_query $ts:ident from $variant:ident { source_query : $argty:ty $(, $($rest:tt)*)? }) => {
        Self::$variant { source_query: $ts, .. }
    };
    (@source_query $ts:ident from $variant:ident { $arg:ident : $argty:ty $(, $($rest:tt)*)? }) => {
        typedb_error!(@source_query $ts from $variant { $($($rest)*)? })
    };
    (@source_query $ts:ident from $variant:ident { $(,)? }) => {
        Self::$variant { .. }
    };

    (@source_query $ts:ident { source_query: $_:ty $(, $($rest:tt)*)? }) => {
        Some($ts)
    };
    (@source_query $ts:ident { $arg:ident : $argty:ty $(, $($rest:tt)*)? }) => {
        typedb_error!(@source_query $ts { $($($rest)*)? })
    };
    (@source_query $ts:ident { $(,)? }) => {
        None
    };

    (@source_span $ts:ident from $variant:ident { source_span : $argty:ty $(, $($rest:tt)*)? }) => {
        Self::$variant { source_span: $ts, .. }
    };
    (@source_span $ts:ident from $variant:ident { $arg:ident : $argty:ty $(, $($rest:tt)*)? }) => {
        typedb_error!(@source_span $ts from $variant { $($($rest)*)? })
    };
    (@source_span $ts:ident from $variant:ident { $(,)? }) => {
        Self::$variant { .. }
    };

    (@source_span $ts:ident { source_span: $_:ty $(, $($rest:tt)*)? }) => {
        *$ts
    };
    (@source_span $ts:ident { $arg:ident : $argty:ty $(, $($rest:tt)*)? }) => {
        typedb_error!(@source_span $ts { $($($rest)*)? })
    };
    (@source_span $ts:ident { $(,)? }) => {
        None
    };
}

// Check for usages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnimplementedFeature {
    Optionals,
    Lists,
    Structs,

    BuiltinFunction(String),
    LetInBuiltinCall,
    Subkey,

    ComparatorContains,
    ComparatorLike,
    UnsortedJoin,

    PipelineStageInFunction(&'static str),

    IrrelevantUnboundInvertedMode(&'static str),
}
impl std::fmt::Display for UnimplementedFeature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[macro_export]
macro_rules! unimplemented_feature {
    ($feature:ident) => {
        unreachable!(
            "FATAL: entered unreachable code that relies on feature: {}. This is a bug!",
            error::UnimplementedFeature::$feature
        )
    };
    ($feature:ident, $msg:literal) => {
        unreachable!(
            "FATAL: entered unreachable code that relies on feature: {}. This is a bug! Details: {}",
            error::UnimplementedFeature::$feature,
            $msg
        )
    };
}

#[macro_export]
macro_rules! todo_must_implement {
    ($msg:literal) => {
        compile_error!(concat!("TODO: Must implement: ", $msg)) // Ensure this is enabled when checking in.
    };
}

#[macro_export]
macro_rules! todo_display_for_error {
    ($f:ident, $self:ident) => {
        write!(
            $f,
            "(Proper formatting has not yet been implemented for {})\nThe error is: {:?}\n",
            std::any::type_name::<Self>(),
            $self
        )
    };
}

#[macro_export]
macro_rules! ensure_unimplemented_unused {
    () => {
        compile_error!("Implement this path before making the function usable")
    };
}

#[macro_export]
macro_rules! needs_update_when_feature_is_implemented {
    // Nothing, we just need the compile error when the feature is deleted
    ($feature:ident) => {};
    ($feature:ident, $msg:literal) => {};
}
