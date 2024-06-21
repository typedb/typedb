/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use answer::variable_value::VariableValue;
use ir::pattern::variable::Variable;

use crate::planner::pattern_plan::{Check, Execution, Iterate, PatternPlan, Single, Step};

pub(crate) struct PatternExecutor {
    plan: PatternPlan,
    variable_positions: HashMap<Variable, Position>,
    steps: Vec<StepExecutor>,
    // modifiers: Modifier,
}

impl PatternExecutor {

    pub(crate) fn new(plan: PatternPlan) -> Self {
        // 1. assign positions based on the output variables of each step
        // 2. create step executors that have an output Batch corresponding to the total size of the variables we care about

        let mut variable_positions = HashMap::new();
        let mut steps = Vec::new();
        for step in plan.into_steps() {
            for variable in step.generated_variables() {
                let previous = variable_positions.insert(*variable, variable_positions.len() as Position);
                debug_assert_eq!(previous, Option::None);
            }
            steps.push(StepExecutor::new(step))
        }

        PatternExecutor {
            plan,
            variable_positions,
            steps,
            // modifiers:
        }
    }

    fn execute(&self) {

    }
}

enum StepExecutor {
    Sorted(SortedExecutor),
    Unsorted(UnsortedExecutor),
    Single(SingleExecutor),

    Disjunction(DisjunctionExecutor),
    Negation(NegationExecutor),
    Optional(OptionalExecutor),
}

impl StepExecutor {
    fn new(step: Step) -> Self {
        let Step { execution: execution, total_variables_count: vars_count, .. } = step;

        match execution {
            Execution::SortedIterators(iterates, sort_var) => {
                Self::Sorted(SortedExecutor::new(iterates, sort_var, vars_count))
            }
            Execution::UnsortedIterator(iterate, checks) => {
                Self::Unsorted(UnsortedExecutor::new(iterate, checks, vars_count))
            }
            Execution::Single(_, _) => {}
            Execution::Disjunction(_) => {}
            Execution::Negation(_) => {}
            Execution::Optional(_) => {}
        }
    }
}

struct SortedExecutor {
    iterates: Vec<Iterate>,
    sort_variable: Variable,
    // iterator:

    output_width: u32,
    output: Option<Batch>,
}

impl SortedExecutor {
    fn new(iterates: Vec<Iterate>, sort_variable: Variable, vars_count: u32) -> Self {
        Self {
            iterates,
            sort_variable,
            output_width: vars_count,
            output: None
        }
    }
}

struct UnsortedExecutor {
    iterate: Iterate,
    checks: Vec<Check>,

    output_width: u32,
    output: Option<Batch>,
}

impl UnsortedExecutor {
    fn new(iterate: Iterate, checks: Vec<Check>, total_vars: u32) -> Self {
        Self {
            iterate,
            checks,
            output_width: total_vars,
            output: None,
        }
    }
}

struct SingleExecutor {
    provider: Single,
    checks: Vec<Check>,
}

struct DisjunctionExecutor {

}

struct NegationExecutor {

}

struct OptionalExecutor {

}

// struct StepExecutor {
//     execution: Execution,
//
//     // iterator:
//
//     // optional optimisation: cache to avoid recomputing unnecessarily?
//
// }
//
// impl StepExecutor {
//     fn new(step_plan: Step) -> Self {
//         let Step { execution: execution, total_variables_count: batch_width, .. } = step_plan;
//
//         Self {
//             execution: execution,
//             output_width: batch_width,
//             output: None,
//         }
//     }
// }


const BATCH_ROWS_MAX: u32 = 64;

struct Batch {
    width: u32,
    data: Vec<VariableValue<'static>>,
}

impl Batch {
    fn new(width: u32) -> Self {
        let size = width * BATCH_ROWS_MAX;
        Batch {
            width,
            data: vec![VariableValue::Empty; size as usize]
        }
    }
}


type Position = u32;
