/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![allow(unexpected_cfgs, reason = "features defined in Bazel targets aren't currently communicated to Cargo")]

mod define;
mod delete;
mod disjunction;
mod expressions;
mod fetch;
mod insert;
mod r#match;
mod modifiers;
mod negation;
mod optional;
mod pipelines;
mod put;
mod redefine;
mod reduce;
mod undefine;
mod update;
