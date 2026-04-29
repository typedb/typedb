/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{future::Future, pin::Pin};

use crate::{AdminClient, error::AdminError};

pub type Result<T> = std::result::Result<T, AdminError>;
pub type CommandResult = Result<()>;

pub struct CommandContext<'a> {
    pub client: &'a mut AdminClient,
    pub address: &'a str,
    pub args: &'a [String],
}

pub struct CommandDefinition {
    pub tokens: &'static [&'static str],
    pub description: &'static str,
    pub args: &'static [&'static str],
    pub executor: for<'a> fn(CommandContext<'a>) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>>,
}

pub struct CommandRegistry {
    commands: Vec<CommandDefinition>,
}

impl CommandRegistry {
    pub fn new() -> Self {
        Self { commands: Vec::new() }
    }

    pub fn register(mut self, command: CommandDefinition) -> Self {
        self.commands.push(command);
        self
    }

    pub fn commands(&self) -> &[CommandDefinition] {
        &self.commands
    }

    pub fn find(&self, input_tokens: &[&str]) -> Option<(&CommandDefinition, Vec<String>)> {
        self.commands
            .iter()
            .filter(|cmd| input_tokens.len() >= cmd.tokens.len())
            .filter(|cmd| cmd.tokens.iter().zip(input_tokens).all(|(a, b)| *a == *b))
            .max_by_key(|cmd| cmd.tokens.len())
            .map(|cmd| {
                let args: Vec<String> = input_tokens[cmd.tokens.len()..].iter().map(|s| s.to_string()).collect();
                (cmd, args)
            })
    }

    pub fn completions(&self, partial: &str) -> Vec<String> {
        let tokens: Vec<&str> = partial.split_whitespace().collect();
        self.commands
            .iter()
            .filter(|cmd| {
                if tokens.is_empty() {
                    return true;
                }
                cmd.tokens.iter().zip(&tokens).all(|(a, b)| a.starts_with(b) || a == b)
            })
            .map(|cmd| cmd.tokens.join(" "))
            .collect()
    }

    pub fn help_text(&self) -> String {
        let max_usage_width = self
            .commands
            .iter()
            .map(|cmd| {
                let usage = format_usage(cmd);
                usage.len()
            })
            .max()
            .unwrap_or(0);
        let width = max_usage_width + 4;

        self.commands
            .iter()
            .map(|cmd| {
                let usage = format_usage(cmd);
                format!("  {:<width$}{}", usage, cmd.description, width = width)
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn format_usage(cmd: &CommandDefinition) -> String {
    let mut usage = cmd.tokens.join(" ");
    for arg in cmd.args {
        usage.push(' ');
        usage.push('<');
        usage.push_str(arg);
        usage.push('>');
    }
    usage
}
