// Copyright 2024 WHERE TRUE Technologies.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use datafusion::sql::{
    parser::{DFParser, Statement},
    sqlparser::{keywords::Keyword, tokenizer::Token},
};

use super::exon_copy_statement::ExonCopyToStatement;

pub(crate) struct ExonParser<'a> {
    df_parser: DFParser<'a>,
}

#[derive(Debug)]
pub(crate) enum ExonStatement {
    DFStatement(Box<Statement>),
    ExonCopyTo(ExonCopyToStatement),
}

impl ExonParser<'_> {
    pub fn new(sql: &str) -> crate::Result<Self> {
        let df_parser = DFParser::new(sql)?;
        Ok(Self { df_parser })
    }

    /// Returns true if the next token is `COPY` keyword, false otherwise
    fn is_copy(&self) -> bool {
        matches!(
            self.df_parser.parser.peek_token().token,
            Token::Word(w) if w.keyword == Keyword::COPY
        )
    }

    /// This is the entry point to our parser -- it handles `COPY` statements specially
    /// but otherwise delegates to the existing DataFusion parser.
    pub fn parse_statement(&mut self) -> crate::Result<ExonStatement> {
        if self.is_copy() {
            self.df_parser.parser.next_token(); // COPY
            let df_statement = self.df_parser.parse_copy()?;

            if let Statement::CopyTo(s) = &df_statement {
                match &s.stored_as {
                    Some(v) if v == "FASTA" => Ok(ExonStatement::ExonCopyTo(
                        ExonCopyToStatement::from(s.clone()),
                    )),
                    _ => Ok(ExonStatement::DFStatement(Box::from(df_statement))),
                }
            } else {
                Ok(ExonStatement::DFStatement(Box::from(df_statement)))
            }
        } else {
            let df_statement = self.df_parser.parse_statement()?;
            Ok(ExonStatement::DFStatement(Box::from(df_statement)))
        }
    }
}
