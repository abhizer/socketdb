use sqlparser::ast::Expr;

use crate::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Binary {
    Plus,
    Minus,
    Mul,
    Div,
    Rem,
    Eq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    NotEq,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Unary {
    Not,
    Plus,
    Minus,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Literal {
    Int(i32),
    Str(String),
    Bool(bool),
    Float(f32),
    Double(f64),
    Null,
}

impl From<String> for Literal {
    fn from(value: String) -> Self {
        if value.to_lowercase() == "null" {
            return Self::Null;
        }

        if let Ok(v) = value.parse::<bool>() {
            return Self::Bool(v);
        }

        if let Ok(v) = value.parse::<i32>() {
            return Self::Int(v);
        }

        if let Ok(v) = value.parse::<f32>() {
            return Self::Float(v);
        }

        if let Ok(v) = value.parse::<f64>() {
            return Self::Double(v);
        }

        Self::Str(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Ident {
    Wildcard,
    Named(String),
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Expression {
    Literal(Literal),
    Ident(Ident),
    IsFalse(Box<Expression>),
    IsTrue(Box<Expression>),
    IsNull(Box<Expression>),
    IsNotNull(Box<Expression>),
    Unary {
        operator: Unary,
        expression: Box<Expression>,
    },
    Binary {
        operator: Binary,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    None,
}

impl Expression {
    pub fn from_expr(expr: Expr) -> Result<Expression, Error> {
        match expr {
            Expr::Value(val) => Ok(Expression::Literal(match val {
                sqlparser::ast::Value::Number(s, _) => Literal::from(s),
                sqlparser::ast::Value::SingleQuotedString(s)
                | sqlparser::ast::Value::EscapedStringLiteral(s)
                | sqlparser::ast::Value::SingleQuotedByteStringLiteral(s)
                | sqlparser::ast::Value::DoubleQuotedByteStringLiteral(s)
                | sqlparser::ast::Value::RawStringLiteral(s)
                | sqlparser::ast::Value::DoubleQuotedString(s) => Literal::Str(s),
                sqlparser::ast::Value::DollarQuotedString(s) => Literal::Str(s.value),
                sqlparser::ast::Value::Boolean(b) => Literal::Bool(b),
                sqlparser::ast::Value::Null => Literal::Null,
                _ => Err(Error::Unsupported(format!("value: {val}")))?,
            })),
            Expr::Identifier(id) => Ok(Self::Ident(Ident::Named(id.to_string()))),
            Expr::IsFalse(inner) | Expr::IsNotTrue(inner) => Ok(Expression::IsFalse(Box::new(
                Expression::from_expr(*inner)?,
            ))),
            Expr::IsNotFalse(inner) | Expr::IsTrue(inner) => {
                Ok(Expression::IsTrue(Box::new(Expression::from_expr(*inner)?)))
            }
            Expr::IsNull(inner) => Ok(Expression::IsNull(Box::new(Expression::from_expr(*inner)?))),
            Expr::IsNotNull(inner) => Ok(Expression::IsNotNull(Box::new(Expression::from_expr(
                *inner,
            )?))),
            Expr::BinaryOp { left, op, right } => Ok(Expression::Binary {
                operator: match op {
                    sqlparser::ast::BinaryOperator::Plus => Binary::Plus,
                    sqlparser::ast::BinaryOperator::Minus => Binary::Minus,
                    sqlparser::ast::BinaryOperator::Multiply => Binary::Mul,
                    sqlparser::ast::BinaryOperator::Divide => Binary::Div,
                    sqlparser::ast::BinaryOperator::Modulo => Binary::Rem,
                    sqlparser::ast::BinaryOperator::Gt => Binary::Gt,
                    sqlparser::ast::BinaryOperator::Lt => Binary::Lt,
                    sqlparser::ast::BinaryOperator::GtEq => Binary::GtEq,
                    sqlparser::ast::BinaryOperator::LtEq => Binary::LtEq,
                    sqlparser::ast::BinaryOperator::Eq => Binary::Eq,
                    sqlparser::ast::BinaryOperator::NotEq => Binary::NotEq,
                    _ => Err(Error::Unsupported(format!("operator: {op}")))?,
                },
                left: Box::new(Expression::from_expr(*left)?),
                right: Box::new(Expression::from_expr(*right)?),
            }),
            Expr::UnaryOp { op, expr } => Ok(Expression::Unary {
                operator: match op {
                    sqlparser::ast::UnaryOperator::Plus => Unary::Plus,
                    sqlparser::ast::UnaryOperator::Minus => Unary::Minus,
                    sqlparser::ast::UnaryOperator::Not => Unary::Not,
                    _ => Err(Error::Unsupported(format!("unary operator: {op}")))?,
                },
                expression: Box::new(Expression::from_expr(*expr)?),
            }),
            _ => Err(Error::Unsupported(format!("expression: {expr}"))),
        }
    }
}
