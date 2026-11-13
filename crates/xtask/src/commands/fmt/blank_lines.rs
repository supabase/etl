//! Blank-line rules for block statements that rustfmt cannot express.
//!
//! rustfmt keeps up to one blank line between statements but never inserts
//! one. This pass inserts the blank lines that set statement paragraphs apart
//! and removes blank lines that pad the inside of a block. It only rewrites
//! whitespace between the elements of block bodies, and rustfmt preserves
//! every blank line this pass keeps, so running both passes reaches a fixed
//! point.
//!
//! Macro bodies are token trees without statements, so they are never
//! touched. Code under `#[rustfmt::skip]` is left alone as well.

use std::{fmt, path::Path};

use anyhow::{Context, Result, bail};
use ra_ap_syntax::{
    AstNode, Edition, NodeOrToken, SourceFile, SyntaxKind, SyntaxNode, SyntaxToken, T, TextRange,
    TextSize,
    ast::{self, BinaryOp, HasName},
};
use xshell::{Shell, cmd};

use crate::{commands::fmt::FormatMode, utils::workspace_root};

/// Attribute path that makes rustfmt leave the annotated code untouched.
const RUSTFMT_SKIP: &str = "rustfmt::skip";
/// Largest block whose `return`, `break`, or `continue` may follow the
/// previous statement directly.
const MAX_CUDDLED_JUMP_STATEMENTS: usize = 2;

/// Blank-line rule broken by the whitespace between two block elements.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Rule {
    /// Multi-line control flow needs a blank line before and after it.
    SeparateParagraph,
    /// A jump needs a blank line above it in a block with more than two
    /// statements.
    SeparateJump,
    /// A block body must not start or end with a blank line.
    PaddedBlock,
}

impl fmt::Display for Rule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::SeparateParagraph => "missing blank line around multi-line control flow",
            Self::SeparateJump => "missing blank line before return, break, or continue",
            Self::PaddedBlock => "blank line at the start or end of a block",
        };

        f.write_str(message)
    }
}

/// Rule violation at a byte offset of the original source.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Violation {
    offset: TextSize,
    rule: Rule,
}

/// Source text with every blank-line violation fixed.
#[derive(Debug)]
pub(super) struct Layout {
    text: String,
    violations: Vec<Violation>,
}

/// Whitespace rewrite that fixes one violation.
struct Fix {
    range: TextRange,
    replacement: String,
    rule: Rule,
}

/// Statement, tail expression, or comment group that is laid out as a whole.
struct Unit {
    /// Range including attached leading and trailing comments.
    range: TextRange,
    kind: UnitKind,
}

/// Layout role of a [`Unit`].
enum UnitKind {
    /// Multi-line control flow that forms its own paragraph.
    Paragraph {
        /// Part of the statement that decides the control flow, where a cuddled
        /// binding must be used.
        head: SyntaxNode,
    },
    /// `let` or assignment that a following paragraph may cuddle with.
    Binding { names: Vec<SyntaxToken> },
    /// `return`, `break`, or `continue`.
    Jump,
    /// Any other statement or tail expression.
    Statement,
    /// Comment group or inner attribute that is not attached to a statement.
    Detached,
}

/// Applies the blank-line rules to every Rust source file of the workspace.
///
/// In [`FormatMode::Check`], prints each violation and fails if any exist.
pub(super) fn run(sh: &Shell, mode: FormatMode) -> Result<()> {
    let root = workspace_root()?;
    let mut violation_count = 0usize;

    for relative_path in workspace_sources(sh, &root)? {
        let path = root.join(&relative_path);
        let source = match std::fs::read_to_string(&path) {
            Ok(source) => source,
            // A tracked file deleted from the working tree has nothing to format.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("Failed to read {relative_path}"));
            }
        };

        let layout =
            layout(&source).with_context(|| format!("Failed to lay out {relative_path}"))?;

        if layout.violations.is_empty() {
            continue;
        }

        match mode {
            FormatMode::Write => std::fs::write(&path, &layout.text)
                .with_context(|| format!("Failed to write {relative_path}"))?,
            FormatMode::Check => {
                for violation in &layout.violations {
                    let line = line_number(&source, violation.offset);
                    println!("{relative_path}:{line}: {}", violation.rule);
                }
            }
        }

        violation_count += layout.violations.len();
    }

    if matches!(mode, FormatMode::Check) && violation_count > 0 {
        bail!("Found {violation_count} blank-line violations; run `cargo x fmt` to fix them");
    }

    Ok(())
}

/// Lays out the blank lines of one Rust source file.
pub(super) fn layout(source: &str) -> Result<Layout> {
    let parse = SourceFile::parse(source, Edition::Edition2024);
    if let Some(error) = parse.errors().first() {
        let line = line_number(source, error.range().start());
        bail!("Failed to parse line {line}: {error}");
    }

    let root = parse.tree();
    let skipped: Vec<TextRange> = root
        .syntax()
        .descendants()
        .filter_map(ast::Attr::cast)
        .filter(|attr| attr.path().is_some_and(|path| path.syntax().text() == RUSTFMT_SKIP))
        .filter_map(|attr| attr.syntax().parent())
        .map(|owner| owner.text_range())
        .collect();

    let mut fixes = Vec::new();

    for list in root.syntax().descendants().filter_map(ast::StmtList::cast) {
        let range = list.syntax().text_range();
        if skipped.iter().any(|skip| skip.contains_range(range)) {
            continue;
        }

        block_fixes(source, &list, &mut fixes);
    }

    fixes.sort_by_key(|fix| fix.range.start());

    let mut text = String::with_capacity(source.len() + fixes.len());
    let mut copied = TextSize::new(0);
    let mut violations = Vec::with_capacity(fixes.len());

    for Fix { range, replacement, rule } in fixes {
        text.push_str(&source[TextRange::new(copied, range.start())]);
        text.push_str(&replacement);
        copied = range.end();
        violations.push(Violation { offset: range.start(), rule });
    }

    text.push_str(&source[usize::from(copied)..]);

    Ok(Layout { text, violations })
}

/// Lists workspace member Rust sources relative to the workspace root,
/// including untracked files that are not ignored.
fn workspace_sources(sh: &Shell, root: &Path) -> Result<Vec<String>> {
    let manifest_path = root.join("Cargo.toml");
    let manifest = std::fs::read_to_string(&manifest_path)
        .with_context(|| format!("Failed to read {}", manifest_path.display()))?;
    let manifest: toml::Table = toml::from_str(&manifest).context("Failed to parse Cargo.toml")?;
    let members = manifest
        .get("workspace")
        .and_then(|workspace| workspace.get("members"))
        .and_then(|members| members.as_array())
        .context("Cargo.toml has no workspace.members array")?;
    let pathspecs = members
        .iter()
        .map(|member| {
            member
                .as_str()
                .map(|member| format!(":(glob){member}/**/*.rs"))
                .context("workspace.members must contain strings")
        })
        .collect::<Result<Vec<_>>>()?;

    let output = cmd!(sh, "git -C {root} ls-files -z --cached --others --exclude-standard")
        .args(["--deduplicate", "--"])
        .args(&pathspecs)
        .read()?;

    Ok(output.split('\0').filter(|path| !path.is_empty()).map(str::to_owned).collect())
}

/// Collects fixes for the whitespace between the elements of one block body.
fn block_fixes(source: &str, list: &ast::StmtList, fixes: &mut Vec<Fix>) {
    let units = units(list);
    let (Some(first), Some(last)) = (units.first(), units.last()) else {
        return;
    };

    let (Some(open), Some(close)) = (list.l_curly_token(), list.r_curly_token()) else {
        return;
    };

    for gap in [
        TextRange::new(open.text_range().end(), first.range.start()),
        TextRange::new(last.range.end(), close.text_range().start()),
    ] {
        let whitespace = &source[gap];
        if let Some(indent_start) = whitespace.rfind('\n')
            && newline_count(whitespace) > 1
        {
            fixes.push(Fix {
                range: gap,
                replacement: whitespace[indent_start..].to_owned(),
                rule: Rule::PaddedBlock,
            });
        }
    }

    let statement_count =
        units.iter().filter(|unit| !matches!(unit.kind, UnitKind::Detached)).count();

    for (previous, next) in units.iter().zip(units.iter().skip(1)) {
        let gap = TextRange::new(previous.range.end(), next.range.start());
        let whitespace = &source[gap];
        debug_assert!(
            whitespace.chars().all(char::is_whitespace),
            "units cover every non-whitespace element of a statement list"
        );

        // Elements on one line are rustfmt's choice, and blank lines already
        // separate the others.
        if newline_count(whitespace) != 1 {
            continue;
        }

        if let Some(rule) = required_separation(previous, next, statement_count) {
            fixes.push(Fix { range: gap, replacement: format!("\n{whitespace}"), rule });
        }
    }
}

/// Returns the rule that requires a blank line between two adjacent units.
fn required_separation(previous: &Unit, next: &Unit, statement_count: usize) -> Option<Rule> {
    if matches!(next.kind, UnitKind::Jump) && statement_count > MAX_CUDDLED_JUMP_STATEMENTS {
        return Some(Rule::SeparateJump);
    }

    match (&previous.kind, &next.kind) {
        (UnitKind::Paragraph { .. }, _) => Some(Rule::SeparateParagraph),
        (UnitKind::Binding { names }, UnitKind::Paragraph { head }) if mentions(head, names) => {
            None
        }
        (_, UnitKind::Paragraph { .. }) => Some(Rule::SeparateParagraph),
        (
            UnitKind::Binding { .. } | UnitKind::Jump | UnitKind::Statement | UnitKind::Detached,
            UnitKind::Binding { .. } | UnitKind::Jump | UnitKind::Statement | UnitKind::Detached,
        ) => None,
    }
}

/// Splits a block body into units, attaching comments to their statements.
///
/// Comments directly above a statement belong to it, and a comment on the
/// line where a statement ends belongs to that statement. Other comment groups
/// are detached units.
fn units(list: &ast::StmtList) -> Vec<Unit> {
    let mut units: Vec<Unit> = Vec::new();
    let mut pending_comments: Option<TextRange> = None;
    // Whether the current element is on the line where the last unit ends.
    let mut on_unit_line = false;

    for element in list.syntax().children_with_tokens() {
        let (range, kind) = match &element {
            NodeOrToken::Token(token) => match token.kind() {
                T!['{'] | T!['}'] => continue,
                SyntaxKind::WHITESPACE => {
                    let newlines = newline_count(token.text());
                    if newlines > 0 {
                        on_unit_line = false;
                    }

                    // A blank line detaches comments from the statement below.
                    if newlines > 1
                        && let Some(range) = pending_comments.take()
                    {
                        units.push(Unit { range, kind: UnitKind::Detached });
                    }

                    continue;
                }
                SyntaxKind::COMMENT => {
                    let range = token.text_range();

                    match units.last_mut() {
                        Some(unit) if on_unit_line && pending_comments.is_none() => {
                            unit.range = unit.range.cover(range);
                        }
                        _ => {
                            pending_comments = Some(
                                pending_comments.map_or(range, |pending| pending.cover(range)),
                            );
                        }
                    }

                    continue;
                }
                _ => (token.text_range(), UnitKind::Statement),
            },
            NodeOrToken::Node(node) => (node.text_range(), classify(node)),
        };

        let start = pending_comments.take().map_or(range.start(), TextRange::start);
        units.push(Unit { range: TextRange::new(start, range.end()), kind });
        on_unit_line = true;
    }

    if let Some(range) = pending_comments {
        units.push(Unit { range, kind: UnitKind::Detached });
    }

    units
}

/// Classifies a statement-list child node.
fn classify(node: &SyntaxNode) -> UnitKind {
    if let Some(statement) = ast::Stmt::cast(node.clone()) {
        return match statement {
            ast::Stmt::LetStmt(let_stmt) => classify_let(&let_stmt),
            ast::Stmt::ExprStmt(expr_stmt) => {
                expr_stmt.expr().map_or(UnitKind::Statement, |expr| classify_expr(&expr, node))
            }
            ast::Stmt::Item(_) => UnitKind::Statement,
        };
    }

    // The only other child nodes are the tail expression and inner attributes.
    match ast::Expr::cast(node.clone()) {
        Some(expr) => classify_expr(&expr, node),
        None => UnitKind::Detached,
    }
}

/// Classifies a `let` statement.
fn classify_let(let_stmt: &ast::LetStmt) -> UnitKind {
    let initializer = let_stmt.initializer();

    if spans_lines(let_stmt.syntax()) {
        let head = match (&initializer, let_stmt.let_else()) {
            (Some(initializer), Some(_)) => Some(initializer.syntax().clone()),
            (Some(initializer), None) => control_flow_head(initializer),
            (None, _) => None,
        };

        if let Some(head) = head {
            return UnitKind::Paragraph { head };
        }
    }

    let names = let_stmt
        .pat()
        .into_iter()
        .flat_map(|pat| pat.syntax().descendants())
        .filter_map(ast::IdentPat::cast)
        .filter_map(|ident| ident.name()?.ident_token())
        .collect();
    UnitKind::Binding { names }
}

/// Classifies an expression statement or tail expression spanning `node`.
fn classify_expr(expr: &ast::Expr, node: &SyntaxNode) -> UnitKind {
    match expr {
        ast::Expr::ReturnExpr(_) | ast::Expr::BreakExpr(_) | ast::Expr::ContinueExpr(_) => {
            UnitKind::Jump
        }
        ast::Expr::BinExpr(bin_expr)
            if matches!(bin_expr.op_kind(), Some(BinaryOp::Assignment { .. })) =>
        {
            match bin_expr.rhs().as_ref().and_then(control_flow_head) {
                Some(head) if spans_lines(node) => UnitKind::Paragraph { head },
                _ => {
                    let names = bin_expr
                        .lhs()
                        .into_iter()
                        .flat_map(|lhs| lhs.syntax().descendants_with_tokens())
                        .filter_map(NodeOrToken::into_token)
                        .filter(is_name_token)
                        .collect();
                    UnitKind::Binding { names }
                }
            }
        }
        _ => match control_flow_head(expr) {
            Some(head) if spans_lines(node) => UnitKind::Paragraph { head },
            _ => UnitKind::Statement,
        },
    }
}

/// Returns the part of a control-flow expression that decides its flow, or
/// `None` if `expr` is not control flow with a block.
fn control_flow_head(expr: &ast::Expr) -> Option<SyntaxNode> {
    let head = match expr {
        ast::Expr::IfExpr(if_expr) => {
            if_expr.condition().map(|condition| condition.syntax().clone())
        }
        ast::Expr::MatchExpr(match_expr) => {
            match_expr.expr().map(|scrutinee| scrutinee.syntax().clone())
        }
        ast::Expr::ForExpr(for_expr) => {
            for_expr.iterable().map(|iterable| iterable.syntax().clone())
        }
        ast::Expr::WhileExpr(while_expr) => {
            while_expr.condition().map(|condition| condition.syntax().clone())
        }
        // Loops and blocks have no head, so their whole body counts.
        ast::Expr::LoopExpr(_) | ast::Expr::BlockExpr(_) => None,
        _ => return None,
    };

    Some(head.unwrap_or_else(|| expr.syntax().clone()))
}

/// Returns whether `head` uses any of the bound `names`, including inside
/// macro arguments and inline format captures.
fn mentions(head: &SyntaxNode, names: &[SyntaxToken]) -> bool {
    let is_bound = |text: &str| names.iter().any(|name| name.text() == text);
    head.descendants_with_tokens().filter_map(NodeOrToken::into_token).any(|token| {
        match token.kind() {
            SyntaxKind::IDENT | T![self] => is_bound(token.text()),
            // `format!("{name}")` uses `name` inside a string literal.
            SyntaxKind::STRING
                if token.parent_ancestors().any(|node| node.kind() == SyntaxKind::TOKEN_TREE) =>
            {
                format_captures(token.text()).any(is_bound)
            }
            _ => false,
        }
    })
}

/// Returns the identifiers captured inline by a format string literal, such
/// as `name` in `"{name}"` or `"{name:?}"`.
fn format_captures(literal: &str) -> impl Iterator<Item = &str> {
    let bytes = literal.as_bytes();
    let mut index = 0;
    std::iter::from_fn(move || {
        while let Some(offset) = bytes.get(index..)?.iter().position(|&byte| byte == b'{') {
            let start = index + offset + 1;
            // `{{` is an escaped brace, not a placeholder.
            if bytes.get(start) == Some(&b'{') {
                index = start + 1;
                continue;
            }

            let length = bytes
                .get(start..)
                .unwrap_or_default()
                .iter()
                .take_while(|&&byte| byte == b'_' || byte.is_ascii_alphanumeric())
                .count();
            let end = start + length;
            index = end;
            let starts_like_identifier =
                bytes.get(start).is_some_and(|&byte| byte == b'_' || byte.is_ascii_alphabetic());
            if starts_like_identifier && matches!(bytes.get(end), Some(b'}' | b':')) {
                // ASCII bytes delimit the range, so it lies on char boundaries.
                return literal.get(start..end);
            }
        }

        None
    })
}

/// Returns whether a token can name a binding.
fn is_name_token(token: &SyntaxToken) -> bool {
    matches!(token.kind(), SyntaxKind::IDENT | T![self])
}

/// Returns whether a node's text contains a line break.
fn spans_lines(node: &SyntaxNode) -> bool {
    node.text().contains_char('\n')
}

/// Counts line breaks in whitespace.
fn newline_count(whitespace: &str) -> usize {
    whitespace.bytes().filter(|&byte| byte == b'\n').count()
}

/// Returns the 1-based line number of a byte offset.
fn line_number(source: &str, offset: TextSize) -> usize {
    newline_count(&source[..usize::from(offset)]) + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Lays out `source` and returns the fixed text and the violated rules.
    fn fixed(source: &str) -> (String, Vec<Rule>) {
        let layout = layout(source).unwrap();
        (layout.text, layout.violations.into_iter().map(|violation| violation.rule).collect())
    }

    #[test]
    fn separates_multi_line_control_flow_from_neighbors() {
        let source = "\
fn f() {
    let a = 1;
    foo();
    if a > 1 {
        bar();
    }
    baz();
}
";
        let expected = "\
fn f() {
    let a = 1;
    foo();

    if a > 1 {
        bar();
    }

    baz();
}
";
        assert_eq!(fixed(source), (expected.to_owned(), vec![Rule::SeparateParagraph; 2]));
    }

    #[test]
    fn keeps_control_flow_cuddled_at_block_edges() {
        let source = "\
fn f() {
    match x {
        _ => {
            for item in items {
                foo(item);
            }
        }
    }
}
";
        assert_eq!(fixed(source), (source.to_owned(), vec![]));
    }

    #[test]
    fn removes_blank_lines_padding_a_block() {
        let source = "fn f() {\n\n    foo();\n\n}\n";
        assert_eq!(
            fixed(source),
            ("fn f() {\n    foo();\n}\n".to_owned(), vec![Rule::PaddedBlock; 2])
        );
    }

    #[test]
    fn allows_cuddling_with_a_binding_used_in_the_head() {
        let source = "\
fn f() {
    foo();
    let value = compute();
    if value > 1 {
        bar();
    }
    total = 0;
    while total < 10 {
        total += 1;
    }
    let ready = state.ready;
    if matches!(ready, Some(_)) {
        bar();
    }
}
";
        let expected = "\
fn f() {
    foo();
    let value = compute();
    if value > 1 {
        bar();
    }

    total = 0;
    while total < 10 {
        total += 1;
    }

    let ready = state.ready;
    if matches!(ready, Some(_)) {
        bar();
    }
}
";
        assert_eq!(fixed(source), (expected.to_owned(), vec![Rule::SeparateParagraph; 2]));
    }

    #[test]
    fn allows_cuddling_with_a_binding_captured_by_a_format_string() {
        let source = r#"fn f() {
    let open = "[".repeat(3);
    for input in [format!("{{x}} {open:?}")] {
        check(input);
    }
    let close = "]";
    if log_enabled!("{{close}}") {
        bar();
    }
}
"#;
        let expected = r#"fn f() {
    let open = "[".repeat(3);
    for input in [format!("{{x}} {open:?}")] {
        check(input);
    }

    let close = "]";

    if log_enabled!("{{close}}") {
        bar();
    }
}
"#;
        assert_eq!(fixed(source), (expected.to_owned(), vec![Rule::SeparateParagraph; 2]));
    }

    #[test]
    fn rejects_cuddling_with_a_binding_used_only_in_the_body() {
        let source = "\
fn f() {
    let value = compute();
    if ready {
        use_it(value);
    }
}
";
        let expected = "\
fn f() {
    let value = compute();

    if ready {
        use_it(value);
    }
}
";
        assert_eq!(fixed(source), (expected.to_owned(), vec![Rule::SeparateParagraph]));
    }

    #[test]
    fn treats_multi_line_let_else_and_control_flow_bindings_as_paragraphs() {
        let source = "\
fn f() {
    let Some(value) = input else {
        return;
    };
    let kind = match value {
        0 => Kind::Zero,
        _ => Kind::Other,
    };
    let Some(single) = input else { return };
    let next = single;
}
";
        let expected = "\
fn f() {
    let Some(value) = input else {
        return;
    };

    let kind = match value {
        0 => Kind::Zero,
        _ => Kind::Other,
    };

    let Some(single) = input else { return };
    let next = single;
}
";
        assert_eq!(fixed(source), (expected.to_owned(), vec![Rule::SeparateParagraph; 2]));
    }

    #[test]
    fn separates_jumps_only_in_blocks_with_more_than_two_statements() {
        let source = "\
fn f() {
    loop {
        let value = next();
        break;
    }
    foo();
    bar();
    return;
}
";
        let expected = "\
fn f() {
    loop {
        let value = next();
        break;
    }

    foo();
    bar();

    return;
}
";
        assert_eq!(
            fixed(source),
            (expected.to_owned(), vec![Rule::SeparateParagraph, Rule::SeparateJump])
        );
    }

    #[test]
    fn puts_the_blank_line_above_comments_attached_to_a_statement() {
        let source = "\
fn f() {
    foo(); // trailing
    // Explains the branch.
    if ready {
        bar();
    } // after
    baz();
}
";
        let expected = "\
fn f() {
    foo(); // trailing

    // Explains the branch.
    if ready {
        bar();
    } // after

    baz();
}
";
        assert_eq!(fixed(source), (expected.to_owned(), vec![Rule::SeparateParagraph; 2]));
    }

    #[test]
    fn skips_macro_bodies_and_rustfmt_skip() {
        let source = "\
fn f() {
    tokio::select! {
        _ = a => {
            foo();
            if ready {
                bar();
            }
        }
    }
}

#[rustfmt::skip]
fn g() {

    foo();
    if ready {
        bar();
    }
}
";
        assert_eq!(fixed(source), (source.to_owned(), vec![]));
    }

    #[test]
    fn layout_is_idempotent() {
        let source = "\
fn f() {

    let value = compute();
    foo();
    if value {
        bar();
    }
    loop {
        a();
        b();
        break;
    }
    return;

}
";
        let (once, _) = fixed(source);
        assert_eq!(fixed(&once), (once.clone(), vec![]));
    }

    #[test]
    fn rejects_unparseable_source() {
        assert!(layout("fn f() {").is_err());
    }
}
