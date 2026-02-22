---
name: pre-commit-review
description: Reviews code changes before commit for correctness, security, style, and project conventions. Use when the user wants a code review before committing, asks to review staged/unstaged changes, or says "review before commit" or "pre-commit review."
---

# Pre-Commit Code Review

Review all changed files (staged and optionally unstaged) before the user commits. Focus on catching bugs, security issues, and convention violations so the commit is safe and consistent.

## When to Apply

- User asks for a "code review before commit," "pre-commit review," or "review my changes"
- User wants staged or unstaged changes reviewed
- User is about to commit and wants a quick quality check

## Workflow

1. **Identify scope**
   - Prefer reviewing staged changes (`git diff --cached`). If the user asks for "all changes," include `git diff` as well.
   - List files and a short summary of what changed (features, fixes, refactors).

2. **Run the review checklist** (below) on the changed code.

3. **Report findings** using the feedback format at the end. Do not block the user from committing; classify issues so they can decide what to fix now vs. later.

## Review Checklist

Use this for each changed file; skip items that don't apply (e.g. no tests in this PR).

- [ ] **Correctness**: Logic is sound; edge cases and error paths considered; no obvious off-by-one or null/empty handling bugs.
- [ ] **Security**: No hardcoded secrets, credentials, or API keys; input validated/sanitized where needed; no unsafe injection (SQL, command, XSS) risks.
- [ ] **Project conventions**: Matches language-specific rules (e.g. Python type hints, C++ RAII, Rust `Result`/`Option`); uses shared utilities (logging, config, HAL) where they exist.
- [ ] **Maintainability**: Clear names, reasonable function/scope size, no unexplained magic numbers; comments explain *why* where non-obvious.
- [ ] **Resource safety**: Handles closed resources, timeouts, and cleanup (files, sockets, threads) where relevant.
- [ ] **Tests**: New behavior has or is covered by tests where the project expects them.

## Feedback Format

Keep the report scannable. Use this format:

```markdown
## Pre-commit review

**Scope:** [staged / staged + unstaged] — [list of files]

### Summary
[1–2 sentences on what the change does and overall assessment.]

### Critical (fix before commit)
- [Issue and file/location if applicable]

### Suggestions (consider fixing)
- [Issue and optional fix]

### Nice to have
- [Optional improvement]
```

If nothing critical or suggested, say so explicitly (e.g. "No critical or suggestion-level issues; good to commit from a review perspective.").

## Notes

- Respect project rules in `.cursor/rules/` and any language-specific guidelines (e.g. Python, C++, Rust) in the repo.
- Do not run destructive or irreversible commands (e.g. force push, mass delete). Review is read-only plus suggested edits.
