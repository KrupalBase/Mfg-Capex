---
name: repo-cleanup-commit
description: Cleans and organizes the repository so it is ready for an effective commit—removing clutter, fixing structure, and improving .gitignore and consistency. Use when the user wants to clean up the repo before committing, organize the codebase, or prepare for a commit.
---

# Repo Cleanup for Commit

Help organize the repository so it is clean, consistent, and ready for a meaningful commit. Focus on removable clutter, structure, and commit hygiene—not on rewriting feature logic.

## When to Apply

- User asks to "clean up the repo," "organize before commit," or "make the repo ready for committing"
- User wants to remove junk files, fix layout, or tighten what gets committed
- User is preparing a PR or first commit and wants the repo to look professional

## Workflow

1. **Assess current state**
   - List root and key directories; note language(s) and build/config files.
   - Check `git status`: untracked, modified, and staged files.
   - Read `.gitignore` (create or extend if missing/insufficient).

2. **Run the cleanup checklist** below. Propose concrete changes; avoid destructive actions without explicit confirmation.

3. **Summarize** what was done and what the user should do next (e.g. review diff, stage, commit).

## Cleanup Checklist

- [ ] **Ignore rules**: Ensure `.gitignore` (and `.cursorignore` if present) excludes build outputs, venvs, IDE/project files, logs, local config with secrets, and OS cruft (e.g. `Thumbs.db`, `.DS_Store`). Add only what the project actually uses.
- [ ] **Junk files**: Identify and propose removing or ignoring: temp files, backup copies (e.g. `*.bak`, `*~`), large binaries or caches that shouldn’t be in version control, duplicate or obsolete scripts.
- [ ] **Structure**: Check that layout matches project conventions (e.g. `utilities/` for shared code, app-specific dirs, clear separation of config vs. code). Suggest moves or renames only where they clearly improve clarity; avoid unnecessary churn.
- [ ] **Consistency**: Same naming style (e.g. `snake_case` for Python, project-specific patterns); no stray debug files or one-off scripts in the root unless intended.
- [ ] **Sensitive data**: No committed secrets, keys, or credentials; recommend adding/updating `.gitignore` and, if needed, documenting how to use keyring/env/local config.
- [ ] **Documentation**: Root README or docs reflect current structure and how to run/build; remove or update obviously outdated instructions.

## Output Format

Provide a short, actionable report:

```markdown
## Repo cleanup summary

### Current state
- [Brief description of repo layout and git status]

### Changes made (or proposed)
1. [Change and reason]
2. [Change and reason]
...

### Recommended next steps
- [ ] [Action, e.g. review .gitignore diff]
- [ ] [Action, e.g. stage and commit]
```

For **proposed** (not yet applied) changes, list them clearly and ask for confirmation before running destructive or broad commands (e.g. deleting many files, moving whole directories).

## Safety

- Do not force-push, rewrite history, or delete branches without explicit request.
- Before deleting or moving files, show what will change (e.g. list or diff). Prefer suggesting edits to `.gitignore` over deleting ignored content unless the user asks.
- If the repo has no `.git` directory, treat as "not a git repo" and limit to file/structure suggestions and a sample `.gitignore` they can add when they init.
