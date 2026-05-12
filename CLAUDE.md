# Claude Code Rules for lematerial-fetcher

1. Before writing any code, describe your approach and wait for approval. Always ask clarifying questions before writing any code if requirements are ambiguous.

2. If a task requires changes to more than 3 files, stop and break it into smaller tasks first.

3. After writing code, list what could break and suggest tests to cover it.

4. When there's a bug, start by writing a test that reproduces it, then fix it until the test passes.

5. Every time I correct you, add a new rule to the CLAUDE.md file so it never happens again.

6. Before every git commit, scan the staged diff for API keys, tokens, passwords, and secrets (patterns: AKIA, hf_, sk-, Bearer, AWS_SECRET, password=). Never commit credentials.
