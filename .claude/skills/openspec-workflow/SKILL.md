---
name: openspec-workflow
description: Spec-Driven Development (SDD) workflow using OpenSpec. Helps you create, understand, and implement features using OpenSpec specifications stored in openspec/specs/ directory. Use when building features, writing requirements, or reviewing specifications.
user-invocable: true
disable-model-invocation: false
---

# OpenSpec Workflow Skill

This skill guides you through spec-driven development using OpenSpec—a lightweight methodology where specifications live in your repository as source of truth for feature requirements and behavior.

## What is OpenSpec?

OpenSpec is a spec-driven development approach that:
- **Stores specs in code**: Specifications live in `openspec/specs/` alongside your codebase
- **Uses Markdown**: Each spec is a `.md` file organized by capability
- **Defines behavior**: Requirements and scenarios (Given/When/Then) define exact behavior
- **Living documentation**: Specs evolve with your codebase and stay current

## Directory Structure

```
openspec/specs/
├── feature-name/
│   └── spec.md
├── another-feature/
│   └── spec.md
└── complex-feature/
    ├── spec.md
    └── implementation-notes.md
```

## OpenSpec Format

Each spec file follows this structure:

```markdown
# [Capability/Feature Name] Specification

## Purpose
Brief description of what this capability handles and why it matters.

## Requirements

### Requirement: [Name]
Description of the requirement.

#### Scenario: [Scenario Name]
- GIVEN [initial state/context]
- WHEN [the action that occurs]
- THEN [the expected outcome/behavior]

#### Scenario: [Another Scenario]
- GIVEN ...
- WHEN ...
- THEN ...

### Requirement: [Another Requirement]
...
```

## OpenSpec Workflow Steps

### 1. Create a Specification
```
/openspec-workflow create [feature-name]
```
Creates a new spec template in `openspec/specs/[feature-name]/spec.md`

### 2. Write Requirements
Define what the feature should do:
- Clear purpose statement
- Requirements with descriptive names
- Scenarios using Given/When/Then format

### 3. Review Specifications
```
/openspec-workflow review
```
Review existing specs and ask Claude to help refine them

### 4. Implement from Specs
Use the specs as source of truth for implementation:
- Each scenario becomes a test case or acceptance criterion
- Requirements guide feature design
- Specs provide context for code reviews

### 5. Update Specs
Keep specs current as implementation evolves. Specs should reflect actual system behavior.

## Why OpenSpec?

### For AI-Assisted Development
- **Context**: Claude can read specs to understand what to build
- **Precision**: Given/When/Then scenarios eliminate ambiguity
- **Continuity**: Specs persist across sessions and developers
- **Testability**: Scenarios map directly to test cases

### For Teams
- **Single source of truth**: Feature behavior is documented
- **Onboarding**: New developers understand features through specs
- **Collaboration**: Discuss requirements before coding
- **Traceability**: Connect implementation to original requirements

## Best Practices

### Writing Requirements
✅ Use clear, specific language
✅ Each scenario tests one behavior
✅ Include edge cases and error conditions
✅ Keep Given/When/Then format consistent

### Organizing Specs
✅ One spec per capability/feature
✅ Group related specs in subdirectories
✅ Use descriptive names (user-auth, payment-processing)
✅ Include implementation notes when helpful

### Keeping Specs Current
✅ Update specs when behavior changes
✅ Mark deprecated specs clearly
✅ Reference specs in code comments
✅ Use specs as basis for test cases

## Example: User Authentication Spec

```markdown
# User Authentication Specification

## Purpose
Manages user login, session creation, and authentication state.

## Requirements

### Requirement: User Login
Authenticate a user with email and password.

#### Scenario: Successful Login
- GIVEN a user with email "user@example.com" and password "correct-password"
- WHEN the user submits login credentials
- THEN a session token is created and returned
- AND the user is marked as authenticated

#### Scenario: Invalid Password
- GIVEN a user with email "user@example.com" and password "wrong-password"
- WHEN the user submits login credentials
- THEN login fails with an "Invalid credentials" error
- AND no session is created

### Requirement: Session Validation
Verify that a session token is valid and not expired.

#### Scenario: Valid Session
- GIVEN a valid, non-expired session token
- WHEN the session is validated
- THEN validation succeeds
- AND user identity is returned

#### Scenario: Expired Session
- GIVEN an expired session token
- WHEN the session is validated
- THEN validation fails with a "Session expired" error
```

## Tips for Working with OpenSpec

1. **Start with specs**: Write the spec before implementation
2. **Use specs for testing**: Given/When/Then becomes your test framework
3. **Iterate specs**: Refine as you learn
4. **Share with teams**: Specs are discussion documents
5. **Automate from specs**: Generate tests or documentation from specs

## Resources

- OpenSpec Official: https://openspec.dev/
- Spec-Driven Development Guide: https://intent-driven.dev/knowledge/openspec/
- GitHub OpenSpec: https://github.com/Fission-AI/OpenSpec

## Next Steps

1. Review existing specs in `openspec/specs/` if they exist
2. Create specs for new features
3. Use specs as the foundation for implementation
4. Keep specs updated as features evolve
