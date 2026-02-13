---
name: implement-spec
description: Implement a feature based on its OpenSpec specification. Reads the spec and guides implementation, test creation, and verification against the specification's scenarios.
user-invocable: true
disable-model-invocation: false
---

# Implement from OpenSpec Specification

Implements a feature using its OpenSpec specification as the source of truth for requirements and behavior.

## Usage

```
/implement-spec [feature-name]
```

Examples:
```
/implement-spec user-authentication
/implement-spec payment-processing
/implement-spec email-notifications
```

## What This Skill Does

1. Finds and reads the spec from `openspec/specs/[feature-name]/spec.md`
2. Reviews all requirements and scenarios
3. Guides implementation to match spec exactly
4. Creates tests for each scenario
5. Verifies implementation against spec

## Implementation Workflow

### Step 1: Review the Specification
- Read the Purpose to understand the feature's role
- Review all Requirements
- Study each Scenario's Given/When/Then format

### Step 2: Design Implementation
- Identify what code changes are needed
- Plan file structure and components
- Consider how scenarios map to functions/methods

### Step 3: Implement Features
- Code each requirement
- Ensure each scenario is handled
- Handle both success and failure cases

### Step 4: Create Tests
- One test per scenario
- Use Given/When/Then to structure tests
- Test both happy path and error cases

### Step 5: Verify Against Spec
- Run all tests
- Confirm implementation matches spec
- Update spec if behavior changes

## Mapping Scenarios to Tests

Each scenario becomes a test case:

**Spec Scenario:**
```
#### Scenario: Successful Login
- GIVEN a user with email "user@example.com" and password "correct-password"
- WHEN the user submits login credentials
- THEN a session token is created and returned
- AND the user is marked as authenticated
```

**Test Implementation:**
```rust
#[test]
fn test_successful_login() {
    // GIVEN a user with email and password
    let user = create_test_user("user@example.com", "correct-password");

    // WHEN the user submits login credentials
    let result = login(&user.email, "correct-password");

    // THEN a session token is created and returned
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(!response.session_token.is_empty());

    // AND the user is marked as authenticated
    assert!(is_authenticated(&response.session_token));
}
```

## Implementation Checklist

- [ ] Read and understand the spec
- [ ] Design implementation approach
- [ ] Create code structure
- [ ] Implement each requirement
- [ ] Create tests for all scenarios
- [ ] Handle success paths
- [ ] Handle error/failure cases
- [ ] Run full test suite
- [ ] Verify against original spec
- [ ] Update spec if needed

## Best Practices

### Stay Faithful to Spec
- Implement exactly what the spec describes
- Don't add features not in the spec
- If spec is unclear, clarify before coding

### Test-Driven from Specs
- Write test for each scenario first
- Let tests guide implementation
- Scenarios = test cases

### Handle All Cases
- Success scenarios
- Failure scenarios
- Edge cases mentioned in spec
- Error conditions

### Keep Code Clear
- Comment references to spec scenarios
- Use scenario names in test names
- Structure code to match requirement organization

### Update Spec When Needed
- If implementation reveals spec gaps, update spec
- If behavior changes, update spec
- Specs should always match reality

## Example: Implementing User Authentication

**From Spec Scenario:**
```
#### Scenario: Invalid Password
- GIVEN a user with email "user@example.com" and password "wrong-password"
- WHEN the user submits login credentials
- THEN login fails with an "Invalid credentials" error
- AND no session is created
```

**Implementation Approach:**
1. Create login function that validates credentials
2. Compare provided password with stored hash
3. Return error if password doesn't match
4. Don't create session on failure
5. Test the exact error message

**Test:**
```python
def test_invalid_password():
    # GIVEN a user with email and correct password
    user = create_user("user@example.com", "correct-password")

    # WHEN the user submits wrong password
    result = login("user@example.com", "wrong-password")

    # THEN login fails with "Invalid credentials" error
    assert result.is_error
    assert result.error_message == "Invalid credentials"

    # AND no session is created
    assert not session_exists(user.id)
```

## Common Patterns

### Optional vs Required
If spec says "THEN field X is returned", implement it as required. If "THEN field X is returned IF...", it's conditional.

### Timeouts and Expiration
Check spec for any time-based scenarios:
- "Session expires in 24 hours"
- "Token valid for 1 hour"
- "Retry after 5 seconds"

### Dependencies
Some scenarios may depend on other features. Implement specs with dependencies in order.

## Verification

After implementation:

1. **Run Tests**: All tests pass
2. **Review Spec**: Each requirement is implemented
3. **Check Scenarios**: Each Given/When/Then is covered
4. **Error Cases**: Failures handled as specified
5. **Integration**: Works with other features

## Tips for Success

- ✅ Read the entire spec before starting
- ✅ Ask clarifying questions if spec is ambiguous
- ✅ Implement one requirement at a time
- ✅ Test as you go
- ✅ Keep spec and code in sync
- ❌ Don't add features not in spec
- ❌ Don't skip error scenarios
- ❌ Don't ignore spec details

## Related Skills

- `/openspec-workflow`: Full spec-driven workflow
- `/create-spec`: Create new specifications
- `/review-spec`: Review and refine specifications

## References

- OpenSpec: https://openspec.dev/
- Spec-Driven Development: https://intent-driven.dev/knowledge/openspec/
