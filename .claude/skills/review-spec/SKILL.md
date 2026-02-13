---
name: review-spec
description: Review and improve OpenSpec specifications. Analyzes specs for completeness, clarity, and proper Given/When/Then format. Suggests improvements and identifies missing scenarios.
user-invocable: true
disable-model-invocation: false
---

# Review OpenSpec Specification

Reviews specifications for quality, completeness, and adherence to OpenSpec format. Identifies improvements and suggests better scenarios.

## Usage

```
/review-spec [feature-name]
```

Examples:
```
/review-spec user-authentication
/review-spec payment-processing
/review-spec email-notifications
```

## What This Skill Does

1. Reads the specification from `openspec/specs/[feature-name]/spec.md`
2. Analyzes for completeness and clarity
3. Checks Given/When/Then format consistency
4. Identifies missing scenarios
5. Suggests improvements
6. Validates against best practices

## Review Checklist

### Purpose Section
- [ ] Clear and concise (1-3 sentences)
- [ ] Explains what the feature does
- [ ] Explains why it matters
- [ ] Uses simple language
- [ ] Avoids implementation details

### Requirements
- [ ] Each requirement has a clear name
- [ ] Descriptive text explains the requirement
- [ ] Related scenarios are grouped together
- [ ] No duplicate requirements
- [ ] All important behaviors covered

### Scenarios
- [ ] Each scenario has a descriptive name
- [ ] Uses GIVEN/WHEN/THEN format consistently
- [ ] GIVEN states the initial context
- [ ] WHEN describes a single action
- [ ] THEN lists expected outcomes
- [ ] Includes AND for additional outcomes
- [ ] Uses specific values (not vague)
- [ ] Tests both success and failure paths

## Quality Criteria

### Clarity
✅ Anyone can understand what the feature should do
✅ Scenarios are unambiguous
✅ Technical jargon is explained
❌ Vague language like "works correctly"
❌ Implementation details in scenarios

### Completeness
✅ Happy path covered
✅ Error/failure cases included
✅ Edge cases considered
✅ Boundary conditions tested
❌ Missing scenarios
❌ Incomplete requirement coverage

### Testability
✅ Each scenario maps to one test
✅ Specific assertions possible
✅ No "and also" sprawl
✅ Given/When/Then are distinct
❌ Multiple behaviors per scenario
❌ Vague outcomes
❌ Unclear initial state

## Common Issues to Fix

### Issue 1: Vague GIVEN
❌ GIVEN a user
✅ GIVEN a user with email "user@example.com" and role "admin"

### Issue 2: Multiple Actions in WHEN
❌ WHEN user logs in and checks account
✅ WHEN user submits login credentials

### Issue 3: Missing Edge Cases
❌ Only success path
✅ Include: success, invalid input, timeout, permission denied

### Issue 4: Implementation in Spec
❌ THEN the bcrypt hash is computed
✅ THEN password is verified securely

### Issue 5: Incomplete THEN
❌ THEN it works
✅ THEN status code 200 is returned AND user object is returned with id, email, created_at

## Example: Before and After

### Before (Weak Spec)
```markdown
# User Login Specification

## Purpose
User login functionality.

## Requirements

### Requirement: Login
User can login.

#### Scenario: Login Works
- GIVEN a user
- WHEN they login
- THEN it works
```

### After (Strong Spec)
```markdown
# User Authentication Specification

## Purpose
Manages user login and session creation. Provides secure authentication with password validation and session token generation.

## Requirements

### Requirement: User Login
Authenticate a user with email and password credentials.

#### Scenario: Successful Login with Valid Credentials
- GIVEN a registered user with email "alice@example.com" and password "SecurePass123"
- WHEN the user submits login with correct email and password
- THEN login succeeds with HTTP 200 status
- AND a session token is returned
- AND the session token is valid for 24 hours
- AND the user's last_login timestamp is updated

#### Scenario: Login Fails with Invalid Password
- GIVEN a registered user with email "alice@example.com"
- WHEN the user submits login with correct email but wrong password
- THEN login fails with HTTP 401 status
- AND error message "Invalid credentials" is returned
- AND no session token is created
- AND failed login attempt is logged

#### Scenario: Login Fails with Non-existent User
- GIVEN no user exists with email "unknown@example.com"
- WHEN login is attempted with this email
- THEN login fails with HTTP 401 status
- AND error message "Invalid credentials" is returned (doesn't leak user existence)
- AND no session is created

### Requirement: Session Validation
Verify active sessions remain valid and handle expiration.

#### Scenario: Valid Session Token is Accepted
- GIVEN a valid, non-expired session token
- WHEN the session is validated
- THEN validation succeeds with HTTP 200
- AND user identity and permissions are returned

#### Scenario: Expired Session Token is Rejected
- GIVEN an expired session token (created more than 24 hours ago)
- WHEN the session is validated
- THEN validation fails with HTTP 401
- AND error "Session expired" is returned
- AND client should redirect to login
```

## Review Process

### 1. Read the Purpose
- Does it clearly explain what the feature does?
- Is it understandable to all team members?
- Does it explain why this feature exists?

### 2. Count Scenarios
- Are all major paths covered?
- Success cases? ✅
- Failure cases? ✅
- Edge cases? ✅

### 3. Check Format Consistency
- All scenarios follow GIVEN/WHEN/THEN? ✅
- Each WHEN is a single action? ✅
- All THEN outcomes are specific? ✅

### 4. Verify Testability
- Could you write a test from each scenario? ✅
- Are the assertions clear? ✅
- Can you execute and verify the behavior? ✅

### 5. Identify Gaps
- Are there missing scenarios?
- Are requirements incomplete?
- Are error cases handled?

## Common Missing Scenarios

Consider adding scenarios for:
- ❌ Invalid input (empty string, null, wrong type)
- ❌ Edge cases (boundary values, maximum limits)
- ❌ Error conditions (permission denied, not found, timeout)
- ❌ Concurrent operations (race conditions, locking)
- ❌ State transitions (order of operations matters)
- ❌ Integration with other features

## Scenario Naming

Good scenario names are:
- Descriptive: "Successful login with valid credentials"
- Outcome-focused: "Session created on successful authentication"
- Include context: "Login fails with invalid password"

❌ Bad: "Test 1", "Login", "Should work"
✅ Good: "Successful login with valid credentials"

## Tips for Effective Reviews

1. **Read Aloud**: Reading scenarios aloud reveals awkward phrasing
2. **Ask Questions**: If something is unclear, the spec needs clarification
3. **Imagine Tests**: Try to write test code mentally from scenarios
4. **Check Completeness**: "What could go wrong?" for each scenario
5. **Involve Team**: Get feedback from implementers and testers
6. **Iterate**: Good specs improve through multiple reviews

## Specification Anti-patterns

### Anti-pattern 1: Implementation Details
❌ "THEN the async/await function returns"
✅ "THEN the result is returned within 100ms"

### Anti-pattern 2: Vague Outcomes
❌ "THEN everything works fine"
✅ "THEN HTTP 200 is returned AND user object includes id, email, and created_at"

### Anti-pattern 3: Multiple Behaviors
❌ "THEN user is logged in AND email is sent AND preferences are loaded"
✅ [Split into separate scenarios or mark as AND only if tightly coupled]

### Anti-pattern 4: Conditional Without Clarity
❌ "THEN maybe an error is returned"
✅ "THEN if credentials are invalid, HTTP 401 is returned with error message"

## After Review

1. **Discuss Findings**: Share review feedback with team
2. **Update Spec**: Make suggested improvements
3. **Get Approval**: Ensure team agrees with updated spec
4. **Share with Implementers**: Provide final spec to development team
5. **Use as Blueprint**: Implement and test against final spec

## Related Skills

- `/openspec-workflow`: Full spec-driven workflow
- `/create-spec`: Create new specifications
- `/implement-spec`: Implement based on specifications

## Resources

- OpenSpec Best Practices: https://openspec.dev/
- BDD and Gherkin: https://cucumber.io/docs/gherkin/
- Specification Quality: https://intent-driven.dev/knowledge/openspec/
