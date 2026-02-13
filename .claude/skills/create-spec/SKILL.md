---
name: create-spec
description: Create a new OpenSpec specification. Generates a spec file in openspec/specs/ with proper format including Purpose, Requirements, and Given/When/Then scenarios.
user-invocable: true
disable-model-invocation: false
---

# Create OpenSpec Specification

Creates a new specification file for a feature or capability following the OpenSpec format.

## Usage

```
/create-spec [feature-name]
```

Example:
```
/create-spec payment-processing
/create-spec user-notifications
/create-spec api-rate-limiting
```

## What This Skill Does

1. Creates a new directory in `openspec/specs/[feature-name]/`
2. Generates `spec.md` with the OpenSpec template
3. Prompts you to fill in Purpose and Requirements
4. Structures requirements with Given/When/Then scenarios

## Spec Template Structure

The generated spec includes:

```markdown
# [Feature Name] Specification

## Purpose
[Describe what this capability handles]

## Requirements

### Requirement: [Name]
[Description]

#### Scenario: [Scenario Name]
- GIVEN [initial state]
- WHEN [action]
- THEN [outcome]
```

## OpenSpec Format Guidelines

### Purpose
- Clear, concise description (1-3 sentences)
- Explain what the feature does and why it matters

### Requirements
- One requirement per major feature aspect
- Use verb-noun naming: "User Login", "Session Validation"
- Descriptive text explaining the requirement

### Scenarios (Given/When/Then)
- **GIVEN**: The initial state or context
- **WHEN**: The action or event that occurs
- **THEN**: The expected outcome or behavior

## Naming Conventions

Use kebab-case for feature names:
- ✅ `user-authentication`
- ✅ `payment-processing`
- ✅ `email-notifications`
- ❌ `UserAuthentication`
- ❌ `payment_processing`

## Examples

### Example 1: Simple Feature
```markdown
# Email Notifications Specification

## Purpose
Sends email notifications to users for important events like password resets and account updates.

## Requirements

### Requirement: Send Password Reset Email
Notify user when they initiate password reset.

#### Scenario: Password Reset Initiated
- GIVEN a user has requested password reset
- WHEN the reset request is processed
- THEN an email with reset link is sent to user's email address
- AND reset token expires in 24 hours
```

### Example 2: Complex Feature
```markdown
# Payment Processing Specification

## Purpose
Manages payment processing, including charge creation, refunds, and transaction status tracking.

## Requirements

### Requirement: Process Payment
Create a charge for a customer.

#### Scenario: Successful Payment
- GIVEN a customer with valid payment method
- WHEN the customer initiates payment of $99.99
- THEN charge is created successfully
- AND confirmation is returned with transaction ID

#### Scenario: Insufficient Funds
- GIVEN a customer with insufficient funds
- WHEN payment is attempted
- THEN payment is declined
- AND error is returned to customer

### Requirement: Refund Payment
Return funds to customer.

#### Scenario: Full Refund
- GIVEN a completed transaction
- WHEN refund is requested
- THEN full amount is refunded
- AND refund status is tracked
```

## After Creating a Spec

1. **Review**: Read the generated spec and refine as needed
2. **Discuss**: Share with team for feedback
3. **Implement**: Use the spec as implementation guide
4. **Test**: Map scenarios to test cases
5. **Update**: Keep spec current as feature evolves

## Tips

- Keep scenarios focused on one behavior
- Include both success and failure paths
- Use specific values (amounts, timeouts) where relevant
- Consider edge cases: expired tokens, missing data, etc.
- Reference external systems if needed

## Related Skills

- `/openspec-workflow`: Complete workflow for spec-driven development
- `/implement-spec`: Implement features based on specifications
