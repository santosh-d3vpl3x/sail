#!/bin/bash
#
# Run all PySpark HMS Kerberos tests
#
# This script:
# 1. Authenticates with Kerberos
# 2. Waits for HMS to be ready
# 3. Runs all pytest tests
# 4. Generates test reports

set -e

echo "=========================================="
echo "HMS Kerberos Test Runner"
echo "=========================================="
echo ""

# Configuration
KEYTAB=${KRB5_CLIENT_KTNAME:-/etc/security/keytabs/client.keytab}
PRINCIPAL="client@EXAMPLE.COM"
HMS_URI=${HMS_URI:-thrift://hms.example.com:9083}
TEST_RESULTS_DIR=${TEST_RESULTS_DIR:-/test-results}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if a service is ready
wait_for_service() {
    local host=$1
    local port=$2
    local max_attempts=30
    local attempt=0

    print_info "Waiting for $host:$port to be ready..."

    while [ $attempt -lt $max_attempts ]; do
        if nc -z "$host" "$port" 2>/dev/null; then
            print_info "$host:$port is ready!"
            return 0
        fi

        attempt=$((attempt + 1))
        echo -n "."
        sleep 2
    done

    print_error "$host:$port is not ready after $max_attempts attempts"
    return 1
}

# Step 1: Wait for HMS to be ready
print_info "Step 1: Checking HMS availability..."
HMS_HOST=$(echo $HMS_URI | sed 's|thrift://||' | cut -d: -f1)
HMS_PORT=$(echo $HMS_URI | sed 's|thrift://||' | cut -d: -f2)
HMS_PORT=${HMS_PORT:-9083}

if ! wait_for_service "$HMS_HOST" "$HMS_PORT"; then
    print_error "HMS is not available. Exiting."
    exit 1
fi

echo ""

# Step 2: Authenticate with Kerberos
print_info "Step 2: Authenticating with Kerberos..."

if [ ! -f "$KEYTAB" ]; then
    print_error "Keytab file not found: $KEYTAB"
    exit 1
fi

print_info "Using keytab: $KEYTAB"
print_info "Principal: $PRINCIPAL"

# Kinit with keytab
if kinit -kt "$KEYTAB" "$PRINCIPAL"; then
    print_info "Kerberos authentication successful"
else
    print_error "Kerberos authentication failed"
    exit 1
fi

# Verify ticket
print_info "Kerberos ticket information:"
klist

echo ""

# Step 3: Run tests
print_info "Step 3: Running PySpark tests..."
echo ""

# Create test results directory
mkdir -p "$TEST_RESULTS_DIR"

# Run pytest with coverage and detailed output
pytest /tests \
    -v \
    --tb=short \
    --timeout=300 \
    --junit-xml="$TEST_RESULTS_DIR/junit.xml" \
    --html="$TEST_RESULTS_DIR/report.html" \
    --self-contained-html \
    -o log_cli=true \
    -o log_cli_level=INFO

# Capture exit code
TEST_EXIT_CODE=$?

echo ""

# Step 4: Generate summary
print_info "Step 4: Test Summary"
echo ""

if [ $TEST_EXIT_CODE -eq 0 ]; then
    print_info "✓ All tests passed!"
else
    print_error "✗ Some tests failed (exit code: $TEST_EXIT_CODE)"
fi

# Show test results location
print_info "Test results saved to: $TEST_RESULTS_DIR"
ls -lh "$TEST_RESULTS_DIR"

echo ""

# Cleanup: Destroy Kerberos ticket
print_info "Cleaning up Kerberos ticket..."
kdestroy || true

echo ""
echo "=========================================="
echo "Test run complete"
echo "=========================================="

exit $TEST_EXIT_CODE
