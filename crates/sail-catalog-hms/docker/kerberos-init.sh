#!/bin/bash
#
# Kerberos KDC initialization script
# This script creates principals and keytabs for HMS and client testing
#
# Run by the KDC container on startup

set -e

echo "Starting Kerberos KDC initialization..."

# Wait for KDC to be fully started
sleep 5

# Realm configuration
REALM=${KRB5_REALM:-EXAMPLE.COM}
KDC_HOST=${KRB5_KDC:-kdc.example.com}
ADMIN_PASSWORD=${KRB5_PASS:-krbpass}

echo "Realm: $REALM"
echo "KDC: $KDC_HOST"

# Create principals
echo "Creating Kerberos principals..."

# HMS service principal
kadmin.local -q "addprinc -randkey hive/hms.example.com@${REALM}"
echo "  ✓ Created hive/hms.example.com@${REALM}"

# Client principal for PySpark
kadmin.local -q "addprinc -randkey client@${REALM}"
echo "  ✓ Created client@${REALM}"

# Test user principal with password
kadmin.local -q "addprinc -pw testpass testuser@${REALM}"
echo "  ✓ Created testuser@${REALM}"

# HTTP principal (for web UIs if needed)
kadmin.local -q "addprinc -randkey HTTP/hms.example.com@${REALM}"
echo "  ✓ Created HTTP/hms.example.com@${REALM}"

# Create keytab directory if it doesn't exist
mkdir -p /keytabs

# Export keytabs
echo "Exporting keytabs..."

# HMS service keytab
kadmin.local -q "xst -norandkey -k /keytabs/hive.keytab hive/hms.example.com@${REALM}"
echo "  ✓ Exported hive.keytab"

# Client keytab
kadmin.local -q "xst -norandkey -k /keytabs/client.keytab client@${REALM}"
echo "  ✓ Exported client.keytab"

# Test user keytab
kadmin.local -q "xst -norandkey -k /keytabs/testuser.keytab testuser@${REALM}"
echo "  ✓ Exported testuser.keytab"

# HTTP keytab
kadmin.local -q "xst -norandkey -k /keytabs/http.keytab HTTP/hms.example.com@${REALM}"
echo "  ✓ Exported http.keytab"

# Set permissions
chmod 644 /keytabs/*.keytab
echo "  ✓ Set keytab permissions"

# List created principals
echo ""
echo "Created principals:"
kadmin.local -q "listprincs" | grep -E "(hive|client|testuser|HTTP)"

# Verify keytabs
echo ""
echo "Verifying keytabs..."
for keytab in /keytabs/*.keytab; do
    echo "  $(basename $keytab):"
    klist -kte "$keytab" | grep -v "^Keytab" | grep -v "^KVNO" | head -3
done

echo ""
echo "Kerberos KDC initialization complete!"
echo "Keytabs are available in /keytabs/"
