# Kerberos Authentication Setup for HMS

This guide explains how to configure Kerberos authentication for the Hive Metastore catalog provider.

## Overview

Kerberos is the standard authentication mechanism for secure Hadoop environments. This implementation provides:

- **Keytab-based authentication**: Service accounts with keytab files
- **Ticket cache authentication**: User authentication via `kinit`
- **SASL/GSSAPI support**: Industry-standard security layer
- **Automatic renewal**: Optional ticket renewal (configurable)
- **Multiple QoP levels**: Authentication, integrity, and confidentiality

## Prerequisites

### System Requirements

1. **Kerberos client libraries** installed:
   ```bash
   # RHEL/CentOS
   sudo yum install krb5-workstation krb5-libs

   # Ubuntu/Debian
   sudo apt-get install krb5-user libkrb5-dev

   # macOS
   brew install krb5
   ```

2. **Kerberos configuration file** (`/etc/krb5.conf`):
   ```ini
   [libdefaults]
       default_realm = EXAMPLE.COM
       dns_lookup_realm = false
       dns_lookup_kdc = false
       ticket_lifetime = 24h
       renew_lifetime = 7d
       forwardable = true

   [realms]
       EXAMPLE.COM = {
           kdc = kdc.example.com
           admin_server = admin.example.com
       }

   [domain_realm]
       .example.com = EXAMPLE.COM
       example.com = EXAMPLE.COM
   ```

### HMS Configuration

Ensure your Hive Metastore is configured for Kerberos authentication:

1. **hive-site.xml** on HMS server:
   ```xml
   <property>
       <name>hive.metastore.sasl.enabled</name>
       <value>true</value>
   </property>

   <property>
       <name>hive.metastore.kerberos.keytab.file</name>
       <value>/etc/security/keytabs/hive.service.keytab</value>
   </property>

   <property>
       <name>hive.metastore.kerberos.principal</name>
       <value>hive/_HOST@EXAMPLE.COM</value>
   </property>
   ```

## Configuration Methods

### Method 1: Keytab-Based Authentication (Recommended for Services)

Use this method for long-running services and automated processes.

#### 1.1 Create a Keytab

```bash
# On the KDC or authorized admin server
kadmin.local

# Create principal
addprinc -randkey myapp/hostname@EXAMPLE.COM

# Create keytab
ktadd -k /path/to/myapp.keytab myapp/hostname@EXAMPLE.COM

# Exit kadmin
quit

# Verify keytab
klist -kt /path/to/myapp.keytab

# Set proper permissions
chmod 400 /path/to/myapp.keytab
chown myapp:myapp /path/to/myapp.keytab
```

#### 1.2 LakeSail Configuration

**YAML Configuration:**

```yaml
catalog:
  list:
    - name: "hive_kerb"
      type: "hive_metastore"
      uri: "thrift://hms-server.example.com:9083"

      auth:
        type: "kerberos"
        principal: "myapp/hostname@EXAMPLE.COM"
        keytab: "/etc/security/keytabs/myapp.keytab"
        service: "hive"  # HMS service name (default: "hive")
        mutual_auth: true
        sasl_qop: "auth"  # Options: auth, auth-int, auth-conf

      connection_pool:
        max_size: 20

      cache:
        enabled: true
        ttl_seconds: 300
```

**Programmatic Configuration:**

```rust
use sail_catalog_hms::{HmsConfig, HmsAuthConfig, SaslQop, HmsProvider};

let config = HmsConfig {
    name: "hive_kerb".to_string(),
    uri: "thrift://hms-server.example.com:9083".to_string(),
    auth: HmsAuthConfig::Kerberos {
        principal: "myapp/hostname@EXAMPLE.COM".to_string(),
        keytab: Some("/etc/security/keytabs/myapp.keytab".to_string()),
        realm: Some("EXAMPLE.COM".to_string()),
        service: "hive".to_string(),
        service_hostname: None,  // Auto-detected from URI
        mutual_auth: true,
        krb5_conf: None,  // Uses system default /etc/krb5.conf
        use_ccache: false,  // Don't use ticket cache
        ticket_lifetime: None,  // Use default from krb5.conf
        auto_renew: false,  // Don't auto-renew (keytab doesn't need renewal)
        sasl_qop: SaslQop::Auth,
    },
    ..Default::default()
};

let provider = HmsProvider::new(config).await?;
```

### Method 2: Ticket Cache Authentication (For User Sessions)

Use this method for interactive user sessions.

#### 2.1 Obtain Kerberos Ticket

```bash
# Get initial ticket
kinit user@EXAMPLE.COM

# Verify ticket
klist

# Expected output:
# Ticket cache: FILE:/tmp/krb5cc_1000
# Default principal: user@EXAMPLE.COM
#
# Valid starting     Expires            Service principal
# 10/22/25 10:00:00  10/23/25 10:00:00  krbtgt/EXAMPLE.COM@EXAMPLE.COM
```

#### 2.2 LakeSail Configuration

**YAML Configuration:**

```yaml
catalog:
  list:
    - name: "hive_user"
      type: "hive_metastore"
      uri: "thrift://hms-server.example.com:9083"

      auth:
        type: "kerberos"
        principal: "user@EXAMPLE.COM"
        use_ccache: true  # Use existing ticket from kinit
        service: "hive"
        sasl_qop: "auth"
```

**Programmatic Configuration:**

```rust
use sail_catalog_hms::{HmsConfig, HmsAuthConfig, SaslQop};

let config = HmsConfig {
    name: "hive_user".to_string(),
    uri: "thrift://hms-server.example.com:9083".to_string(),
    auth: HmsAuthConfig::Kerberos {
        principal: "user@EXAMPLE.COM".to_string(),
        keytab: None,  // No keytab, using ticket cache
        realm: Some("EXAMPLE.COM".to_string()),
        service: "hive".to_string(),
        service_hostname: None,
        mutual_auth: true,
        krb5_conf: None,
        use_ccache: true,  // Use ticket cache
        ticket_lifetime: None,
        auto_renew: false,
        sasl_qop: SaslQop::Auth,
    },
    ..Default::default()
};
```

### Method 3: Custom Kerberos Configuration

For environments with non-standard Kerberos setups.

```yaml
catalog:
  list:
    - name: "hive_custom"
      type: "hive_metastore"
      uri: "thrift://hms-server.example.com:9083"

      auth:
        type: "kerberos"
        principal: "myapp/hostname@EXAMPLE.COM"
        keytab: "/etc/security/keytabs/myapp.keytab"
        realm: "EXAMPLE.COM"
        service: "hive"
        service_hostname: "hms-server.example.com"  # Override hostname
        mutual_auth: true
        krb5_conf: "/opt/app/config/custom-krb5.conf"  # Custom config
        sasl_qop: "auth-int"  # Integrity protection
        auto_renew: true  # Enable auto-renewal
        ticket_lifetime: 86400  # 24 hours in seconds
```

## SASL Quality of Protection (QoP)

Choose the appropriate QoP level based on your security requirements:

### 1. `auth` (Authentication Only) - **Recommended**

- **Performance**: Fastest, minimal overhead
- **Security**: Authenticates identity only
- **Network**: No encryption, data sent in clear
- **Use case**: Trusted networks, performance-critical applications

```yaml
sasl_qop: "auth"
```

### 2. `auth-int` (Authentication + Integrity)

- **Performance**: Moderate overhead
- **Security**: Authenticates identity + prevents tampering
- **Network**: Messages include integrity checksums
- **Use case**: Semi-trusted networks, data integrity requirements

```yaml
sasl_qop: "auth-int"
```

### 3. `auth-conf` (Authentication + Confidentiality)

- **Performance**: Highest overhead
- **Security**: Authenticates + encrypts all communication
- **Network**: Full encryption of data
- **Use case**: Untrusted networks, strict compliance requirements

```yaml
sasl_qop: "auth-conf"
```

## Troubleshooting

### Common Issues

#### 1. "Cannot obtain Kerberos ticket"

```
Error: Kerberos authentication failed: Cannot obtain ticket
```

**Solutions:**

```bash
# Check KDC connectivity
telnet kdc.example.com 88

# Verify krb5.conf is correct
cat /etc/krb5.conf

# Test with kinit
kinit -kt /path/to/keytab principal@REALM

# Check for time sync issues (Kerberos is time-sensitive)
ntpdate -q pool.ntp.org

# Verify keytab is valid
klist -kt /path/to/keytab

# Check keytab permissions
ls -la /path/to/keytab  # Should be 400 or 600
```

#### 2. "Service principal not found"

```
Error: Service principal hive/hostname@REALM not found
```

**Solutions:**

- Verify HMS server principal: `klist -kt /path/to/hive.keytab`
- Check HMS configuration: Look for `hive.metastore.kerberos.principal`
- Ensure `service_hostname` matches HMS server hostname
- Try explicit `service_hostname` in config:

```yaml
auth:
  type: "kerberos"
  principal: "user@REALM"
  service: "hive"
  service_hostname: "actual-hms-hostname.example.com"
```

#### 3. "Clock skew too great"

```
Error: Clock skew too great
```

**Solution:**

```bash
# Sync time with NTP
sudo ntpdate pool.ntp.org

# Or enable NTP daemon
sudo systemctl enable ntpd
sudo systemctl start ntpd

# Verify time sync
date
```

#### 4. "Ticket expired"

```
Error: Ticket has expired
```

**Solutions:**

For **keytab authentication** (should not happen):
```bash
# Verify keytab is valid
klist -kt /path/to/keytab

# Try manual authentication
kinit -kt /path/to/keytab principal@REALM
```

For **ticket cache authentication**:
```bash
# Renew ticket
kinit -R

# Or get new ticket
kinit user@REALM

# Enable auto-renewal in config:
```

```yaml
auth:
  type: "kerberos"
  auto_renew: true
  ticket_lifetime: 86400
```

#### 5. "GSS failure: Unspecified GSS failure"

```
Error: GSS failure: Unspecified GSS failure. Minor code may provide more information
```

**Solutions:**

```bash
# Enable Kerberos debugging
export KRB5_TRACE=/dev/stderr
export RUST_LOG=sail_catalog_hms=debug

# Re-run application to see detailed Kerberos logs

# Check DNS resolution
nslookup hms-server.example.com

# Verify reverse DNS
nslookup <IP_ADDRESS>

# Ensure forward and reverse DNS match
```

### Debug Mode

Enable detailed Kerberos logging:

```bash
# Environment variables
export KRB5_TRACE=/tmp/krb5_trace.log
export RUST_LOG=sail_catalog_hms::auth=debug

# Run your application
./myapp

# Check trace log
tail -f /tmp/krb5_trace.log
```

## Security Best Practices

### 1. Keytab Security

```bash
# Proper permissions (read-only for service account)
chmod 400 /etc/security/keytabs/myapp.keytab
chown myapp:myapp /etc/security/keytabs/myapp.keytab

# Store in protected directory
# Never commit keytabs to version control
# Rotate keytabs periodically (e.g., every 90 days)
```

### 2. Principal Naming

- **Service principals**: Use format `service/hostname@REALM`
  - Example: `hive/hms-prod.example.com@PROD.REALM`
- **User principals**: Use format `username@REALM`
  - Example: `john.doe@EXAMPLE.COM`

### 3. Network Security

- Use `auth-conf` QoP on untrusted networks
- Use `auth` QoP on secured internal networks
- Enable mutual authentication: `mutual_auth: true`

### 4. Credential Management

- **For services**: Always use keytabs, never ticket cache
- **For users**: Use ticket cache (`kinit`) for interactive sessions
- **Auto-renewal**: Only enable if necessary, prefer keytab for long-running services

## Advanced Configuration

### High Availability Setup

```yaml
catalog:
  list:
    # Primary HMS
    - name: "hive_primary"
      type: "hive_metastore"
      uri: "thrift://hms-primary.example.com:9083"
      auth:
        type: "kerberos"
        principal: "myapp/hostname@EXAMPLE.COM"
        keytab: "/etc/security/keytabs/myapp.keytab"
        service: "hive"

    # Backup HMS
    - name: "hive_backup"
      type: "hive_metastore"
      uri: "thrift://hms-backup.example.com:9083"
      auth:
        type: "kerberos"
        principal: "myapp/hostname@EXAMPLE.COM"
        keytab: "/etc/security/keytabs/myapp.keytab"
        service: "hive"
```

### Cross-Realm Authentication

```yaml
auth:
  type: "kerberos"
  principal: "user@REALMB.COM"
  keytab: "/etc/security/keytabs/user.keytab"
  realm: "REALMB.COM"
  service: "hive"
  service_hostname: "hms.realma.com"
  # Service is in REALMA.COM, client is in REALMB.COM
  # Requires cross-realm trust configuration
```

### Docker/Kubernetes Deployment

**Docker:**

```dockerfile
FROM ubuntu:22.04

# Install Kerberos client
RUN apt-get update && apt-get install -y krb5-user

# Copy keytab
COPY keytabs/myapp.keytab /etc/security/keytabs/
RUN chmod 400 /etc/security/keytabs/myapp.keytab

# Copy krb5.conf
COPY krb5.conf /etc/krb5.conf

# Your application
COPY myapp /usr/local/bin/
CMD ["/usr/local/bin/myapp"]
```

**Kubernetes:**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: hms-keytab
type: Opaque
data:
  myapp.keytab: <base64-encoded-keytab>
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: krb5-config
data:
  krb5.conf: |
    [libdefaults]
        default_realm = EXAMPLE.COM
    # ... rest of krb5.conf
---
apiVersion: v1
kind: Pod
metadata:
  name: myapp
spec:
  containers:
  - name: myapp
    image: myapp:latest
    volumeMounts:
    - name: keytab
      mountPath: /etc/security/keytabs
      readOnly: true
    - name: krb5-config
      mountPath: /etc/krb5.conf
      subPath: krb5.conf
      readOnly: true
  volumes:
  - name: keytab
    secret:
      secretName: hms-keytab
  - name: krb5-config
    configMap:
      name: krb5-config
```

## Testing Kerberos Setup

### Manual Testing

```bash
# 1. Verify keytab
klist -kt /path/to/keytab

# 2. Get ticket using keytab
kinit -kt /path/to/keytab principal@REALM

# 3. Verify ticket
klist

# 4. Test HMS connection using beeline
beeline -u "jdbc:hive2://hms-server:10000/default;principal=hive/hms-server@REALM"

# 5. Clean up
kdestroy
```

### Programmatic Testing

```rust
use sail_catalog_hms::{HmsConfig, HmsAuthConfig, HmsProvider, SaslQop};
use sail_catalog::provider::CatalogProvider;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure Kerberos auth
    let config = HmsConfig {
        name: "test_hive".to_string(),
        uri: "thrift://hms-server.example.com:9083".to_string(),
        auth: HmsAuthConfig::Kerberos {
            principal: "myapp/hostname@EXAMPLE.COM".to_string(),
            keytab: Some("/etc/security/keytabs/myapp.keytab".to_string()),
            realm: Some("EXAMPLE.COM".to_string()),
            service: "hive".to_string(),
            service_hostname: None,
            mutual_auth: true,
            krb5_conf: None,
            use_ccache: false,
            ticket_lifetime: None,
            auto_renew: false,
            sasl_qop: SaslQop::Auth,
        },
        ..Default::default()
    };

    // Create provider
    let provider = HmsProvider::new(config).await?;

    // Test by listing databases
    let databases = provider.list_databases(None).await?;
    println!("Successfully connected! Found {} databases", databases.len());

    for db in databases {
        println!("  - {}", db.database.join("."));
    }

    Ok(())
}
```

## References

- [MIT Kerberos Documentation](https://web.mit.edu/kerberos/krb5-latest/doc/)
- [Apache Hive Security Configuration](https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2#SettingUpHiveServer2-Kerberos)
- [SASL Authentication](https://www.ietf.org/rfc/rfc2222.txt)
- [GSSAPI Specification](https://www.ietf.org/rfc/rfc2743.txt)
