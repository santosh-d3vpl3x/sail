//! Configuration for HMS catalog provider

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for HMS catalog provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HmsConfig {
    /// Catalog name
    pub name: String,

    /// HMS Thrift URI (e.g., "thrift://localhost:9083")
    pub uri: String,

    /// Authentication configuration
    #[serde(default)]
    pub auth: HmsAuthConfig,

    /// Connection pool configuration
    #[serde(default)]
    pub connection_pool: HmsConnectionPoolConfig,

    /// Cache configuration
    #[serde(default)]
    pub cache: HmsCacheConfig,

    /// Thrift protocol configuration
    #[serde(default)]
    pub thrift: HmsThriftConfig,
}

/// Authentication configuration for HMS
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HmsAuthConfig {
    /// No authentication (insecure, for testing only)
    None,

    /// Simple username/password authentication
    Simple {
        #[serde(default = "default_username")]
        username: String,
        password: Option<String>,
    },

    /// Kerberos authentication (SASL/GSSAPI)
    Kerberos {
        /// Kerberos principal (e.g., "hive/hostname@REALM" or "user@REALM")
        principal: String,

        /// Path to keytab file (optional, uses ticket cache if not specified)
        keytab: Option<String>,

        /// Kerberos realm (optional, extracted from principal if not specified)
        realm: Option<String>,

        /// Service name (default: "hive")
        #[serde(default = "default_kerberos_service")]
        service: String,

        /// Service hostname (optional, extracted from HMS URI if not specified)
        service_hostname: Option<String>,

        /// Enable mutual authentication
        #[serde(default = "default_mutual_auth")]
        mutual_auth: bool,

        /// Kerberos configuration file path (optional, uses /etc/krb5.conf by default)
        krb5_conf: Option<String>,

        /// Enable credential cache (kinit ticket cache)
        #[serde(default = "default_use_ccache")]
        use_ccache: bool,

        /// Ticket lifetime in seconds (optional)
        ticket_lifetime: Option<u64>,

        /// Enable automatic credential renewal
        #[serde(default = "default_auto_renew")]
        auto_renew: bool,

        /// SASL quality of protection (auth, auth-int, auth-conf)
        #[serde(default = "default_sasl_qop")]
        sasl_qop: SaslQop,
    },

    /// Delegation token authentication (for Hadoop ecosystem)
    DelegationToken {
        /// Delegation token string
        token: String,

        /// Token identifier (optional)
        identifier: Option<String>,

        /// Token service (optional)
        service: Option<String>,
    },
}

/// SASL Quality of Protection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SaslQop {
    /// Authentication only
    Auth,
    /// Authentication with integrity protection
    AuthInt,
    /// Authentication with confidentiality
    AuthConf,
}

impl Default for HmsConfig {
    fn default() -> Self {
        Self {
            name: "hive".to_string(),
            uri: "thrift://localhost:9083".to_string(),
            auth: Default::default(),
            connection_pool: Default::default(),
            cache: Default::default(),
            thrift: Default::default(),
        }
    }
}

impl Default for HmsAuthConfig {
    fn default() -> Self {
        HmsAuthConfig::Simple {
            username: default_username(),
            password: None,
        }
    }
}

impl HmsAuthConfig {
    /// Get username for logging/display purposes
    pub fn username(&self) -> Option<&str> {
        match self {
            HmsAuthConfig::None => None,
            HmsAuthConfig::Simple { username, .. } => Some(username),
            HmsAuthConfig::Kerberos { principal, .. } => Some(principal),
            HmsAuthConfig::DelegationToken { identifier, .. } => identifier.as_deref(),
        }
    }

    /// Check if authentication is enabled
    pub fn is_secure(&self) -> bool {
        !matches!(self, HmsAuthConfig::None)
    }

    /// Check if Kerberos authentication is configured
    pub fn is_kerberos(&self) -> bool {
        matches!(self, HmsAuthConfig::Kerberos { .. })
    }
}

/// Connection pool configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HmsConnectionPoolConfig {
    /// Maximum number of connections in the pool
    #[serde(default = "default_max_size")]
    pub max_size: u32,

    /// Minimum idle connections
    #[serde(default = "default_min_idle")]
    pub min_idle: Option<u32>,

    /// Connection timeout in milliseconds
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout_ms: u64,

    /// Idle timeout in milliseconds
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_ms: u64,

    /// Max lifetime of a connection in milliseconds
    #[serde(default = "default_max_lifetime")]
    pub max_lifetime_ms: Option<u64>,
}

impl Default for HmsConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_size: default_max_size(),
            min_idle: default_min_idle(),
            connection_timeout_ms: default_connection_timeout(),
            idle_timeout_ms: default_idle_timeout(),
            max_lifetime_ms: default_max_lifetime(),
        }
    }
}

impl HmsConnectionPoolConfig {
    /// Get connection timeout as Duration
    pub fn connection_timeout(&self) -> Duration {
        Duration::from_millis(self.connection_timeout_ms)
    }

    /// Get idle timeout as Duration
    pub fn idle_timeout(&self) -> Option<Duration> {
        Some(Duration::from_millis(self.idle_timeout_ms))
    }

    /// Get max lifetime as Duration
    pub fn max_lifetime(&self) -> Option<Duration> {
        self.max_lifetime_ms.map(Duration::from_millis)
    }
}

/// Metadata cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HmsCacheConfig {
    /// Enable metadata caching
    #[serde(default = "default_cache_enabled")]
    pub enabled: bool,

    /// Maximum number of entries in cache
    #[serde(default = "default_max_capacity")]
    pub max_capacity: u64,

    /// Time-to-live for cache entries in seconds
    #[serde(default = "default_ttl")]
    pub ttl_seconds: u64,

    /// Time-to-idle for cache entries in seconds
    #[serde(default = "default_tti")]
    pub tti_seconds: u64,
}

impl Default for HmsCacheConfig {
    fn default() -> Self {
        Self {
            enabled: default_cache_enabled(),
            max_capacity: default_max_capacity(),
            ttl_seconds: default_ttl(),
            tti_seconds: default_tti(),
        }
    }
}

impl HmsCacheConfig {
    /// Get TTL as Duration
    pub fn ttl(&self) -> Duration {
        Duration::from_secs(self.ttl_seconds)
    }

    /// Get TTI as Duration
    pub fn tti(&self) -> Duration {
        Duration::from_secs(self.tti_seconds)
    }
}

/// Thrift protocol configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HmsThriftConfig {
    /// Thrift protocol type
    #[serde(default = "default_protocol")]
    pub protocol: ThriftProtocol,

    /// Thrift transport type
    #[serde(default = "default_transport")]
    pub transport: ThriftTransport,

    /// Request timeout in milliseconds
    #[serde(default = "default_thrift_timeout")]
    pub timeout_ms: u64,

    /// Maximum number of retries for transient failures
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Base backoff duration in milliseconds
    #[serde(default = "default_backoff_ms")]
    pub backoff_ms: u64,
}

impl Default for HmsThriftConfig {
    fn default() -> Self {
        Self {
            protocol: default_protocol(),
            transport: default_transport(),
            timeout_ms: default_thrift_timeout(),
            max_retries: default_max_retries(),
            backoff_ms: default_backoff_ms(),
        }
    }
}

impl HmsThriftConfig {
    /// Get timeout as Duration
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    /// Get backoff as Duration
    pub fn backoff(&self) -> Duration {
        Duration::from_millis(self.backoff_ms)
    }
}

/// Thrift protocol type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThriftProtocol {
    /// Binary protocol (default, all HMS versions)
    Binary,
    /// Compact protocol (more efficient, HMS 3.0+)
    Compact,
}

/// Thrift transport type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThriftTransport {
    /// Buffered transport (simple, lower performance)
    Buffered,
    /// Framed transport (default, better for large messages)
    Framed,
}

// Default value functions for connection pool
fn default_max_size() -> u32 {
    10
}

fn default_min_idle() -> Option<u32> {
    Some(2)
}

fn default_connection_timeout() -> u64 {
    30_000 // 30 seconds
}

fn default_idle_timeout() -> u64 {
    600_000 // 10 minutes
}

fn default_max_lifetime() -> Option<u64> {
    Some(1_800_000) // 30 minutes
}

// Default value functions for cache
fn default_cache_enabled() -> bool {
    true
}

fn default_max_capacity() -> u64 {
    10_000
}

fn default_ttl() -> u64 {
    300 // 5 minutes
}

fn default_tti() -> u64 {
    180 // 3 minutes
}

// Default value functions for Thrift
fn default_protocol() -> ThriftProtocol {
    ThriftProtocol::Binary
}

fn default_transport() -> ThriftTransport {
    ThriftTransport::Framed
}

fn default_thrift_timeout() -> u64 {
    60_000 // 60 seconds
}

fn default_max_retries() -> u32 {
    3
}

fn default_backoff_ms() -> u64 {
    100
}

// Default username
fn default_username() -> String {
    "hive".to_string()
}

// Default value functions for Kerberos
fn default_kerberos_service() -> String {
    "hive".to_string()
}

fn default_mutual_auth() -> bool {
    true
}

fn default_use_ccache() -> bool {
    true
}

fn default_auto_renew() -> bool {
    false
}

fn default_sasl_qop() -> SaslQop {
    SaslQop::Auth
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = HmsConfig::default();
        assert_eq!(config.name, "hive");
        assert_eq!(config.uri, "thrift://localhost:9083");

        // Default auth is Simple with username "hive"
        assert!(matches!(config.auth, HmsAuthConfig::Simple { .. }));
        assert_eq!(config.auth.username(), Some("hive"));
        assert!(config.auth.is_secure());
        assert!(!config.auth.is_kerberos());
    }

    #[test]
    fn test_kerberos_config() {
        let auth = HmsAuthConfig::Kerberos {
            principal: "hive/hostname@REALM".to_string(),
            keytab: Some("/etc/security/keytabs/hive.keytab".to_string()),
            realm: Some("REALM".to_string()),
            service: "hive".to_string(),
            service_hostname: None,
            mutual_auth: true,
            krb5_conf: None,
            use_ccache: false,
            ticket_lifetime: None,
            auto_renew: false,
            sasl_qop: SaslQop::Auth,
        };

        assert!(auth.is_kerberos());
        assert!(auth.is_secure());
        assert_eq!(auth.username(), Some("hive/hostname@REALM"));
    }

    #[test]
    fn test_no_auth_config() {
        let auth = HmsAuthConfig::None;
        assert!(!auth.is_secure());
        assert!(!auth.is_kerberos());
        assert_eq!(auth.username(), None);
    }

    #[test]
    fn test_connection_pool_config() {
        let config = HmsConnectionPoolConfig::default();
        assert_eq!(config.max_size, 10);
        assert_eq!(config.min_idle, Some(2));
        assert_eq!(config.connection_timeout(), Duration::from_secs(30));
        assert_eq!(config.idle_timeout(), Some(Duration::from_secs(600)));
    }

    #[test]
    fn test_cache_config() {
        let config = HmsCacheConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_capacity, 10_000);
        assert_eq!(config.ttl(), Duration::from_secs(300));
        assert_eq!(config.tti(), Duration::from_secs(180));
    }

    #[test]
    fn test_thrift_config() {
        let config = HmsThriftConfig::default();
        assert_eq!(config.protocol, ThriftProtocol::Binary);
        assert_eq!(config.transport, ThriftTransport::Framed);
        assert_eq!(config.timeout(), Duration::from_secs(60));
        assert_eq!(config.max_retries, 3);
    }
}
