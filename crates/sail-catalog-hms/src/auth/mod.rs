//! Authentication module for HMS
//!
//! This module provides various authentication mechanisms for HMS connections:
//! - No authentication (for testing)
//! - Simple username/password
//! - Kerberos (SASL/GSSAPI)
//! - Delegation tokens

pub mod kerberos;

use crate::config::HmsAuthConfig;
use crate::error::{HmsError, HmsResult};
use kerberos::{KerberosAuth, KerberosCredentials, SaslNegotiator};
use tracing::{debug, info};

/// Authentication manager for HMS connections
pub struct AuthManager {
    config: HmsAuthConfig,
    kerberos_auth: Option<KerberosAuth>,
    kerberos_creds: Option<KerberosCredentials>,
}

impl AuthManager {
    /// Create a new authentication manager
    pub async fn new(config: HmsAuthConfig) -> HmsResult<Self> {
        let mut manager = Self {
            config: config.clone(),
            kerberos_auth: None,
            kerberos_creds: None,
        };

        // Initialize Kerberos if configured
        if config.is_kerberos() {
            manager.initialize_kerberos().await?;
        }

        Ok(manager)
    }

    /// Initialize Kerberos authentication
    async fn initialize_kerberos(&mut self) -> HmsResult<()> {
        info!("Initializing Kerberos authentication");

        let auth = KerberosAuth::from_config(&self.config)?;
        let creds = auth.initialize().await?;

        self.kerberos_auth = Some(auth);
        self.kerberos_creds = Some(creds);

        Ok(())
    }

    /// Get authentication type name
    pub fn auth_type(&self) -> &str {
        match &self.config {
            HmsAuthConfig::None => "none",
            HmsAuthConfig::Simple { .. } => "simple",
            HmsAuthConfig::Kerberos { .. } => "kerberos",
            HmsAuthConfig::DelegationToken { .. } => "token",
        }
    }

    /// Get username for authentication
    pub fn username(&self) -> Option<String> {
        self.config.username().map(String::from)
    }

    /// Authenticate a Thrift connection
    pub async fn authenticate(&self, server_hostname: &str) -> HmsResult<AuthContext> {
        match &self.config {
            HmsAuthConfig::None => {
                debug!("No authentication configured");
                Ok(AuthContext::None)
            }

            HmsAuthConfig::Simple { username, password } => {
                debug!("Using simple authentication for user: {}", username);
                Ok(AuthContext::Simple {
                    username: username.clone(),
                    password: password.clone(),
                })
            }

            HmsAuthConfig::Kerberos { .. } => {
                debug!("Using Kerberos authentication");

                let auth = self.kerberos_auth.as_ref().ok_or_else(|| {
                    HmsError::Internal("Kerberos auth not initialized".to_string())
                })?;

                let creds = self.kerberos_creds.as_ref().ok_or_else(|| {
                    HmsError::Internal("Kerberos credentials not initialized".to_string())
                })?;

                // Perform SASL negotiation
                let negotiator = SaslNegotiator::new(auth.clone(), creds.clone());
                let sasl_context = negotiator.negotiate(server_hostname).await?;

                Ok(AuthContext::Kerberos {
                    sasl_context: Box::new(sasl_context),
                })
            }

            HmsAuthConfig::DelegationToken { token, identifier, service } => {
                debug!("Using delegation token authentication");
                Ok(AuthContext::DelegationToken {
                    token: token.clone(),
                    identifier: identifier.clone(),
                    service: service.clone(),
                })
            }
        }
    }

    /// Renew Kerberos credentials if needed
    pub async fn renew_credentials(&mut self) -> HmsResult<()> {
        if let Some(ref mut creds) = self.kerberos_creds {
            if !creds.is_valid() {
                info!("Renewing Kerberos credentials");
                creds.renew().await?;
            }
        }
        Ok(())
    }

    /// Check if credentials are still valid
    pub fn is_valid(&self) -> bool {
        match &self.config {
            HmsAuthConfig::None | HmsAuthConfig::Simple { .. } => true,
            HmsAuthConfig::Kerberos { .. } => {
                self.kerberos_creds.as_ref().map(|c| c.is_valid()).unwrap_or(false)
            }
            HmsAuthConfig::DelegationToken { .. } => true, // TODO: Check token expiration
        }
    }
}

/// Authentication context for an active connection
#[derive(Debug)]
pub enum AuthContext {
    /// No authentication
    None,

    /// Simple username/password authentication
    Simple {
        username: String,
        password: Option<String>,
    },

    /// Kerberos authentication with SASL context
    Kerberos {
        sasl_context: Box<kerberos::SaslContext>,
    },

    /// Delegation token authentication
    DelegationToken {
        token: String,
        identifier: Option<String>,
        service: Option<String>,
    },
}

impl AuthContext {
    /// Get authentication headers for Thrift
    pub fn get_auth_headers(&self) -> Vec<(String, String)> {
        match self {
            AuthContext::None => vec![],

            AuthContext::Simple { username, .. } => {
                vec![("X-Hive-User".to_string(), username.clone())]
            }

            AuthContext::Kerberos { sasl_context } => {
                vec![
                    ("X-Hive-Auth".to_string(), "GSSAPI".to_string()),
                    ("X-Hive-Service".to_string(), sasl_context.service_principal.clone()),
                ]
            }

            AuthContext::DelegationToken { token, .. } => {
                vec![("X-Hive-Delegation-Token".to_string(), token.clone())]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_no_auth() {
        let config = HmsAuthConfig::None;
        let manager = AuthManager::new(config).await.unwrap();

        assert_eq!(manager.auth_type(), "none");
        assert!(manager.is_valid());
        assert_eq!(manager.username(), None);
    }

    #[tokio::test]
    async fn test_simple_auth() {
        let config = HmsAuthConfig::Simple {
            username: "testuser".to_string(),
            password: Some("testpass".to_string()),
        };

        let manager = AuthManager::new(config).await.unwrap();

        assert_eq!(manager.auth_type(), "simple");
        assert!(manager.is_valid());
        assert_eq!(manager.username(), Some("testuser".to_string()));
    }

    #[tokio::test]
    async fn test_auth_context_headers() {
        let ctx = AuthContext::Simple {
            username: "user".to_string(),
            password: None,
        };

        let headers = ctx.get_auth_headers();
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].0, "X-Hive-User");
        assert_eq!(headers[0].1, "user");
    }
}
