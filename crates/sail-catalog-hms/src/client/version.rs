//! HMS version detection and parsing

use crate::error::{HmsError, HmsResult};
use std::cmp::Ordering;
use std::fmt;

/// HMS version information
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HmsVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl HmsVersion {
    /// Parse version from string (e.g., "3.1.3" or "4.0.0")
    pub fn parse(version_str: &str) -> HmsResult<Self> {
        let parts: Vec<&str> = version_str.split('.').collect();

        if parts.len() < 2 {
            return Err(HmsError::VersionDetectionFailed(format!(
                "Invalid version format: {}",
                version_str
            )));
        }

        let major = parts[0].parse().map_err(|_| {
            HmsError::VersionDetectionFailed(format!("Invalid major version: {}", parts[0]))
        })?;

        let minor = parts[1].parse().map_err(|_| {
            HmsError::VersionDetectionFailed(format!("Invalid minor version: {}", parts[1]))
        })?;

        let patch = if parts.len() > 2 {
            parts[2].parse().unwrap_or(0)
        } else {
            0
        };

        Ok(Self {
            major,
            minor,
            patch,
        })
    }

    /// Check if this version is at least the given version
    pub fn at_least(&self, other: &HmsVersion) -> bool {
        self >= other
    }

    /// Check if this version is exactly the given version
    pub fn exact(&self, other: &HmsVersion) -> bool {
        self == other
    }

    /// Get version as tuple
    pub fn as_tuple(&self) -> (u32, u32, u32) {
        (self.major, self.minor, self.patch)
    }
}

impl fmt::Display for HmsVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl PartialOrd for HmsVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HmsVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        self.major
            .cmp(&other.major)
            .then(self.minor.cmp(&other.minor))
            .then(self.patch.cmp(&other.patch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_version() {
        let v = HmsVersion::parse("3.1.3").unwrap();
        assert_eq!(v.major, 3);
        assert_eq!(v.minor, 1);
        assert_eq!(v.patch, 3);
    }

    #[test]
    fn test_parse_version_no_patch() {
        let v = HmsVersion::parse("4.0").unwrap();
        assert_eq!(v.major, 4);
        assert_eq!(v.minor, 0);
        assert_eq!(v.patch, 0);
    }

    #[test]
    fn test_version_comparison() {
        let v2_3 = HmsVersion::parse("2.3.0").unwrap();
        let v3_0 = HmsVersion::parse("3.0.0").unwrap();
        let v3_1 = HmsVersion::parse("3.1.3").unwrap();
        let v4_0 = HmsVersion::parse("4.0.0").unwrap();

        assert!(v3_0 > v2_3);
        assert!(v3_1 > v3_0);
        assert!(v4_0 > v3_1);

        assert!(v3_1.at_least(&v3_0));
        assert!(!v3_0.at_least(&v3_1));
    }

    #[test]
    fn test_version_display() {
        let v = HmsVersion::parse("3.1.3").unwrap();
        assert_eq!(v.to_string(), "3.1.3");
    }

    #[test]
    fn test_invalid_version() {
        assert!(HmsVersion::parse("invalid").is_err());
        assert!(HmsVersion::parse("").is_err());
    }
}
