use std::{error::Error, fmt};

/// Resources counted cumulatively by a [`DecodeBudget`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecodeResource {
    /// Entire decoded record-data regions, including prefixes and headers.
    DecodedBytes,
    /// Records declared by batches being decoded.
    Records,
    /// Header entries declared by records, including duplicate keys.
    Headers,
    /// Batches being decoded, including empty batches.
    Batches,
}

impl fmt::Display for DecodeResource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::DecodedBytes => "decoded bytes",
            Self::Records => "records",
            Self::Headers => "headers",
            Self::Batches => "batches",
        })
    }
}

/// Policy for eager record decoding. Zero means zero allowance for each counter.
///
/// These limits do not bound total process memory: input, collection capacity,
/// allocator overhead and native codec workspace require additional memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodeLimits {
    /// Cumulative decoded record-data bytes. Default: 128 MiB.
    pub decoded_bytes: usize,
    /// Cumulative records. Default: 1,000,000.
    pub records: usize,
    /// Cumulative header entries, including duplicates. Default: 1,000,000.
    pub headers: usize,
    /// Cumulative batches. Default: 4,096.
    pub batches: usize,
    /// Independent Zstandard buffered-history limit as a base-2 logarithm.
    /// Default: 27 (128 MiB). Lower values can reject valid high-level streams.
    /// Values must be supported by the linked Zstandard library.
    pub zstd_window_log_max: u32,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            decoded_bytes: 128 * 1024 * 1024,
            records: 1_000_000,
            headers: 1_000_000,
            batches: 4096,
            zstd_window_log_max: 27,
        }
    }
}

impl DecodeLimits {
    /// Remove byte/count policies for trusted data (up to `usize::MAX`).
    /// The independent Zstandard window limit remains at its default.
    pub fn unlimited() -> Self {
        Self {
            decoded_bytes: usize::MAX,
            records: usize::MAX,
            headers: usize::MAX,
            batches: usize::MAX,
            ..Self::default()
        }
    }

    fn get(&self, resource: DecodeResource) -> usize {
        match resource {
            DecodeResource::DecodedBytes => self.decoded_bytes,
            DecodeResource::Records => self.records,
            DecodeResource::Headers => self.headers,
            DecodeResource::Batches => self.batches,
        }
    }
}

/// A decode policy was exceeded. Available via `anyhow::Error::downcast_ref`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodeLimitExceeded {
    /// Resource whose allowance was exceeded.
    pub resource: DecodeResource,
    /// Configured allowance for that resource.
    pub limit: usize,
    /// Already charged consumption, excluding the failed charge.
    pub consumed: usize,
    /// Additional consumption attempted; may be a lower bound on total work needed.
    pub requested: usize,
}

impl fmt::Display for DecodeLimitExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Decode limit exceeded for {}: limit {}, consumed {}, requested {}; configure DecodeLimits and use a new DecodeBudget to raise the limit",
            self.resource, self.limit, self.consumed, self.requested)
    }
}

impl Error for DecodeLimitExceeded {}

/// Cumulative accounting for one caller-selected decoding scope.
///
/// Share a budget across batches or partitions of a request. Charges are never
/// refunded, including on parse/allocation errors or when returned records are
/// dropped. A limit violation permanently fails this budget; subsequent use
/// returns the original error before decoding. Input cursors are not rolled back.
#[derive(Debug, Default)]
pub struct DecodeBudget {
    limits: DecodeLimits,
    consumed: [usize; 4],
    failure: Option<DecodeLimitExceeded>,
}

impl DecodeBudget {
    /// Start a new scope with the supplied policy.
    pub fn new(limits: DecodeLimits) -> Self {
        Self {
            limits,
            consumed: [0; 4],
            failure: None,
        }
    }

    /// Start a scope without byte/count limits, retaining the default codec window.
    pub fn unlimited() -> Self {
        Self::new(DecodeLimits::unlimited())
    }

    /// The immutable policy for this scope.
    pub fn limits(&self) -> &DecodeLimits {
        &self.limits
    }

    /// Consumption charged so far for a resource.
    pub fn consumed(&self, resource: DecodeResource) -> usize {
        self.consumed[resource as usize]
    }

    /// Unspent allowance. A failed budget remains unusable even with allowance left.
    pub fn remaining(&self, resource: DecodeResource) -> usize {
        self.limits.get(resource) - self.consumed(resource)
    }

    /// Fail if an earlier limit violation made this scope unusable.
    pub fn check(&self) -> Result<(), DecodeLimitExceeded> {
        match &self.failure {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    /// Charge before allocating or processing a resource.
    ///
    /// Custom bounded decompressors can use this to account output before growth.
    pub fn charge(
        &mut self,
        resource: DecodeResource,
        amount: usize,
    ) -> Result<(), DecodeLimitExceeded> {
        self.check()?;
        if amount > self.remaining(resource) {
            let error = DecodeLimitExceeded {
                resource,
                limit: self.limits.get(resource),
                consumed: self.consumed(resource),
                requested: amount,
            };
            self.failure = Some(error.clone());
            return Err(error);
        }
        self.consumed[resource as usize] += amount;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_limits_zero_and_sticky_failures() {
        let mut budget = DecodeBudget::new(DecodeLimits {
            decoded_bytes: 3,
            records: 0,
            ..DecodeLimits::default()
        });
        budget.charge(DecodeResource::DecodedBytes, 3).unwrap();
        budget.charge(DecodeResource::Records, 0).unwrap();
        let error = budget.charge(DecodeResource::Records, 1).unwrap_err();
        assert_eq!(error.consumed, 0);
        assert_eq!(error.limit, 0);
        assert_eq!(
            budget.charge(DecodeResource::Headers, 0).unwrap_err(),
            error
        );
    }

    #[test]
    fn counter_addition_cannot_overflow() {
        let mut budget = DecodeBudget::unlimited();
        budget
            .charge(DecodeResource::DecodedBytes, usize::MAX)
            .unwrap();
        assert!(budget.charge(DecodeResource::DecodedBytes, 1).is_err());
        assert_eq!(budget.limits().zstd_window_log_max, 27);
    }
}
