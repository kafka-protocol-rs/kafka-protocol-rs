use std::io::{self, Write};

use anyhow::{Context, Result};
use bytes::Bytes;

use crate::records::{DecodeBudget, DecodeResource};

/// Checks output before growth and preserves errors across codec cleanup writes.
pub(super) struct BoundedWriter<'a> {
    output: Vec<u8>,
    capacity_limit: usize,
    budget: &'a mut DecodeBudget,
    failure: Option<anyhow::Error>,
}

impl<'a> BoundedWriter<'a> {
    pub(super) fn new(budget: &'a mut DecodeBudget) -> Result<Self> {
        budget.check()?;
        Ok(Self {
            output: Vec::new(),
            capacity_limit: budget.remaining(DecodeResource::DecodedBytes),
            budget,
            failure: None,
        })
    }

    pub(super) fn finish(self, result: io::Result<()>) -> Result<Bytes> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        result?;
        Ok(self.output.into())
    }

    fn append(&mut self, buf: &[u8]) -> Result<()> {
        self.budget
            .charge(DecodeResource::DecodedBytes, buf.len())?;
        let needed = self.output.len() + buf.len();
        if needed > self.output.capacity() {
            let capacity = needed
                .max(self.output.capacity().saturating_mul(2).max(8192))
                .min(self.capacity_limit);
            self.output
                .try_reserve_exact(capacity - self.output.len())
                .context("Failed to allocate decompressed output")?;
        }
        self.output.extend_from_slice(buf);
        Ok(())
    }
}

impl Write for BoundedWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.failure.is_none() {
            match self.append(buf) {
                Ok(()) => return Ok(buf.len()),
                Err(error) => self.failure = Some(error),
            }
        }
        // Keep the original typed error outside io::Error so anyhow downcasts work.
        Err(io::Error::other("bounded decompression output failed"))
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.failure.is_some() {
            Err(io::Error::other("bounded decompression output failed"))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::{DecodeLimitExceeded, DecodeLimits};

    #[test]
    fn capacity_is_capped_and_failure_survives_cleanup() {
        let mut budget = DecodeBudget::new(DecodeLimits {
            decoded_bytes: 17,
            ..DecodeLimits::default()
        });
        let mut writer = BoundedWriter::new(&mut budget).unwrap();
        writer.write_all(&[1; 16]).unwrap();
        assert!(writer.output.capacity() <= 17);
        writer.write_all(&[1]).unwrap();
        assert!(writer.write_all(&[1]).is_err());
        assert!(writer.write_all(&[]).is_ok()); // write_all can skip write for empty input
        assert!(writer.flush().is_err());
        let error = writer.finish(Ok(())).unwrap_err();
        assert!(error.downcast_ref::<DecodeLimitExceeded>().is_some());
        assert_eq!(budget.consumed(DecodeResource::DecodedBytes), 17);
    }
}
