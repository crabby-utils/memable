use std::time::Duration;

/// Configures automatic retry behavior for a step.
///
/// When a step closure returns [`StepError::Retryable`](crate::StepError::Retryable),
/// the engine retries the closure according to this policy. Permanent errors
/// bypass retries entirely.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use memable::RetryPolicy;
///
/// // Retry up to 3 times with 1-second fixed delay.
/// let fixed = RetryPolicy::fixed(3, Duration::from_secs(1));
///
/// // Retry up to 5 times with exponential backoff (1s, 2s, 4s, 8s, 16s).
/// let expo = RetryPolicy::exponential(5, Duration::from_secs(1));
///
/// // Cap exponential delay at 10 seconds.
/// let capped = RetryPolicy::exponential(5, Duration::from_secs(1))
///     .with_max_delay(Duration::from_secs(10));
/// ```
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub(crate) max_retries: u32,
    pub(crate) backoff: Backoff,
}

#[derive(Debug, Clone)]
pub(crate) enum Backoff {
    Fixed(Duration),
    Exponential { base: Duration, max: Duration },
}

impl RetryPolicy {
    /// Creates a policy with a fixed delay between attempts.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use memable::RetryPolicy;
    ///
    /// let policy = RetryPolicy::fixed(3, Duration::from_secs(1));
    /// ```
    #[must_use]
    pub fn fixed(max_retries: u32, delay: Duration) -> Self {
        Self {
            max_retries,
            backoff: Backoff::Fixed(delay),
        }
    }

    /// Creates a policy with exponential backoff.
    ///
    /// Delay doubles each attempt: `base`, `2*base`, `4*base`, etc.,
    /// capped at 30 seconds by default. Use [`with_max_delay`](Self::with_max_delay)
    /// to change the cap.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use memable::RetryPolicy;
    ///
    /// let policy = RetryPolicy::exponential(3, Duration::from_secs(1));
    /// ```
    #[must_use]
    pub fn exponential(max_retries: u32, base: Duration) -> Self {
        Self {
            max_retries,
            backoff: Backoff::Exponential {
                base,
                max: Duration::from_secs(30),
            },
        }
    }

    /// Sets the maximum delay for exponential backoff.
    ///
    /// Has no effect on fixed-delay policies.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use memable::RetryPolicy;
    ///
    /// let policy = RetryPolicy::exponential(5, Duration::from_secs(1))
    ///     .with_max_delay(Duration::from_secs(10));
    /// ```
    #[must_use]
    pub fn with_max_delay(mut self, max: Duration) -> Self {
        if let Backoff::Exponential { max: ref mut m, .. } = self.backoff {
            *m = max;
        }
        self
    }

    /// Returns the delay before the given retry attempt (0-indexed).
    pub(crate) fn delay_for(&self, attempt: u32) -> Duration {
        match self.backoff {
            Backoff::Fixed(d) => d,
            Backoff::Exponential { base, max } => {
                let multiplier = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
                let delay = base.saturating_mul(multiplier.try_into().unwrap_or(u32::MAX));
                delay.min(max)
            }
        }
    }
}
