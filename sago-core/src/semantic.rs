use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum SemanticType {
    Email,
    CreditCard,
    PhoneNumber,
    UUID,
    IPAddress,
    Url,
    Unknown,
}

// Domain is one or more dot-separated labels; each label starts and ends with an
// alphanumeric (hyphens only in the middle), which rejects the trailing-dot/dash
// TLDs the old `[a-zA-Z0-9-.]+` suffix let through (e.g. `user@example.c-`).
static EMAIL_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+$",
    )
    .unwrap()
});
static CREDIT_CARD_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})$").unwrap()
});
static PHONE_NUMBER_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\+?[1-9]\d{1,14}$").unwrap());
static UUID_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}$",
    )
    .unwrap()
});
static IP_ADDRESS_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$").unwrap()
});
// Require a real host after the scheme: a dotted domain (label.label…), an
// optional port, then an optional path/query. The old `[^\s/$.?#].[^\s]*`
// accepted junk like `http://@@`.
static URL_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^https?://[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+(:[0-9]{1,5})?(/\S*)?$",
    )
    .unwrap()
});

pub fn infer_semantic_type(column_name: &str, array: &dyn Array) -> SemanticType {
    let lower_name = column_name.to_lowercase();

    if lower_name.contains("email") {
        return SemanticType::Email;
    } else if lower_name.contains("credit_card") || lower_name.contains("cc_number") {
        return SemanticType::CreditCard;
    } else if lower_name.contains("phone") {
        return SemanticType::PhoneNumber;
    } else if lower_name.contains("uuid") {
        return SemanticType::UUID;
    } else if lower_name
        .split(|c: char| !c.is_alphanumeric())
        .any(|seg| seg == "ip")
    {
        return SemanticType::IPAddress;
    } else if lower_name.contains("url") || lower_name.contains("website") {
        return SemanticType::Url;
    }

    // Secondary phone hints. `phone` itself is handled by the early return
    // above; these catch phone-shaped columns whose name doesn't contain
    // "phone" but is still an unambiguous phone hint, so the data vote can
    // safely classify them. A bare numeric column with none of these stays
    // Unknown rather than being mislabelled a phone number.
    let name_hints_phone = ["mobile", "msisdn", "fax", "telephone"]
        .iter()
        .any(|h| lower_name.contains(h))
        || lower_name
            .split(|c: char| !c.is_alphanumeric())
            .any(|seg| seg == "tel" || seg == "cell");

    if (array.data_type() == &DataType::Utf8 || array.data_type() == &DataType::LargeUtf8)
        && let Some(string_array) = array.as_any().downcast_ref::<StringArray>()
    {
        let mut email_count = 0;
        let mut cc_count = 0;
        let mut phone_count = 0;
        let mut uuid_count = 0;
        let mut ip_count = 0;
        let mut url_count = 0;
        let mut total_checked = 0;

        // Sample up to 100 values spread evenly across the column rather than
        // just its prefix: a column whose front happens to be homogeneous
        // (sorted data, a batch boundary, garbage padding at the start) would
        // otherwise bias classification toward that prefix's composition
        // instead of the column as a whole. `(k * len) / sample_size` for
        // `k` in `0..sample_size` lands on `sample_size` indices spread
        // evenly across `[0, len)` — and reduces to exactly `0..len` when
        // `len <= 100`, so short columns are checked in full as before.
        let len = string_array.len();
        let sample_size = std::cmp::min(100, len);

        for k in 0..sample_size {
            let i = (k * len) / sample_size;
            if !string_array.is_null(i) {
                total_checked += 1;
                let val = string_array.value(i);

                if EMAIL_REGEX.is_match(val) {
                    email_count += 1;
                } else if CREDIT_CARD_REGEX.is_match(val) {
                    cc_count += 1;
                } else if PHONE_NUMBER_REGEX.is_match(val) {
                    phone_count += 1;
                } else if UUID_REGEX.is_match(val) {
                    uuid_count += 1;
                } else if IP_ADDRESS_REGEX.is_match(val) {
                    ip_count += 1;
                } else if URL_REGEX.is_match(val) {
                    url_count += 1;
                }
            }
        }

        if total_checked > 0 {
            let threshold = (total_checked as f32) * 0.8;
            if (email_count as f32) >= threshold {
                return SemanticType::Email;
            } else if (cc_count as f32) >= threshold {
                return SemanticType::CreditCard;
            } else if (phone_count as f32) >= threshold && name_hints_phone {
                // The phone regex matches any 2–15 digit integer (optionally
                // `+`-prefixed), so plain numeric ID/count/code columns would
                // otherwise be misclassified as phone numbers. Only accept the
                // phone vote when the column *name* also hints at a phone; the
                // name-based branch above already returns early for obvious cases
                // like `phone`, so this guards the ambiguous ones.
                return SemanticType::PhoneNumber;
            } else if (uuid_count as f32) >= threshold {
                return SemanticType::UUID;
            } else if (ip_count as f32) >= threshold {
                return SemanticType::IPAddress;
            } else if (url_count as f32) >= threshold {
                return SemanticType::Url;
            }
        }
    }

    SemanticType::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;

    // ── name-based inference ─────────────────────────────────────────────────

    #[test]
    fn test_infer_by_name() {
        let array = StringArray::from(vec!["test"]);
        assert_eq!(
            infer_semantic_type("user_email", &array),
            SemanticType::Email
        );
        assert_eq!(
            infer_semantic_type("credit_card_number", &array),
            SemanticType::CreditCard
        );
        assert_eq!(
            infer_semantic_type("phone_num", &array),
            SemanticType::PhoneNumber
        );
        assert_eq!(
            infer_semantic_type("session_uuid", &array),
            SemanticType::UUID
        );
        assert_eq!(
            infer_semantic_type("client_ip", &array),
            SemanticType::IPAddress
        );
        assert_eq!(
            infer_semantic_type("website_url", &array),
            SemanticType::Url
        );
    }

    #[test]
    fn test_ip_name_regression() {
        // The `ip` name-hint must match only as a whole word-segment, not as a
        // substring — `script`, `zip`, `tip`, `recipient` previously triggered
        // false-positive IPAddress classification.
        let array = StringArray::from(vec!["test"]);

        // Should NOT classify as IPAddress
        for name in [
            "script",
            "scripted",
            "zip_code",
            "tip_amount",
            "recipient",
            "shipment",
        ] {
            assert_eq!(
                infer_semantic_type(name, &array),
                SemanticType::Unknown,
                "{name} should not match IPAddress",
            );
        }

        // Should still classify as IPAddress
        for name in ["ip_address", "client_ip", "ip", "source.ip", "user-ip-addr"] {
            assert_eq!(
                infer_semantic_type(name, &array),
                SemanticType::IPAddress,
                "{name} should match IPAddress",
            );
        }
    }

    // ── data-based inference — each semantic type ────────────────────────────

    #[test]
    fn test_infer_by_data_email() {
        let array = StringArray::from(vec![
            Some("test@example.com"),
            Some("user@domain.org"),
            None,
            Some("another@email.net"),
        ]);
        assert_eq!(
            infer_semantic_type("unknown_col", &array),
            SemanticType::Email
        );
    }

    #[test]
    fn test_infer_by_data_credit_card() {
        // Visa test numbers
        let array = StringArray::from(vec![
            Some("4111111111111111"),
            Some("4012888888881881"),
            Some("4222222222222"),
            Some("4111111111111111"),
            Some("4012888888881881"),
        ]);
        assert_eq!(
            infer_semantic_type("unknown_col", &array),
            SemanticType::CreditCard
        );
    }

    #[test]
    fn test_infer_by_data_phone_requires_name_hint() {
        let array = StringArray::from(vec![
            Some("+14155552671"),
            Some("+442071234567"),
            Some("+33123456789"),
            Some("+14155552672"),
            Some("+14155552673"),
        ]);
        // With a secondary phone hint in the name, the data vote classifies it.
        assert_eq!(
            infer_semantic_type("mobile", &array),
            SemanticType::PhoneNumber
        );
        // Without any phone hint, phone-shaped digits must NOT be labelled a
        // phone number (they could be IDs, order numbers, etc.).
        assert_eq!(
            infer_semantic_type("unknown_col", &array),
            SemanticType::Unknown
        );
    }

    #[test]
    fn test_numeric_id_column_not_misclassified_as_phone() {
        // Regression: short unhinted numeric strings (e.g. a `code` column) must
        // stay Unknown even though they match the permissive phone regex.
        let array = StringArray::from(vec![
            Some("12345"),
            Some("67890"),
            Some("24680"),
            Some("13579"),
            Some("11223"),
        ]);
        assert_eq!(infer_semantic_type("code", &array), SemanticType::Unknown);
    }

    #[test]
    fn test_infer_by_data_uuid() {
        let array = StringArray::from(vec![
            Some("550e8400-e29b-41d4-a716-446655440000"),
            Some("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
            Some("6ba7b811-9dad-11d1-80b4-00c04fd430c8"),
            Some("6ba7b812-9dad-11d1-80b4-00c04fd430c8"),
            Some("6ba7b813-9dad-11d1-80b4-00c04fd430c8"),
        ]);
        assert_eq!(
            infer_semantic_type("unknown_col", &array),
            SemanticType::UUID
        );
    }

    #[test]
    fn test_infer_by_data_ip() {
        let array = StringArray::from(vec![
            Some("192.168.1.1"),
            Some("10.0.0.1"),
            Some("172.16.0.1"),
            Some("8.8.8.8"),
            Some("1.1.1.1"),
        ]);
        assert_eq!(
            infer_semantic_type("unknown_col", &array),
            SemanticType::IPAddress
        );
    }

    #[test]
    fn test_infer_by_data_url() {
        let array = StringArray::from(vec![
            Some("https://example.com"),
            Some("http://foo.org/path?q=1"),
            Some("https://bar.io"),
            Some("https://baz.net/page"),
            Some("http://qux.com"),
        ]);
        assert_eq!(
            infer_semantic_type("unknown_col", &array),
            SemanticType::Url
        );
    }

    // ── regex tightening regressions ─────────────────────────────────────────

    #[test]
    fn test_email_regex_rejects_trailing_dot_or_dash_tld() {
        // Malformed TLDs must not classify as Email (data-based path, no name hint).
        for bad in ["user@example.c-", "user@example.c.", "user@-example.com"] {
            let array = StringArray::from(vec![bad, bad, bad, bad, bad]);
            assert_eq!(
                infer_semantic_type("col", &array),
                SemanticType::Unknown,
                "{bad} must not be classified Email",
            );
        }
    }

    #[test]
    fn test_email_regex_still_accepts_valid() {
        let array = StringArray::from(vec![
            "a@example.com",
            "b.c+tag@sub.domain.co.uk",
            "d_e@example.io",
            "f@example.com",
            "g@example.com",
        ]);
        assert_eq!(infer_semantic_type("col", &array), SemanticType::Email);
    }

    #[test]
    fn test_url_regex_rejects_hostless_junk() {
        for bad in ["http://@@", "https://", "http:// spaced.com"] {
            let array = StringArray::from(vec![bad, bad, bad, bad, bad]);
            assert_eq!(
                infer_semantic_type("col", &array),
                SemanticType::Unknown,
                "{bad} must not be classified Url",
            );
        }
    }

    #[test]
    fn test_url_regex_still_accepts_valid() {
        let array = StringArray::from(vec![
            "https://example.com",
            "http://foo.org/path?q=1",
            "https://bar.io",
            "https://baz.net:8080/page",
            "http://qux.com",
        ]);
        assert_eq!(infer_semantic_type("col", &array), SemanticType::Url);
    }

    // ── threshold boundary behaviour ─────────────────────────────────────────

    #[test]
    fn test_threshold_below_80_percent() {
        // 6 emails out of 10 = 60% — below the 80% threshold
        let array = StringArray::from(vec![
            Some("a@example.com"),
            Some("b@example.com"),
            Some("c@example.com"),
            Some("d@example.com"),
            Some("e@example.com"),
            Some("f@example.com"),
            Some("not-an-email"),
            Some("not-an-email"),
            Some("not-an-email"),
            Some("not-an-email"),
        ]);
        assert_eq!(
            infer_semantic_type("unknown_col", &array),
            SemanticType::Unknown
        );
    }

    #[test]
    fn test_threshold_at_80_percent() {
        // 8 emails out of 10 = exactly 80% — meets the threshold
        let array = StringArray::from(vec![
            Some("a@example.com"),
            Some("b@example.com"),
            Some("c@example.com"),
            Some("d@example.com"),
            Some("e@example.com"),
            Some("f@example.com"),
            Some("g@example.com"),
            Some("h@example.com"),
            Some("not-an-email"),
            Some("not-an-email"),
        ]);
        assert_eq!(
            infer_semantic_type("unknown_col", &array),
            SemanticType::Email
        );
    }

    // ── sampling spread ──────────────────────────────────────────────────────

    #[test]
    fn test_sampling_is_spread_not_just_prefix() {
        // 1000 rows: first 100 are junk, the remaining 900 are emails. A
        // prefix-only sample (the first 100 rows) would see 0% emails and
        // return Unknown; a sample spread evenly across the whole column
        // lands ~90% of its 100 picks past index 100, clearing the 80%
        // threshold and correctly classifying the column as Email.
        let mut values: Vec<Option<&str>> = vec![Some("not-an-email"); 100];
        values.extend(std::iter::repeat_n(Some("user@example.com"), 900));
        let array = StringArray::from(values);
        assert_eq!(
            infer_semantic_type("unknown_col", &array),
            SemanticType::Email
        );
    }

    #[test]
    fn test_sampling_covers_full_short_column() {
        // A column no longer than the sample size (100) must still be
        // checked in full, matching the pre-fix behaviour for small data.
        let array = StringArray::from(vec![Some("user@example.com"); 50]);
        assert_eq!(
            infer_semantic_type("unknown_col", &array),
            SemanticType::Email
        );
    }

    // ── non-Utf8 and all-null arrays ─────────────────────────────────────────

    #[test]
    fn test_non_utf8_array() {
        let array = Int32Array::from(vec![1, 2, 3]);
        assert_eq!(
            infer_semantic_type("unknown_col", &array),
            SemanticType::Unknown
        );
    }

    #[test]
    fn test_all_null_array() {
        let array = StringArray::from(vec![None::<&str>, None, None]);
        assert_eq!(
            infer_semantic_type("unknown_col", &array),
            SemanticType::Unknown
        );
    }

    // ── SemanticType round-trip (JSON serialization) ──────────────────────────

    #[test]
    fn test_semantic_type_json_round_trip() {
        for v in [
            SemanticType::Email,
            SemanticType::CreditCard,
            SemanticType::PhoneNumber,
            SemanticType::UUID,
            SemanticType::IPAddress,
            SemanticType::Url,
            SemanticType::Unknown,
        ] {
            let json = serde_json::to_string(&v).unwrap();
            let back: SemanticType = serde_json::from_str(&json).unwrap();
            assert_eq!(v, back);
        }
    }
}
