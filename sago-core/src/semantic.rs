use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use regex::Regex;
use serde::Serialize;

#[derive(Debug, Serialize, PartialEq, Clone)]
pub enum SemanticType {
    Email,
    CreditCard,
    PhoneNumber,
    UUID,
    IPAddress,
    Url,
    Unknown,
}

lazy_static::lazy_static! {
    static ref EMAIL_REGEX: Regex = Regex::new(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$").unwrap();
    static ref CREDIT_CARD_REGEX: Regex = Regex::new(r"^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})$").unwrap();
    static ref PHONE_NUMBER_REGEX: Regex = Regex::new(r"^\+?[1-9]\d{1,14}$").unwrap();
    static ref UUID_REGEX: Regex = Regex::new(r"^[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}$").unwrap();
    static ref IP_ADDRESS_REGEX: Regex = Regex::new(r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$").unwrap();
    static ref URL_REGEX: Regex = Regex::new(r"^https?://[^\s/$.?#].[^\s]*$").unwrap();
}

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
    } else if lower_name.contains("ip_address") || lower_name.contains("ip") {
        return SemanticType::IPAddress;
    } else if lower_name.contains("url") || lower_name.contains("website") {
        return SemanticType::Url;
    }

    if array.data_type() == &DataType::Utf8 || array.data_type() == &DataType::LargeUtf8 {
        if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
            let mut email_count = 0;
            let mut cc_count = 0;
            let mut phone_count = 0;
            let mut uuid_count = 0;
            let mut ip_count = 0;
            let mut url_count = 0;
            let mut total_checked = 0;

            let check_limit = std::cmp::min(100, string_array.len());

            for i in 0..check_limit {
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
                } else if (phone_count as f32) >= threshold {
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
        assert_eq!(infer_semantic_type("user_email", &array), SemanticType::Email);
        assert_eq!(infer_semantic_type("credit_card_number", &array), SemanticType::CreditCard);
        assert_eq!(infer_semantic_type("phone_num", &array), SemanticType::PhoneNumber);
        assert_eq!(infer_semantic_type("session_uuid", &array), SemanticType::UUID);
        assert_eq!(infer_semantic_type("client_ip", &array), SemanticType::IPAddress);
        assert_eq!(infer_semantic_type("website_url", &array), SemanticType::Url);
    }

    // ── data-based inference — each semantic type ────────────────────────────

    #[test]
    fn test_infer_by_data_email() {
        let array = StringArray::from(vec![
            Some("test@example.com"), Some("user@domain.org"), None, Some("another@email.net"),
        ]);
        assert_eq!(infer_semantic_type("unknown_col", &array), SemanticType::Email);
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
        assert_eq!(infer_semantic_type("unknown_col", &array), SemanticType::CreditCard);
    }

    #[test]
    fn test_infer_by_data_phone() {
        let array = StringArray::from(vec![
            Some("+14155552671"),
            Some("+442071234567"),
            Some("+33123456789"),
            Some("+14155552672"),
            Some("+14155552673"),
        ]);
        assert_eq!(infer_semantic_type("unknown_col", &array), SemanticType::PhoneNumber);
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
        assert_eq!(infer_semantic_type("unknown_col", &array), SemanticType::UUID);
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
        assert_eq!(infer_semantic_type("unknown_col", &array), SemanticType::IPAddress);
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
        assert_eq!(infer_semantic_type("unknown_col", &array), SemanticType::Url);
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
        assert_eq!(infer_semantic_type("unknown_col", &array), SemanticType::Unknown);
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
        assert_eq!(infer_semantic_type("unknown_col", &array), SemanticType::Email);
    }

    // ── non-Utf8 and all-null arrays ─────────────────────────────────────────

    #[test]
    fn test_non_utf8_array() {
        let array = Int32Array::from(vec![1, 2, 3]);
        assert_eq!(infer_semantic_type("unknown_col", &array), SemanticType::Unknown);
    }

    #[test]
    fn test_all_null_array() {
        let array = StringArray::from(vec![None::<&str>, None, None]);
        assert_eq!(infer_semantic_type("unknown_col", &array), SemanticType::Unknown);
    }
}
