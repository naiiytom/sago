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

    #[test]
    fn test_infer_by_data_email() {
        let array = StringArray::from(vec![Some("test@example.com"), Some("user@domain.org"), None, Some("another@email.net")]);
        assert_eq!(infer_semantic_type("unknown_col", &array), SemanticType::Email);
    }
}
