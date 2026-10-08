use super::Evaluator;
use chrono::{Datelike, Timelike};
use serde_json::Value;

impl Evaluator {
    // ---------- datetime functions (all in UTC) ----------

    /// Resolve a value to epoch milliseconds: numbers directly, RFC3339
    /// strings via parsing, other strings when numerically parseable.
    pub(crate) fn to_epoch_millis(v: &Value) -> Option<i64> {
        if let Some(s) = v.as_str() {
            let t = s.trim();
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(t) {
                return Some(dt.timestamp_millis());
            }
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S%.f") {
                return Some(dt.and_utc().timestamp_millis());
            }
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S") {
                return Some(dt.and_utc().timestamp_millis());
            }
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S%.f") {
                return Some(dt.and_utc().timestamp_millis());
            }
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S") {
                return Some(dt.and_utc().timestamp_millis());
            }
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(t, "%Y/%m/%d %H:%M:%S%.f") {
                return Some(dt.and_utc().timestamp_millis());
            }
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(t, "%Y/%m/%d %H:%M:%S") {
                return Some(dt.and_utc().timestamp_millis());
            }
            if let Ok(d) = chrono::NaiveDate::parse_from_str(t, "%Y-%m-%d") {
                if let Some(dt) = d.and_hms_opt(0, 0, 0) {
                    return Some(dt.and_utc().timestamp_millis());
                }
            }
            if let Ok(d) = chrono::NaiveDate::parse_from_str(t, "%Y/%m/%d") {
                if let Some(dt) = d.and_hms_opt(0, 0, 0) {
                    return Some(dt.and_utc().timestamp_millis());
                }
            }
        }
        Self::to_i64_arg(v)
    }

    pub(crate) fn datetime_from_millis(ms: i64) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::from_timestamp_millis(ms)
    }

    pub(crate) fn func_now(args: &[Value]) -> Value {
        match args.len() {
            0 => Value::String(chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()),
            1 => {
                if let Some(fmt_str) = args[0].as_str() {
                    let pattern = Self::java_to_strftime(fmt_str);
                    Value::String(chrono::Utc::now().format(&pattern).to_string())
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        }
    }

    fn java_to_strftime(fmt: &str) -> String {
        if fmt.contains('%') {
            return fmt.to_string();
        }
        fmt.replace("YYYY", "%Y")
            .replace("yyyy", "%Y")
            .replace("yy", "%y")
            .replace("MM", "%m")
            .replace("dd", "%d")
            .replace("DD", "%d")
            .replace("HH", "%H")
            .replace("hh", "%I")
            .replace("mm", "%M")
            .replace("ss", "%S")
            .replace("SSS", "%3f")
    }

    pub(crate) fn func_format_date(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(fmt) = args[1].as_str() else {
            return Value::Null;
        };
        let dt = match &args[0] {
            Value::Number(_) => {
                Self::to_epoch_millis(&args[0]).and_then(Self::datetime_from_millis)
            }
            Value::String(_) => {
                if let Some(s) = args[0].as_str().and_then(|s| {
                    chrono::DateTime::parse_from_rfc3339(s.trim())
                        .ok()
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                }) {
                    Some(s)
                } else {
                    Self::to_epoch_millis(&args[0]).and_then(Self::datetime_from_millis)
                }
            }
            _ => None,
        };
        let pattern = Self::java_to_strftime(fmt);
        match dt {
            Some(dt) => Value::String(dt.format(&pattern).to_string()),
            None => Value::Null,
        }
    }

    pub(crate) fn func_date_parse(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let (Some(s), Some(fmt)) = (args[0].as_str(), args[1].as_str()) else {
            return Value::Null;
        };
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, fmt) {
            return Value::from(dt.timestamp_millis());
        }
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Value::from(dt.and_utc().timestamp_millis());
        }
        if let Ok(d) = chrono::NaiveDate::parse_from_str(s, fmt) {
            if let Some(dt) = d.and_hms_opt(0, 0, 0) {
                return Value::from(dt.and_utc().timestamp_millis());
            }
        }
        Value::Null
    }

    /// Milliseconds per interval unit: dd|day, hh|hour, mi|minute|min,
    /// ss|second|sec, ms|millisecond (case-insensitive).
    fn interval_unit_millis(part: &str) -> Option<i64> {
        match part.trim().to_ascii_lowercase().as_str() {
            "dd" | "day" => Some(86_400_000),
            "hh" | "hour" => Some(3_600_000),
            "mi" | "minute" | "min" => Some(60_000),
            "ss" | "second" | "sec" => Some(1_000),
            "ms" | "millisecond" => Some(1),
            _ => None,
        }
    }

    pub(crate) fn func_date_add(args: &[Value]) -> Value {
        if args.len() != 3 {
            return Value::Null;
        }
        let (Some(part), Some(num)) = (args[0].as_str(), Self::to_i64_arg(&args[1])) else {
            return Value::Null;
        };
        let (Some(unit), Some(ts)) = (
            Self::interval_unit_millis(part),
            Self::to_epoch_millis(&args[2]),
        ) else {
            return Value::Null;
        };
        match num.checked_mul(unit).and_then(|d| ts.checked_add(d)) {
            Some(ms) => Value::from(ms),
            None => Value::Null,
        }
    }

    pub(crate) fn func_date_diff(args: &[Value]) -> Value {
        if args.len() == 2 {
            let (Some(unit), Some(t1), Some(t2)) = (
                Self::interval_unit_millis("day"),
                Self::to_epoch_millis(&args[0]),
                Self::to_epoch_millis(&args[1]),
            ) else {
                return Value::Null;
            };
            return match t2.checked_sub(t1) {
                Some(diff) => Value::from(diff / unit),
                None => Value::Null,
            };
        }
        if args.len() != 3 {
            return Value::Null;
        }
        let Some(part) = args[0].as_str() else {
            return Value::Null;
        };
        let (Some(unit), Some(t1), Some(t2)) = (
            Self::interval_unit_millis(part),
            Self::to_epoch_millis(&args[1]),
            Self::to_epoch_millis(&args[2]),
        ) else {
            return Value::Null;
        };
        match t2.checked_sub(t1) {
            // Integer division truncates toward zero, matching SQL semantics.
            Some(diff) => Value::from(diff / unit),
            None => Value::Null,
        }
    }

    fn parse_duration_millis(s: &str) -> Option<i64> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        let (neg, s) = if let Some(rest) = s.strip_prefix('-') {
            (true, rest)
        } else if let Some(rest) = s.strip_prefix('+') {
            (false, rest)
        } else {
            (false, s)
        };

        let mut total_millis: i64 = 0;
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let start_num = i;
            let mut has_dot = false;
            while i < bytes.len() && (bytes[i].is_ascii_digit() || (bytes[i] == b'.' && !has_dot)) {
                if bytes[i] == b'.' {
                    has_dot = true;
                }
                i += 1;
            }
            if i == start_num {
                return None;
            }
            let num: f64 = s[start_num..i].parse().ok()?;

            let start_unit = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphabetic() || s[i..].starts_with('µ')) {
                if s[i..].starts_with('µ') {
                    i += 'µ'.len_utf8();
                } else {
                    i += 1;
                }
            }
            let unit = &s[start_unit..i];
            let factor = match unit {
                "ns" => 0.000_001,
                "us" | "µs" => 0.001,
                "ms" => 1.0,
                "s" => 1_000.0,
                "m" => 60_000.0,
                "h" => 3_600_000.0,
                "d" => 86_400_000.0,
                _ => return None,
            };
            total_millis = total_millis.checked_add((num * factor).round() as i64)?;
        }
        if neg {
            Some(-total_millis)
        } else {
            Some(total_millis)
        }
    }

    pub(crate) fn func_date_calc(args: &[Value]) -> Value {
        if args.len() != 2 {
            return Value::Null;
        }
        let Some(ts) = Self::to_epoch_millis(&args[0]) else {
            return Value::Null;
        };
        let Some(dur_str) = args[1].as_str() else {
            return Value::Null;
        };
        let Some(diff_ms) = Self::parse_duration_millis(dur_str) else {
            return Value::Null;
        };
        match ts.checked_add(diff_ms) {
            Some(res) => Value::from(res),
            None => Value::Null,
        }
    }

    pub(crate) fn func_convert_tz(args: &[Value]) -> Value {
        if args.len() < 2 || args.len() > 3 {
            return Value::Null;
        }
        let Some(to_tz_name) = (match args.len() {
            2 => args[1].as_str(),
            3 => args[2].as_str(),
            _ => None,
        }) else {
            return Value::Null;
        };
        let is_local = to_tz_name.eq_ignore_ascii_case("local");
        let to_tz: Option<chrono_tz::Tz> = if is_local {
            None
        } else if to_tz_name.eq_ignore_ascii_case("utc") {
            Some(chrono_tz::UTC)
        } else {
            to_tz_name.parse().ok()
        };
        if !is_local && to_tz.is_none() {
            return Value::Null;
        }

        let Some(millis) = Self::to_epoch_millis(&args[0]) else {
            return Value::Null;
        };
        let Some(dt) = Self::datetime_from_millis(millis) else {
            return Value::Null;
        };

        if is_local {
            let converted = dt.with_timezone(&chrono::Local);
            Value::String(converted.format("%Y-%m-%d %H:%M:%S").to_string())
        } else {
            let converted = dt.with_timezone(&to_tz.unwrap());
            Value::String(converted.format("%Y-%m-%d %H:%M:%S").to_string())
        }
    }

    fn datetime_component<F>(args: &[Value], extract: F) -> Value
    where
        F: Fn(chrono::DateTime<chrono::Utc>) -> i32,
    {
        if args.len() != 1 {
            return Value::Null;
        }
        match Self::to_epoch_millis(&args[0]).and_then(Self::datetime_from_millis) {
            Some(dt) => Value::from(extract(dt)),
            None => Value::Null,
        }
    }

    pub(crate) fn func_year(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| dt.year())
    }

    pub(crate) fn func_month(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| dt.month() as i32)
    }

    pub(crate) fn func_day(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| dt.day() as i32)
    }

    pub(crate) fn func_hour(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| dt.hour() as i32)
    }

    pub(crate) fn func_minute(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| dt.minute() as i32)
    }

    pub(crate) fn func_second(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| dt.second() as i32)
    }

    pub(crate) fn func_current_date(args: &[Value]) -> Value {
        if !args.is_empty() {
            return Value::Null;
        }
        Value::String(chrono::Utc::now().format("%Y-%m-%d").to_string())
    }

    pub(crate) fn func_current_time(args: &[Value]) -> Value {
        if !args.is_empty() {
            return Value::Null;
        }
        Value::String(chrono::Utc::now().format("%H:%M:%S").to_string())
    }

    pub(crate) fn func_from_unix_time(args: &[Value]) -> Value {
        if args.is_empty() || args.len() > 2 {
            return Value::Null;
        }
        if args[0].is_null() {
            return Value::Null;
        }
        if args.len() == 2 {
            return Self::func_format_date(args);
        }
        match Self::to_epoch_millis(&args[0]).and_then(Self::datetime_from_millis) {
            Some(dt) => Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()),
            None => Value::Null,
        }
    }

    /// MySQL convention: 1 = Sunday through 7 = Saturday.
    pub(crate) fn func_day_of_week(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| dt.weekday().num_days_from_sunday() as i32 + 1)
    }

    pub(crate) fn func_day_of_year(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| dt.ordinal() as i32)
    }

    pub(crate) fn func_day_name(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        const NAMES: [&str; 7] = [
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
        ];
        match Self::to_epoch_millis(&args[0]).and_then(Self::datetime_from_millis) {
            Some(dt) => {
                Value::String(NAMES[dt.weekday().num_days_from_sunday() as usize].to_string())
            }
            None => Value::Null,
        }
    }

    pub(crate) fn func_month_name(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        const NAMES: [&str; 12] = [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ];
        match Self::to_epoch_millis(&args[0]).and_then(Self::datetime_from_millis) {
            Some(dt) => Value::String(NAMES[dt.month() as usize - 1].to_string()),
            None => Value::Null,
        }
    }

    pub(crate) fn func_microsecond(args: &[Value]) -> Value {
        Self::datetime_component(args, |dt| (dt.timestamp_subsec_micros() % 1_000_000) as i32)
    }

    /// Last calendar day of the argument's month as `"YYYY-MM-DD"`.
    pub(crate) fn func_last_day(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let dt = match Self::to_epoch_millis(&args[0]).and_then(Self::datetime_from_millis) {
            Some(dt) => dt.date_naive(),
            None => return Value::Null,
        };
        let (next_year, next_month) = if dt.month() == 12 {
            (dt.year() + 1, 1)
        } else {
            (dt.year(), dt.month() + 1)
        };
        match chrono::NaiveDate::from_ymd_opt(next_year, next_month, 1)
            .and_then(|first| first.pred_opt())
        {
            Some(last) => Value::String(last.format("%Y-%m-%d").to_string()),
            None => Value::Null,
        }
    }

    /// MySQL `TO_SECONDS`: seconds from year 0 to `ts`.
    /// `num_days_from_ce` counts from 0001-01-01, and year 0 contributes a
    /// further 365 days (matching `TO_SECONDS('0001-01-01') = 31622400`).
    pub(crate) fn func_to_seconds(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        match Self::to_epoch_millis(&args[0]) {
            Some(ms) => Value::from(ms / 1000),
            None => Value::Null,
        }
    }

    /// MySQL `FROM_DAYS`: day count since year 0 back to `"YYYY-MM-DD"`.
    pub(crate) fn func_from_days(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::Null;
        }
        let Some(n) = Self::to_i64_arg(&args[0]) else {
            return Value::Null;
        };
        // The chrono constructor takes i32 days; out-of-range inputs fail.
        let days = n.checked_sub(365).and_then(|d| i32::try_from(d).ok());
        match days.and_then(chrono::NaiveDate::from_num_days_from_ce_opt) {
            Some(date) => Value::String(date.format("%Y-%m-%d").to_string()),
            None => Value::Null,
        }
    }
}

/// Validate whether a timezone identifier is a supported named timezone
/// (case-insensitive "local" or "utc", or any valid IANA/chrono-tz timezone).
pub fn is_valid_timezone(tz_name: &str) -> bool {
    let s = tz_name.trim();
    if s.is_empty() {
        return false;
    }
    if s.eq_ignore_ascii_case("local") || s.eq_ignore_ascii_case("utc") {
        return true;
    }
    s.parse::<chrono_tz::Tz>().is_ok()
}
