use super::*;
use crate::now;
use chrono::{format::ParseErrorKind, offset::Local, Duration, NaiveDateTime};
use std::cmp::Ordering;

/// Add timestamp from:
pub enum DateFrom {
    /// Date yesterday, to represent the timestamps within the log file.
    DateYesterday,
    /// Date from hour ago, useful with rotate hourly.
    DateHourAgo,
    /// Date from now.
    Now,
}

/// Append current timestamp as suffix when rotating files.
/// If the timestamp already exists, an additional number is appended.
///
/// Current limitations:
///  - Neither `format` nor the base filename can include the character `"."`.
///  - The `format` should ensure that the lexical and chronological orderings are the same
pub struct AppendTimestamp {
    /// The format of the timestamp suffix
    pub format: &'static str,
    /// The file limit, e.g. when to delete an old file - by age (given by suffix) or by number of files
    pub file_limit: FileLimit,
    /// Add timestamp from DateFrom
    pub date_from: DateFrom,
}

impl AppendTimestamp {
    /// With format `"%Y%m%dT%H%M%S"`
    pub fn default(file_limit: FileLimit) -> Self {
        Self {
            format: "%Y%m%dT%H%M%S",
            file_limit,
            date_from: DateFrom::Now,
        }
    }
    /// Create new AppendTimestamp suffix scheme
    pub fn with_format(format: &'static str, file_limit: FileLimit, date_from: DateFrom) -> Self {
        Self {
            format,
            file_limit,
            date_from,
        }
    }
}

/// Structured representation of the suffixes of AppendTimestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampSuffix {
    /// The timestamp
    pub timestamp: String,
    /// Optional number suffix if two timestamp suffixes are the same
    pub number: Option<usize>,
}
impl Representation for TimestampSuffix {}
impl Ord for TimestampSuffix {
    fn cmp(&self, other: &Self) -> Ordering {
        // Most recent = smallest (opposite as the timestamp Ord)
        // Smallest = most recent. Thus, biggest timestamp first. And then biggest number
        match other.timestamp.cmp(&self.timestamp) {
            Ordering::Equal => other.number.cmp(&self.number),
            unequal => unequal,
        }
    }
}
impl PartialOrd for TimestampSuffix {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl std::fmt::Display for TimestampSuffix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self.number {
            Some(n) => write!(f, "{}.{}", self.timestamp, n),
            None => write!(f, "{}", self.timestamp),
        }
    }
}

impl SuffixScheme for AppendTimestamp {
    type Repr = TimestampSuffix;

    fn rotate_file(
        &mut self,
        _basepath: &Path,
        newest_suffix: Option<&TimestampSuffix>,
        suffix: &Option<TimestampSuffix>,
    ) -> io::Result<TimestampSuffix> {
        assert!(suffix.is_none());
        if suffix.is_none() {
            let mut now = now();

            match self.date_from {
                DateFrom::DateYesterday => {
                    now = now - Duration::days(1);
                }
                DateFrom::DateHourAgo => {
                    now = now - Duration::hours(1);
                }
                _ => {}
            };

            let fmt_now = now.format(self.format).to_string();

            let number = if let Some(newest_suffix) = newest_suffix {
                if newest_suffix.timestamp == fmt_now {
                    Some(newest_suffix.number.unwrap_or(0) + 1)
                } else {
                    None
                }
            } else {
                None
            };
            Ok(TimestampSuffix {
                timestamp: fmt_now,
                number,
            })
        } else {
            // This rotation scheme dictates that only the main log file should ever be renamed.
            // In debug build the above assert will catch this.
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Critical error in file-rotate algorithm",
            ))
        }
    }
    fn parse(&self, suffix: &str) -> Option<Self::Repr> {
        let (timestamp_str, n) = if let Some(dot) = suffix.find('.') {
            if let Ok(n) = suffix[(dot + 1)..].parse::<usize>() {
                (&suffix[..dot], Some(n))
            } else {
                return None;
            }
        } else {
            (suffix, None)
        };
        let success = match NaiveDateTime::parse_from_str(timestamp_str, self.format) {
            Ok(_) => true,
            Err(e) => e.kind() == ParseErrorKind::NotEnough,
        };
        if success {
            Some(TimestampSuffix {
                timestamp: timestamp_str.to_string(),
                number: n,
            })
        } else {
            None
        }
    }
    fn too_old(&self, suffix: &TimestampSuffix, file_number: usize) -> bool {
        match self.file_limit {
            FileLimit::MaxFiles(max_files) => file_number >= max_files,
            FileLimit::Age(age) => {
                let old_timestamp = (Local::now() - age).format(self.format).to_string();
                suffix.timestamp < old_timestamp
            }
            FileLimit::Unlimited => false,
        }
    }
}

/// How to determine whether a file should be deleted, in the case of [AppendTimestamp].
pub enum FileLimit {
    /// Delete the oldest files if number of files is too high
    MaxFiles(usize),
    /// Delete files whose age exceeds the `Duration` - age is determined by the suffix of the file
    Age(Duration),
    /// Never delete files
    Unlimited,
}

#[cfg(test)]
mod test {
    use crate::suffix::*;
    use chrono::Duration;
    use std::fs::File;
    use tempfile::TempDir;
    #[test]
    fn timestamp_ordering() {
        assert!(
            TimestampSuffix {
                timestamp: "2021".to_string(),
                number: None
            } < TimestampSuffix {
                timestamp: "2020".to_string(),
                number: None
            }
        );
        assert!(
            TimestampSuffix {
                timestamp: "2021".to_string(),
                number: Some(1)
            } < TimestampSuffix {
                timestamp: "2021".to_string(),
                number: None
            }
        );
    }

    #[test]
    fn timestamp_scan_suffixes_base_paths() {
        let working_dir = TempDir::new().unwrap();
        let working_dir = working_dir.path().join("dir");
        let suffix_scheme = AppendTimestamp::default(FileLimit::Age(Duration::weeks(1)));

        // Test `scan_suffixes` for different possible paths given to it
        // (it used to have a bug taking e.g. "log".parent() --> panic)
        for relative_path in ["logs/log", "./log", "log", "../log", "../logs/log"] {
            std::fs::create_dir_all(&working_dir).unwrap();
            println!("Testing relative path: {}", relative_path);
            let relative_path = Path::new(relative_path);

            let log_file = working_dir.join(relative_path);
            let log_dir = log_file.parent().unwrap();
            // Ensure all directories needed exist
            std::fs::create_dir_all(log_dir).unwrap();

            // We cd into working_dir
            std::env::set_current_dir(&working_dir).unwrap();

            // Need to create the log file in order to canonicalize it and then get the parent
            File::create(working_dir.join(&relative_path)).unwrap();
            let canonicalized = relative_path.canonicalize().unwrap();
            let relative_dir = canonicalized.parent().unwrap();

            File::create(relative_dir.join("log.20210911T121830")).unwrap();
            File::create(relative_dir.join("log.20210911T121831.gz")).unwrap();

            let paths = suffix_scheme.scan_suffixes(relative_path);
            assert_eq!(paths.len(), 2);

            // Reset CWD: necessary on Windows only - otherwise we get the error:
            // "The process cannot access the file because it is being used by another process."
            // (code 32)
            std::env::set_current_dir("/").unwrap();

            // Cleanup
            std::fs::remove_dir_all(&working_dir).unwrap();
        }
    }

    #[test]
    fn timestamp_scan_suffixes_formats() {
        struct TestCase {
            format: &'static str,
            suffixes: &'static [&'static str],
            incorrect_suffixes: &'static [&'static str],
        }

        let cases = [
            TestCase {
                format: "%Y%m%dT%H%M%S",
                suffixes: &["20220201T101010", "20220202T101010"],
                incorrect_suffixes: &["20220201T1010", "20220201T999999", "2022-02-02"],
            },
            TestCase {
                format: "%Y-%m-%d",
                suffixes: &["2022-02-01", "2022-02-02"],
                incorrect_suffixes: &[
                    "abc",
                    "2022-99-99",
                    "2022-05",
                    "2022",
                    "20220202",
                    "2022-02-02T112233",
                ],
            },
        ];

        for (i, case) in cases.iter().enumerate() {
            println!("Case {}", i);
            let tmp_dir = TempDir::new().unwrap();
            let dir = tmp_dir.path();
            let log_path = dir.join("file");

            for suffix in case.suffixes.iter().chain(case.incorrect_suffixes) {
                File::create(dir.join(format!("file.{}", suffix))).unwrap();
            }

            let scheme = AppendTimestamp::with_format(
                case.format,
                FileLimit::MaxFiles(1),
                DateFrom::DateYesterday,
            );

            // Scan for suffixes
            let suffixes_set = scheme.scan_suffixes(&log_path);

            // Collect these suffixes, and the expected suffixes, into Vec, and sort
            let mut suffixes = suffixes_set
                .into_iter()
                .map(|x| x.suffix.to_string())
                .collect::<Vec<_>>();
            suffixes.sort_unstable();

            let mut expected_suffixes = case.suffixes.to_vec();
            expected_suffixes.sort_unstable();

            assert_eq!(suffixes, case.suffixes);
            println!("Passed\n");
        }
    }
}
