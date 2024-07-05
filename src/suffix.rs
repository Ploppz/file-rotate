//! Suffix schemes determine the suffix of rotated files
//!
//! This behaviour is fully extensible through the [SuffixScheme] trait, and two behaviours are
//! provided: [AppendCount] and [AppendTimestamp]
//!
use crate::SuffixInfo;
use std::{
    collections::BTreeSet,
    io,
    path::{Path, PathBuf},
};

#[cfg(feature = "time")]
mod time;
#[cfg(feature = "time")]
pub use time::*;

/// Representation of a suffix
/// `Ord + PartialOrd`: sort by age of the suffix. Most recent first (smallest).
pub trait Representation: Ord + ToString + Eq + Clone + std::fmt::Debug {
    /// Create path
    fn to_path(&self, basepath: &Path) -> PathBuf {
        PathBuf::from(format!("{}.{}", basepath.display(), self.to_string()))
    }
}

/// How to move files: How to rename, when to delete.
pub trait SuffixScheme {
    /// The representation of suffixes that this suffix scheme uses.
    /// E.g. if the suffix is a number, you can use `usize`.
    type Repr: Representation;

    /// `file-rotate` calls this function when the file at `suffix` needs to be rotated, and moves the log file
    /// accordingly. Thus, this function should not move any files itself.
    ///
    /// If `suffix` is `None`, it means it's the main log file (with path equal to just `basepath`)
    /// that is being rotated.
    ///
    /// Returns the target suffix that the log file should be moved to.
    /// If the target suffix already exists, `rotate_file` is called again with `suffix` set to the
    /// target suffix.  Thus it cascades files by default, and if this is not desired, it's up to
    /// `rotate_file` to return a suffix that does not already exist on disk.
    ///
    /// `newest_suffix` is provided just in case it's useful (depending on the particular suffix scheme, it's not always useful)
    fn rotate_file(
        &mut self,
        basepath: &Path,
        newest_suffix: Option<&Self::Repr>,
        suffix: &Option<Self::Repr>,
    ) -> io::Result<Self::Repr>;

    /// Parse suffix from string.
    fn parse(&self, suffix: &str) -> Option<Self::Repr>;

    /// Whether either the suffix or the chronological file number indicates that the file is old
    /// and should be deleted, depending of course on the file limit.
    /// `file_number` starts at 0 for the most recent suffix.
    fn too_old(&self, suffix: &Self::Repr, file_number: usize) -> bool;

    /// Find all files in the basepath.parent() directory that has path equal to basepath + a valid
    /// suffix. Return sorted collection - sorted from most recent to oldest based on the
    /// [Ord] implementation of `Self::Repr`.
    fn scan_suffixes(&self, basepath: &Path) -> BTreeSet<SuffixInfo<Self::Repr>> {
        let mut suffixes = BTreeSet::new();
        let filename_prefix = basepath
            .file_name()
            .expect("basepath.file_name()")
            .to_string_lossy();

        // We need the parent directory of the given basepath, but this should also work when the path
        // only has one segment. Thus we prepend the current working dir if the path is relative:
        let basepath = if basepath.is_relative() {
            let mut path = std::env::current_dir().unwrap();
            path.push(basepath);
            path
        } else {
            basepath.to_path_buf()
        };

        let parent = basepath.parent().unwrap();

        let filenames = std::fs::read_dir(parent)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().is_file())
            .map(|entry| entry.file_name());
        for filename in filenames {
            let filename = filename.to_string_lossy();
            if !filename.starts_with(&*filename_prefix) {
                continue;
            }
            let (filename, compressed) = prepare_filename(&*filename);
            let suffix_str = filename.strip_prefix(&format!("{}.", filename_prefix));
            if let Some(suffix) = suffix_str.and_then(|s| self.parse(s)) {
                suffixes.insert(SuffixInfo { suffix, compressed });
            }
        }
        suffixes
    }
}
fn prepare_filename(path: &str) -> (&str, bool) {
    path.strip_suffix(".gz")
        .map(|x| (x, true))
        .unwrap_or((path, false))
}

/// Append a number when rotating the file.
/// The greater the number, the older. The oldest files are deleted.
pub struct AppendCount {
    max_files: usize,
}

impl AppendCount {
    /// New suffix scheme, deleting files when the number of rotated files (i.e. excluding the main
    /// file) exceeds `max_files`.
    /// For example, if `max_files` is 3, then the files `log`, `log.1`, `log.2`, `log.3` may exist
    /// but not `log.4`. In other words, `max_files` determines the largest possible suffix number.
    pub fn new(max_files: usize) -> Self {
        Self { max_files }
    }
}

impl Representation for usize {}
impl SuffixScheme for AppendCount {
    type Repr = usize;
    fn rotate_file(
        &mut self,
        _basepath: &Path,
        _: Option<&usize>,
        suffix: &Option<usize>,
    ) -> io::Result<usize> {
        Ok(match suffix {
            Some(suffix) => suffix + 1,
            None => 1,
        })
    }
    fn parse(&self, suffix: &str) -> Option<usize> {
        suffix.parse::<usize>().ok()
    }
    fn too_old(&self, _suffix: &usize, file_number: usize) -> bool {
        file_number >= self.max_files
    }
}
