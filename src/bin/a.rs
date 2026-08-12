use std::{collections::HashMap, str::FromStr};

pub fn main() -> eyre::Result<()> {
    let base = std::path::PathBuf::from_str("C:/delete_after/test_dirtree/")?;
    let old_root = base.join("old_tree");
    let new_root = base.join("new_tree");

    let a = patchsync::dirwalker::walkdir(&old_root)?;
    let b = patchsync::dirwalker::walkdir(&new_root)?;

    let a_hmap = a
        .into_iter()
        .map(|x| eyre::Ok((x.into_path_root_trimmed(&old_root)?, x)))
        .collect::<Result<HashMap<_, _>, eyre::Error>>()?;
    let b_hmap = b
        .into_iter()
        .map(|x| eyre::Ok((x.into_path_root_trimmed(&new_root)?, x)))
        .collect::<Result<HashMap<_, _>, eyre::Error>>()?;

    let asdf = patchsync::snapshot::diff(a_hmap, b_hmap)?;
    dbg!(asdf);
    Ok(())
}
