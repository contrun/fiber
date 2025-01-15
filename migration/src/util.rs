use serde::{Deserialize, Serialize};
use std::io::{stdin, stdout, Write};

pub(crate) fn convert<Old, New>(old: &Old) -> New
where
    Old: Serialize,
    New: for<'de> Deserialize<'de>,
{
    let mut serialized = Vec::new();
    ciborium::into_writer(old, &mut serialized).unwrap();
    ciborium::from_reader(serialized.as_slice()).expect("deserialize to new state")
}

pub fn prompt(msg: &str) -> String {
    let stdout = stdout();
    let mut stdout = stdout.lock();
    let stdin = stdin();

    write!(stdout, "{msg}").unwrap();
    stdout.flush().unwrap();

    let mut input = String::new();
    let _ = stdin.read_line(&mut input);

    input
}
