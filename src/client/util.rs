use std::collections::HashMap;
use std::str::FromStr;

use ascii::AsAsciiStr;

pub fn parse_number<N: FromStr>(m: &HashMap<&[u8], &[u8]>, key: &[u8]) -> ::std::option::Option<N> {
    m.get(&key).and_then(move |x| match (*x).as_ascii_str() {
        Ok(y) => match (*y).as_str().parse::<N>() {
            Ok(v) => Some(v),
            Err(_) => None,
        },
        Err(_) => None,
    })
}
