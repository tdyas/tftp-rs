use std::collections::HashMap;
use std::str::FromStr;

use ascii::AsAsciiStr;
use bytes::Bytes;

pub fn parse_number<N: FromStr>(
    m: &HashMap<Bytes, Bytes>,
    key: &'static [u8],
) -> ::std::option::Option<N> {
    let key = Bytes::from_static(key);
    m.get(&key).and_then(move |x| match (*x).as_ascii_str() {
        Ok(y) => match (*y).as_str().parse::<N>() {
            Ok(v) => Some(v),
            Err(_) => None,
        },
        Err(_) => None,
    })
}
