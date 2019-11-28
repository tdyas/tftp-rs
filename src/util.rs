use std::io;

use tokio::io::{AsyncRead, AsyncReadExt};

pub async fn read_full<R: AsyncRead + Unpin>(
    reader: &mut R,
    buffer: &mut [u8],
) -> io::Result<usize> {
    let mut current_len: usize = 0;
    let desired_len = buffer.len();
    while current_len < desired_len {
        let n = reader.read(&mut buffer[current_len..]).await?;
        current_len += n;
        if current_len == desired_len || n == 0 {
            break;
        }
    }
    Ok(current_len)
}
