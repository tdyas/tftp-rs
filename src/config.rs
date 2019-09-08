#[derive(Eq, PartialEq, Clone, Debug)]
pub struct TftpConfig {
    pub max_block_size: u16,
    pub retransmission_timeout: u16,
    pub max_retries: u16,
    pub trace_packets: bool,
    pub enable_tsize_option: bool,
    pub enable_blksize_option: bool,
}

impl Default for TftpConfig {
    fn default() -> Self {
        TftpConfig {
            max_block_size: 512,
            retransmission_timeout: 2,
            max_retries: 5,
            trace_packets: true,
            enable_tsize_option: true,
            enable_blksize_option: true,
        }
    }
}
