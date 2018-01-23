pub struct TftpConfig {
    pub block_size: u16,
    pub retransmission_timeout: u16,
    pub max_retries: u16,
}

impl Default for TftpConfig {
    fn default() -> Self {
        TftpConfig {
            block_size: 512,
            retransmission_timeout: 1,
            max_retries: 5,
        }
    }
}
