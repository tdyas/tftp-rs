#[derive(Eq, PartialEq, Clone, Debug)]
pub struct TftpConfig {
    pub max_block_size: u16,
    pub retransmission_timeout: u16,
    pub max_retries: u16,
    pub trace_packets: bool,
    pub disable_options: bool,
}

impl Default for TftpConfig {
    fn default() -> Self {
        TftpConfig {
            max_block_size: 512,
            retransmission_timeout: 2,
            max_retries: 5,
            trace_packets: true,
            disable_options: false,
        }
    }
}
