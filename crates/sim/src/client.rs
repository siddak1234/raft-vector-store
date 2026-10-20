//! Closed-loop clients.
//!
//! A closed-loop client sends one command, waits for the commit reply, and then sends the
//! next. Commands travel over the simulated network with the same delay model as messages
//! between servers. Partitions and loss apply only to links between servers.

use consensus::Command;

use crate::SimTime;

/// Bytes at the start of every client command that identify it: the client number (4
/// bytes) and the command's sequence number (8 bytes), little-endian.
pub(crate) const HEADER: usize = 12;

/// The client number used for commands proposed directly by an experiment procedure.
pub(crate) const DIRECT: u32 = u32::MAX;

/// Builds a command of `size` bytes (at least [`HEADER`]) that identifies its client and
/// sequence number.
pub(crate) fn encode(client: u32, seq: u64, size: usize) -> Command {
    let mut bytes = vec![0; size.max(HEADER)];
    bytes[..4].copy_from_slice(&client.to_le_bytes());
    bytes[4..HEADER].copy_from_slice(&seq.to_le_bytes());
    Command(bytes)
}

/// Returns the client number and sequence number of a command built by [`encode`].
pub(crate) fn decode(command: &Command) -> Option<(u32, u64)> {
    let bytes = command.0.get(..HEADER)?;
    let client = u32::from_le_bytes(bytes[..4].try_into().ok()?);
    let seq = u64::from_le_bytes(bytes[4..].try_into().ok()?);
    Some((client, seq))
}

pub(crate) struct Client {
    /// The server slot the client sends to next.
    pub(crate) target: usize,
    pub(crate) next_seq: u64,
    /// Increases with every send, so stale timeouts and redirects can be ignored.
    pub(crate) attempt: u64,
    pub(crate) outstanding: Option<Outstanding>,
}

pub(crate) struct Outstanding {
    pub(crate) seq: u64,
    pub(crate) first_sent: SimTime,
}

#[cfg(test)]
mod tests {
    use super::{decode, encode};

    #[test]
    fn round_trip() {
        let command = encode(7, 42, 128);
        assert_eq!(command.0.len(), 128);
        assert_eq!(decode(&command), Some((7, 42)));
        assert_eq!(encode(1, 2, 0).0.len(), 12);
    }
}
