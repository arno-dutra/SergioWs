use crate::mask::mask_inplace;
use crate::{Frame, OpCode, Payload};
use bytes::Bytes;
use std::io::IoSlice;

pub enum MessageOut {
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close(Vec<u8>),
    Text(String),
    Binary(Vec<u8>),
    FragmentedBinary(Vec<Bytes>),
}

impl MessageOut {
    pub fn is_close(&self) -> bool {
        matches!(self, MessageOut::Close(_))
    }

    pub fn is_fragmented(&self) -> bool {
        matches!(self, MessageOut::FragmentedBinary(_))
    }

    pub(crate) fn to_single_frame(self, mask: Option<[u8; 4]>) -> Frame<'static> {
        match self {
            MessageOut::Ping(payload) => Frame::new(true, OpCode::Ping, mask, Payload::Owned(payload)),
            MessageOut::Pong(payload) => Frame::new(true, OpCode::Pong, mask, Payload::Owned(payload)),
            MessageOut::Close(payload) => Frame::new(true, OpCode::Close, mask, Payload::Owned(payload)),
            MessageOut::Text(text) => Frame::new(true, OpCode::Text, mask, Payload::Owned(text.into_bytes())),
            MessageOut::Binary(data) => Frame::new(true, OpCode::Binary, mask, Payload::Owned(data)),
            MessageOut::FragmentedBinary(_) => panic!("Cannot convert fragmented binary message to a single frame"),
        }
    }

    pub(crate) fn build_header_for_fragmented_message(&self, mask: &Option<[u8; 4]>) -> Vec<u8> {
        let mask_bit: u8 = if mask.is_some() { 0b1000_0000 } else { 0 };

        match self {
            Self::FragmentedBinary(fragments) => {
                let mut header = Vec::with_capacity(10);
                let total_length: usize = fragments.iter().map(|f| f.len()).sum();

                header.push(
                    0b1000_0000
                        | 0b0000_0010 // Opcode: Binary
                );

                if total_length < 126 {
                    header.push(total_length as u8 | mask_bit);
                } else if total_length < 65536 {
                    header.push(126 | mask_bit);
                    header.extend_from_slice(&(total_length as u16).to_be_bytes());
                } else {
                    header.push(127 | mask_bit);
                    header.extend_from_slice(&(total_length as u64).to_be_bytes());
                }

                if let Some(mask) = mask {
                    header.extend_from_slice(mask);
                }
                header
            }
            _ => panic!("Header can only be built for fragmented messages"),
        }
    }

    pub(crate) fn fragmented_to_slices<'a>(&'a self, header: &'a [u8]) -> Vec<IoSlice<'a>> {
        match self {
            Self::FragmentedBinary(fragments) => {
                let mut slices = Vec::with_capacity(fragments.len() + 1);
                slices.push(IoSlice::new(&header));
                for fragment in fragments {
                    slices.push(IoSlice::new(fragment));
                }
                slices
            }
            _ => {
                panic!("Cannot convert non-fragmented message to slices");
            }
        }
    }

    pub(crate) fn apply_mask(self, mask: Option<[u8; 4]>) -> Self {
        match self {
            Self::FragmentedBinary(raw_fragments) => {
                Self::FragmentedBinary(if let Some(mask) = mask {
                    // According to ./benches/mask.rs it's faster to collect the vector and then to apply the mask than do them at the same time.
                    let mut collected = Vec::with_capacity(raw_fragments.iter().map(|f| f.len()).sum());
                    for bytes in raw_fragments {
                        collected.extend_from_slice(&bytes)
                    }
                    mask_inplace(collected.as_mut_slice(), mask);
                    vec![Bytes::from_owner(collected)]
                } else {
                    raw_fragments
                })
            }
            _ => panic!("Cannot apply mask to non-fragmented message"),
        }
    }
}