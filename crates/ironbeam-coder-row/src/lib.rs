//! Beam `RowCoder` support for fixed-schema Ironbeam elements.

mod row;

pub use row::{decode_row, decode_row_evolved, encode_row, BeamRow, RowSchema};

use std::io::{Read, Write};

use anyhow::{anyhow, Context, Result};

fn encode_varint<W: Write>(mut n: u64, out: &mut W) -> Result<()> {
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.write_all(&[byte])?;
            return Ok(());
        }
        out.write_all(&[byte | 0x80])?;
    }
}

fn decode_varint<R: Read>(input: &mut R) -> Result<u64> {
    let mut value = 0_u64;
    for shift in (0..64).step_by(7) {
        let mut byte = [0_u8; 1];
        input
            .read_exact(&mut byte)
            .context("varint: unexpected EOF")?;
        if shift == 63 && byte[0] & 0x7f > 1 {
            return Err(anyhow!("varint: value exceeds u64"));
        }
        value |= u64::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(anyhow!("varint: value longer than 10 bytes"))
}
