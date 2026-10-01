//! T1 dependency gate, not production codec/I/O coverage (owned by T2).
use mac_worker::controller::{
    channel::{
        codec::ChannelCodec,
        testing::{StubCodec, identity_fixture},
    },
    decode_frame,
};

#[test]
fn gate_codec_contract_accepts_optional_journal_changes() {
    let codec: Box<dyn ChannelCodec> = Box::new(StubCodec);
    let expected = identity_fixture();
    let mut actual = expected.clone();
    actual.service.journal_id = None;
    let ready = codec.encode_ready(&actual).unwrap();
    codec
        .decode_ready(decode_frame(&ready).unwrap(), &expected)
        .unwrap();
}
