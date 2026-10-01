//! Explicit integration-test access for channel contracts.

pub use crate::controller::channel::contracts::{ControllerAccount, Pin, ServiceIdentity};
pub use crate::controller::channel::{
    CHANNEL_VERSION, ChannelCodec, ChannelExecutor, ChannelFailure, ChannelReason, ChannelRuntime,
    ChildRpcSpec, CleanupContext, ClientContext, ClientDeps, ConfiguredRoute,
    DETACHED_RUNNER_EXECUTABLE_ENV, DecodeProgress, EntryIdentity, ForwardControl,
    ForwardDisposition, ForwardLease, ForwardOpenFailure, ForwardPath, ForwardPaths, FrameDecoder,
    IDENTITY_BYTES, IDLE_GUARD, IdentitySource, MAX_FEATURE_BYTES, MAX_FEATURES, MAX_FRAME_BYTES,
    MAX_HOME_BYTES, MAX_SESSIONS, MAX_SUPERVISORS, MAX_USERNAME_BYTES, MasterPlan, PIN_BYTES,
    PIN_SCHEMA_VERSION, PinStore, PinnedExecutable, READ_SCRATCH_BYTES, REQUEST_GUARD,
    ReadLoopScope, RouteDigest, RunningImage, RunningImageSource, SERVICE_SCHEMA_VERSION,
    SETUP_GUARD, SOCKET_PATH_BYTES, ServerContext, ServiceRecord, SocketBinding, SocketConnector,
    SocketIdentity, SocketIdentityResult, SocketSession, UuidString, eligible_read,
    server_eligible_read, verify_expected_service,
};
pub mod client {
    pub use crate::controller::channel::client::ChannelProcessRunner;
}
pub mod codec {
    pub use crate::controller::channel::codec::{FramedSocketConnector, SessionCodec};
    pub mod io {
        pub use crate::controller::channel::codec::io::FramedSocketConnector;
    }
}
pub mod contracts {
    pub use crate::controller::channel::contracts::{
        CHANNEL_VERSION, ChannelCodec, ChannelExecutor, ChannelFailure, ChannelReason,
        ChannelRuntime, ChildRpcSpec, CleanupContext, ClientContext, ClientDeps, ConfiguredRoute,
        DETACHED_RUNNER_EXECUTABLE_ENV, DecodeProgress, EntryIdentity, ForwardControl,
        ForwardDisposition, ForwardLease, ForwardOpenFailure, ForwardPath, ForwardPaths,
        FrameDecoder, IDENTITY_BYTES, IDLE_GUARD, IdentitySource, MAX_FEATURE_BYTES, MAX_FEATURES,
        MAX_FRAME_BYTES, MAX_HOME_BYTES, MAX_SESSIONS, MAX_SUPERVISORS, MAX_USERNAME_BYTES,
        MasterPlan, PIN_BYTES, PIN_SCHEMA_VERSION, PinStore, PinnedExecutable, READ_SCRATCH_BYTES,
        REQUEST_GUARD, ReadLoopScope, RouteDigest, RunningImage, RunningImageSource,
        SERVICE_SCHEMA_VERSION, SETUP_GUARD, SOCKET_PATH_BYTES, ServerContext, SocketBinding,
        SocketConnector, SocketIdentity, SocketIdentityResult, SocketSession, UuidString,
        eligible_read, server_eligible_read, verify_expected_service,
    };
    pub use crate::controller::channel::contracts::{
        ControllerAccount, Pin, ServiceIdentity, ServiceRecord,
    };
}
pub mod files {
    pub use crate::controller::channel::files::{
        LeaderSocketLease, PrivateChannelFiles, RetentionTime, bind_leader, bind_leader_at,
        cleanup_prior_generation,
    };
}
pub mod forward {
    pub use crate::controller::channel::forward::MasterForwardControl;
}
pub mod identity {
    pub use crate::controller::channel::identity::{
        StdioIdentitySource, is_socket_selector, read_live_service, serve_identity_selector,
    };
}
pub mod image {
    pub use crate::controller::channel::image::SystemRunningImageSource;
}
pub mod pin {
    pub use crate::controller::channel::pin::{Pin, PrivatePinStore};
}
pub mod server {
    pub use crate::controller::channel::server::{
        ChildRpcExecutor, NativeControl, ServerDeps, ShutdownEvidence, SocketService,
    };
}
pub mod testing {
    pub use crate::controller::channel::testing::{
        FakeForwardControl, FakeForwardPaths, ManualRuntime, MemoryPinStore, RecordingExecutor,
        RecordingRunner, RecordingTrackedRunner, ScriptedConnector, ScriptedIdentitySource,
        ScriptedImageSource, StubCodec, identity_fixture, request_fixture, result_fixture,
    };
}
