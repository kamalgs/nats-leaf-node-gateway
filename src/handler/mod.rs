//! Shared handler types and delivery functions for protocol command dispatch.
//!
//! Organized into two submodules:
//! - `conn` — Connection types and handler interface (ConnCtx, ConnExt, ConnectionHandler)
//! - `delivery` — Message delivery pipeline (Msg, MessageDeliveryHub, deliver_to_subs, publish)

mod conn;
mod delivery;

pub(crate) use conn::*;
pub(crate) use delivery::*;
