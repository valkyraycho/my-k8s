//! Raft consensus, from scratch, per the paper's Figure 2. The core is a PURE
//! state machine (`core::RaftNode::step`); persistence, timers, and transport
//! live in the shell modules around it.

pub mod log;
pub mod message;
