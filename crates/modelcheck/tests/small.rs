//! Small searches that finish quickly: both algorithms keep the safety properties, and the
//! search reaches a committed command.

use consensus::Config;
use modelcheck::{Bounds, Checkable, ConsensusModel};
use paxos::Paxos;
use raft::Raft;
use stateright::{Checker, Model};

const SMALL: Bounds = Bounds {
    servers: 2,
    commands: 1,
    elections: 1,
    heartbeats: 1,
};

fn check<C: Checkable>() {
    let model = ConsensusModel::<C>::new(SMALL, Config::default());
    let projections = model.projections();
    let checker = model.checker().threads(2).spawn_dfs().join();
    checker.assert_properties();
    eprintln!(
        "{}: {} states, {} compared states",
        std::any::type_name::<C>(),
        checker.unique_state_count(),
        projections.count()
    );
    assert!(checker.unique_state_count() > 100);
    assert!(projections.count() > 100);
    assert!(projections.count() <= checker.unique_state_count());
}

#[test]
fn raft_is_safe_in_a_small_search() {
    check::<Raft>();
}

#[test]
fn paxos_is_safe_in_a_small_search() {
    check::<Paxos>();
}
