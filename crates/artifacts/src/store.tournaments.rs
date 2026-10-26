//! Tournament match replays: the one kind of object the private container holds
//! that is not part of a run tree.
//!
//! The backend stores each match's replay here as the arena reports it, and reads
//! it back to serve it. A replay is one object, written whole and replaced whole,
//! under its own top-level prefix:
//!
//! ```text
//! tournaments/<tournament-id>/matches/<match-id>/replay.json
//! ```
//!
//! The prefix is reserved: no run is keyed by it (see
//! [`ArtifactStore::store_run`]), so [`ArtifactStore::list_runs`] never reports
//! it, the backend's reclamation sweep never deletes a replay, and a run tree's
//! delete never reaches one. See
//! [Tournament match replays](https://docs.testcabinet.ai/components/artifacts/overview/#tournament-match-replays).

use super::{ArtifactStore, StoreError, is_safe_segment};
use crate::object::{ObjectBody, ObjectStream};

/// The top-level prefix every tournament's objects sit under, which is
/// therefore never a run id.
pub(super) const TOURNAMENTS: &str = "tournaments";

/// The media type a replay is stored and served with.
pub const MATCH_REPLAY_CONTENT_TYPE: &str = "application/json";

impl ArtifactStore {
    /// Store `body` as the replay of match `match_id` of tournament
    /// `tournament_id`, whole, replacing any replay already stored for the
    /// match. A body that fails part-way leaves the earlier replay in place.
    /// Answers the number of bytes written; [`StoreError::UnsafeSegment`] for
    /// an id that is not a single safe path segment.
    pub async fn put_match_replay(
        &self,
        tournament_id: &str,
        match_id: &str,
        body: ObjectBody,
    ) -> Result<u64, StoreError> {
        let key = match_replay_key(tournament_id, match_id)?;
        self.objects
            .put(&key, body, MATCH_REPLAY_CONTENT_TYPE)
            .await
    }

    /// The replay of match `match_id` of tournament `tournament_id` as a
    /// stream, `Ok(None)` when none is stored; [`StoreError::UnsafeSegment`]
    /// for an id that is not a single safe path segment.
    pub async fn match_replay(
        &self,
        tournament_id: &str,
        match_id: &str,
    ) -> Result<Option<ObjectStream>, StoreError> {
        let key = match_replay_key(tournament_id, match_id)?;
        self.objects.get(&key).await
    }
}

/// The key of a match's replay, refusing an id that is not a single safe path
/// segment, so neither id can reach outside the match's own key.
fn match_replay_key(tournament_id: &str, match_id: &str) -> Result<String, StoreError> {
    for id in [tournament_id, match_id] {
        if !is_safe_segment(id) {
            return Err(StoreError::UnsafeSegment(id.to_string()));
        }
    }
    Ok(format!(
        "{TOURNAMENTS}/{tournament_id}/matches/{match_id}/replay.json"
    ))
}

#[cfg(test)]
#[path = "store.tournaments.test.rs"]
mod tests;
