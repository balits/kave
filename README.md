Hello world
//elmelteti leiras + felhasznali doku + fejlesztesi doku (tesztelesi doksi + vhol template)

# Notes
- eventual consistency -> Leader only GET, no VerfiyLeader()
- strong consistency -> Leader only GET, VerifyLeader()
- dirty reads -> Leader, follower GET, no VerifyLeader()

# TODO
- [ ] fix cluster tests
- [x] simplify http server handlers
- [ ] BatchingFSM
- [ ] lease
- [ ] Snapshot metrics too
- [ ] prune random string(bytes) and []byte(string)
- [ ] bytestrore.Defragment
- [ ] raft index -> mvcc 
    - [x] Global monotonic revision
    - ~~[] Snapshot isolation~~
        - snapshot isolation is achieved by atomic reads at certain revisions
    - [x] Deterministic txn ~~executor~~ mvcc.Engine
    - [ ] Watch event log
        - refactor delete into emmiting phantom delete events, instead of noop if meta wasnt found in key_index
    - ~~[ ] Raft-triggered compaction~~
        - gonna be either periodic or retention window
    - [ ] Linearizable read path
    - [x] DELETE -> tombstone marker
    - [ ] Transactions
        - [x] applyTxnOp
            - distinguishing between txn ending errors and regural errors that should be converted into TxnOpResult
            - encode/decode should use binary so it doesnt return errors
        - [x] add TxnOpTypeGet = ~~"GET"~~ "RANGE" (if no writes chosen ops then then return early, no new rev needed)
    - [ ] Compaction
        - automatic retention window: currentRev - compactedRev > THRESHOLD
        - deterministic
        - run from raft apply or by a fsm command directly
            - [ ] or let it be configurable: could be set to periodic (--compact-timer_hourly INT), could be window retention
        - [ ] simple :
            - at revision C:
            - for each key:
                - find all revisions
                - determine latest <= C
                - delete others
            - _meta/compacted_revision = C
        - [ ] advanced:
            - keep a separate (revision, key) -> nil bucket
            - lexicographically sotred for revision, way faster to scan and delete old versions for a key
        - [ ] or even better:
            - store _meta/compacted_revision
            - during compaction, only iterate keys whose latest rev < C
                - key_index: key -> metadata{modRev}
                - if modRev < C then: key is fully below compaction window AND all but latest can be pruned aggressively
        - [ ] production grade: incremental compaction
            - store _meta/compaction_cursor
            - process 10K entries or so
            - return and let raft do the rest of the commands
            - prevents fsm stalls, lateny spikes or leader blocking
    - [x] Snapshot
        - Storage layer already handles this
        - on restore, load _meta keys into RevisionManager
    - [ ] BUCKETS
        - [x] "_meta/"
            - current_revision
            - consistent_index
                - after apply log at raft log index i: store consistent_index = i
            - compacted_revision
        - ~~[x] "key_index/":~~
            - key index is gonna be inmemory, sort of like a cache
                so we dont have to store redundant data on disk, and dont have to do additional roundtrips
            - it stores all the revisions per key, handles deleted but revived keys via generations (list of revisions)
	        - ~~Latest metadata about each key. Stores key -> (createRevision uint64, modRevision uint64, version uint64, tombstone bool)~~
        - [x] ~~"key_history/"~~ "main/":
	        - Append only historical log of all version of a key. Stores (mainRev, subRev) -> Entry{key, value, createRev, modRev, version, tombstone, leaseID}
    - [ ] Ops:
        - [ ] GET:
            - Case 1: Read latest
                - lookup key_index
                - if tombstone == true then KeyNotFound
                - else fetch values from key_history using (key, modRev)
            Case 2: Read at revision R
                Scan key_history:
                    Find highest revision ≤ R.
                    if none then key not found 
                    else if found and tombstone then deleted at that point
                    else return keyvalue
        - [x] SET:
            - allocate new revision
            - insert into key_history (key, newRev) -> "foo"
            - update key_index (createRev == null ? newRew : createRev, modRev = newRev, version == null ? 1 : ++version, tombstone = false)
            - update _meta/current_revision = newRev
            - [ ] etcd starts a new "generation" after SET on a previously deleted value
        - [x] DEL:
            - allocate new revision
            - update key_history (key, newRev) -> tombstone_marker
            - update key_index (key) -> (modRev, tombstone = true, ++version)
            - update _meta/current_revision = newRev
    - [ ] TODO: remove meta.tombstone -> use meta.version == 0 ? tombstone : no-tombstone
    - [ ] TODO: snapshot storage metrics too
    - [ ] LICENSE from etcd: http://www.apache.org/licenses/LICENSE-2.0