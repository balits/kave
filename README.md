Hello world
//elmelteti leiras + felhasznali doku + fejlesztesi doku (tesztelesi doksi + vhol template)

# Notes
- eventual consistency -> Leader only GET, no VerfiyLeader()
- strong consistency -> Leader only GET, VerifyLeader()
- dirty reads -> Leader, follower GET, no VerifyLeader()

# TODO
- [ ] finish workload generator/checker
- [x] simplify http server handlers
- [ ] BatchingFSM
- [x] prune random string(bytes) and []byte(string)
- [ ] bytestrore.Defragment
- [ ] raft index -> mvcc 
    - [x] Global monotonic revision
    - ~~[x] Snapshot isolation~~
        - snapshot isolation is achieved by atomic reads at certain revisions
    - [x] Deterministic txn ~~executor~~ mvcc.Engine
    - ~~[ ] Watch event log~~ Watches are outside of this project, as of yet
        - ~~refactor delete into emmiting phantom delete events, instead of noop if meta wasnt found in key_index~~
    - [x] Raft-triggered compaction
        - gonna be either periodic or retention window | See the combined compaction scheduler below
    - [x] DELETE -> tombstone marker
    - [x] Transactions
        - [x] applyTxnOp
            - distinguishing between txn ending errors and regural errors that should be converted into TxnOpResult
            - encode/decode should use binary so it doesnt return errors
        - [x] add TxnOpTypeGet = ~~"GET"~~ "RANGE" (if no writes chosen ops then then return early, no new rev needed)
        - [ ] abort on read error
        - [ ] kv http endpoint
    - ~~Compaction~~  (see combined compactor below)
        - automatic retention window: currentRev - compactedRev > THRESHOLD
        - deterministic
        - run from raft apply or by a fsm command directly
            - [ ] or let it be configurable: could be set to periodic (--compact-timer_hourly INT), could be window retention
        - simple :
            - at revision C:
            - for each key:
                - find all revisions
                - determine latest <= C
                - delete others
            - _meta/compacted_revision = C
        - advanced:
            - keep a separate (revision, key) -> nil bucket
            - lexicographically sotred for revision, way faster to scan and delete old versions for a key
        - or even better:
            - store _meta/compacted_revision
            - during compaction, only iterate keys whose latest rev < C
                - key_index: key -> metadata{modRev}
                - if modRev < C then: key is fully below compaction window AND all but latest can be pruned aggressively
        - production grade: incremental compaction
            - store _meta/compaction_cursor
            - process 10K entries or so
            - return and let raft do the rest of the commands
            - prevents fsm stalls, lateny spikes or leader blocking
    - [x] Snapshot
        - Storage layer already handles this
        - on restore, load _meta keys into RevisionManager
        - and also restore leases
    - [x] BUCKETS
        - [x] "kv/"
        - [x] "lease/"
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
    - [x] Ops:
        - [x] GET:
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
    - [x] TODO: ~~snapshot  storage~~ compaction metrics too
    - [ ] LICENSE from etcd: http://www.apache.org/licenses/LICENSE-2.0
    - [ ] batch kvindex updates, rollback on commit failure
    - [x] mvcc.writer: support revision.sub++ on txn ops
    - [x] lease:
        - [x] type Lease
        - [x] type LeaseManager
            - [x] impl
            - [x] test
        - [x] type LeaseHeap
            - [x] impl
            - [x] test
        - [x] type Checkpoint / CheckpointScheduler
            - [x] impl
            - [x] test
        - [x] type ExpiryLoop
            - [x] impl
            - [x] test
        - [x] lm.Restore()
            - [x] impl
            - [x] test
    - [x] background routines
        - [x] create an interface with OnLeadershipGranted(f) bool or have a channel that returns leadership grants/revocatinos
            and set ticker to nil if granted := <- C; granted == false or to the real one if granted == true
    - [x] fix kvservice tests with mocked raft or similar
    - [x] combined compaction scheduler
        - [x] periodic ticks, candidateRev = rev at last tick
        - [x] threshold under which we shouldnt compact
        - [x] BUT if were between periods, but writes have accumulated fast -> lets compact
        - [x] use util.Ticker
        - [x] instead of calling compactable.Compact(), propose a command.CompactCmd to the fsm and let fsm.store handle it
    
    - MARCH.21:
    - [x] read path
        - [x] eventual consistency: only leader 
        - [x] strong consistency: only leader + VerifyLeader()
    - [x] put request
        - [x] IgnoreLease
        - [x] IgnoreValue
    - [ ] txn through kvservice (+ http)
        - [x] kvservice
        - [x] http
        - [x] general tests
        - [ ] comparison panics: imporve cmp.Check()
    - [x] backend.Defragment
        - [x] bytestore impl's Defrag()
        - [x] lock the backend and call store.Defrag()
            so no user sees an empty db while reading/writing
    - [ ] metrics
        - [ ] Snapshot metrics too
        - [ ] meaningful grafana
        - [ ] figure out where and how to collect every kind of metric
            - [ ] raft
            - [ ] kv
            - [ ] backend
            - [x] lease
    - [ ] tls
        - [ ] tls on http server
        - [ ] tls on raft inter node communication
    - [ ] handle http redirects from follower to leader
        - [ ] single host reverse proxy
    - [ ] http tests
    - [ ] cluster integratino tests
    - [ ] live workload + web ui for stats, metrics and manual commands

# CHORES
- [ ] use require in every test insteaf of if err != nil ...
- [ ] use either english or hungarian in all of the doc comments ???