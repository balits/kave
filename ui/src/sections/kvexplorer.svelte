<script lang="ts">
  import type { KaveClient } from '$lib/kave_client'
  import type { Entry, ResponseHeader } from '$lib/kv'
  import { KaveError } from '$lib/kv'
 
  let { client }: { client: KaveClient } = $props()
 
  type Op = 'put' | 'get' | 'range' | 'delete'
 
  // inputs:
  let op       = $state<Op>('put')
  let key      = $state('')
  let value    = $state('')
  let end      = $state('')
  let revision = $state(0)
  let prefix   = $state(false)
  let leaseId  = $state(0)
 
  // output:
  let loading     = $state(false)
  let error       = $state<string | null>(null)
  let header      = $state<ResponseHeader | null>(null)
  let entries     = $state<Entry[]>([])
  let numDeleted  = $state<number | null>(null)
  let resultLabel = $state<string | null>(null) // label shown above the entries panel: "prev entry", "results", etc.
 
  function reset_output() {
    error = null
    header = null
    entries = []
    numDeleted = null
    resultLabel = null
  }
 
  function selectOp(next: Op) {
    op = next
    reset_output()
  }
 
  async function exec() {
    if (!key.trim()) { error = 'key is required'; return }
    reset_output()
    loading = true

    try {
      switch (op) {
        case 'put': {
          const res = await client.kvPut(key, value, {
            leaseId: leaseId || undefined,
          })
          header = res.header
          if (res.prev_entry) {
            entries = [res.prev_entry]
            resultLabel = 'previous entry'
          } else {
            resultLabel = null
          }
          break
        }
        case 'get': {
          const res = await client.kvGet(key, {
            revision: revision || undefined,
          })
          header = res.header
          entries = res.entries
          resultLabel = revision > 0
            ? `entry at revision ${revision}`
            : 'entry'
          break
        }
        case 'range': {
          const res = await client.kvRange(key, {
            end:      end || undefined,
            prefix,
            revision: revision || undefined,
          })
          header = res.header
          entries = res.entries
          resultLabel = `${res.count} result${res.count !== 1 ? 's' : ''}${revision > 0 ? ` at revision ${revision}` : ''}`
          break
        }
        case 'delete': {
          const res = await client.kvDelete(key, {
            end:         end || undefined,
            prevEntries: true,
          })
          header = res.header
          numDeleted = res.num_deleted
          entries = res.prev_entries ?? []
          resultLabel = `deleted ${res.num_deleted} key${res.num_deleted !== 1 ? 's' : ''}`
          break
        }
      }
    } catch (e) {
      if (e instanceof KaveError) {
        error = e.cause ? `${e.message} — ${e.cause}` : e.message
      } else if (e instanceof Error) {
        error = e.message
      } else {
        error = 'unknown error'
      }
    } finally {
      loading = false
    }
  }
 
  function hex(n: number): string {
    return n === 0 ? '—' : `0x${n.toString(16).padStart(8, '0')}`
  }
</script>

<section class="">
  <div class="" role="tablist">
    {#each ['put','get','range','delete'] as o (o)}
      <button
        role="tab"
        class="tab"
        class:active={op === o}
        onclick={() => selectOp(o as Op)}
      >{o.toUpperCase()}</button>
    {/each}
  </div>
 
  <div class="">
    <div class="">
      <label for="key">key</label>
      <input id="key" bind:value={key} placeholder="e.g. myapp/config" spellcheck="false" />
    </div>
 
    {#if op === 'put'}
      <div class="">
        <label for="val">value</label>
        <input id="val" bind:value={value} placeholder="e.g. hello" spellcheck="false" />
      </div>
      <div class="">
        <label for="lid">lease id <span class="hint">(0 = none)</span></label>
        <input id="lid" type="number" min="0" bind:value={leaseId} />
      </div>
    {/if}
 
    {#if op === 'range' || op === 'delete'}
      <div class="">
        <label for="end">
          end key <span class="">[key, end) range — leave empty for single key</span>
        </label>
        <input id="end" bind:value={end} placeholder="e.g. myapp/z" spellcheck="false" />
      </div>
    {/if}
 
    {#if op === 'range'}
      <label class="">
        <input type="checkbox" bind:checked={prefix} />
        prefix scan
      </label>
    {/if}
 
    {#if op === 'get' || op === 'range'}
      <div class="">
        <label for="rev">
          revision <span class="">(0 = current — try an older revision to see MVCC in action)</span>
        </label>
        <input id="rev" type="number" min="0" bind:value={revision} />
      </div>
    {/if}
 
    <button class="" onclick={exec} disabled={loading}>
      {#if loading}
        <span class=""></span>
      {:else}
        {op.toUpperCase()}
      {/if}
    </button>
  </div>
 
  {#if error}
    <div class="">
      <span class="">✕</span>
      <span>{error}</span>
    </div>
  {/if}
 
  {#if header}
    <div class="">
      <div class="">
        <span class="">revision</span>
        <span class="">{header.revision}</span>
      </div>
      <div class="">
        <span class="">compacted</span>
        <span class="">{header.compacted_revision}</span>
      </div>
      <div class="">
        <span class="">term</span>
        <span class="">{header.raft_term}</span>
      </div>
      <div class="">
        <span class="">index</span>
        <span class="">{header.raft_index}</span>
      </div>
      <div class="">
        <span class="">node</span>
        <span class="">{header.node_id}</span>
      </div>
    </div>
  {/if}
 
  {#if resultLabel !== null || entries.length > 0}
    <div class="results-header">
      <span class="results-label">{resultLabel ?? ''}</span>
    </div>
 
    {#if entries.length === 0 && numDeleted === null}
      <div class="empty">key not found</div>
    {/if}
 
    {#each entries as entry (entry.key + ':' + entry.mod_revision)}
      <div class="entry">
        <!-- key / value row -->
        <div class="entry-kv">
          <span class="entry-key">{entry.key}</span>
          <span class="entry-sep">→</span>
          {#if entry.value}
            <span class="entry-val">{entry.value}</span>
          {:else}
            <span class="entry-tombstone">⌀ tombstone</span>
          {/if}
        </div>
 
        <!-- metadata grid — this is the MVCC story -->
        <div class="meta-grid">
          <div class="meta-cell">
            <span class="meta-label">create_rev</span>
            <span class="meta-val">{entry.create_revision}</span>
          </div>
          <div class="meta-cell">
            <span class="meta-label">mod_rev</span>
            <span class="meta-val accent">{entry.mod_revision}</span>
          </div>
          <div class="meta-cell">
            <span class="meta-label">version</span>
            <span class="meta-val accent">{entry.version}</span>
          </div>
          <div class="meta-cell">
            <span class="meta-label">lease_id</span>
            <span class="meta-val dim">{hex(entry.lease_id)}</span>
          </div>
        </div>
      </div>
    {/each}
 
    <!-- MVCC callout shown only when reading at a past revision -->
    {#if op === 'get' && revision > 0 && entries.length > 0}
      <div class="mvcc-note?">
        You are reading a snapshot at revision <strong>{revision}</strong>.
        The store is append-only — this entry was never overwritten, just superseded.
        Increment the revision to walk forward through history.
      </div>
    {/if}
  {/if}
 
</section>
