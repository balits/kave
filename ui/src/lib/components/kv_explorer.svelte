<script lang="ts">
	import type { KaveClient } from '$lib/kave_client';
	import type { Entry, ResponseHeader } from '$lib/kv';
	import { KaveError } from '$lib/kv';

	let { client }: { client: KaveClient } = $props();

	type Op = 'put' | 'get' | 'range' | 'delete';

	// inputs:
	let op = $state<Op>('put');
	let key = $state('');
	let value = $state('');
	let end = $state('');
	let revision = $state(0);
	let prefix = $state(false);
	let leaseId = $state(0);

	// output:
	let loading = $state(false);
	let error = $state<string | null>(null);
	let header = $state<ResponseHeader | null>(null);
	let entries = $state<Entry[]>([]);
	let numDeleted = $state<number | null>(null);
	let resultLabel = $state<string | null>(null); // label shown above the entries panel: "prev entry", "results", etc.

	function reset_output() {
		error = null;
		header = null;
		entries = [];
		numDeleted = null;
		resultLabel = null;
	}

	function selectOp(next: Op) {
		op = next;
		reset_output();
	}

	async function exec() {
		if (!key.trim()) {
			error = 'key is required';
			return;
		}
		reset_output();
		loading = true;

		try {
			switch (op) {
				case 'put': {
					const res = await client.kvPut(key, value, {
						leaseId: leaseId || undefined
					});
					header = res.header;
					if (res.prev_entry) {
						entries = [res.prev_entry];
						resultLabel = 'previous entry';
					} else {
						resultLabel = null;
					}
					break;
				}
				case 'get': {
					const res = await client.kvGet(key, {
						revision: revision || undefined
					});
					header = res.header;
					entries = res.entries;
					resultLabel = revision > 0 ? `entry at revision ${revision}` : 'entry';
					break;
				}
				case 'range': {
					const res = await client.kvRange(key, {
						end: end || undefined,
						prefix,
						revision: revision || undefined
					});
					header = res.header;
					entries = res.entries;
					resultLabel = `${res.count} result${res.count !== 1 ? 's' : ''}${revision > 0 ? ` at revision ${revision}` : ''}`;
					break;
				}
				case 'delete': {
					const res = await client.kvDelete(key, {
						end: end || undefined,
						prevEntries: true
					});
					header = res.header;
					numDeleted = res.num_deleted;
					entries = res.prev_entries ?? [];
					resultLabel = `deleted ${res.num_deleted} key${res.num_deleted !== 1 ? 's' : ''}`;
					break;
				}
			}
		} catch (e) {
			if (e instanceof KaveError) {
				error = e.cause ? `${e.message} — ${e.cause}` : e.message;
			} else if (e instanceof Error) {
				error = e.message;
			} else {
				error = 'unknown error';
			}
		} finally {
			loading = false;
		}
	}

	function hex(n: number): string {
		return n === 0 ? '—' : `0x${n.toString(16).padStart(8, '0')}`;
	}
</script>

<section class="">
	<div class="" role="tablist">
		{#each ['put', 'get', 'range', 'delete'] as o (o)}
			<button role="tab" class="tab" class:active={op === o} onclick={() => selectOp(o as Op)}
				>{o.toUpperCase()}</button
			>
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
				<input id="val" bind:value placeholder="e.g. hello" spellcheck="false" />
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

		<button class="btn-exec" onclick={exec} disabled={loading}>
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
			<div class="mvcc-note">
				You are reading a snapshot at revision <strong>{revision}</strong>. The store is
				append-only, meaning new changes dont mutate previous state. Increment the revision to walk
				forward through history.
			</div>
		{/if}
	{/if}
</section>

<style>
	section {
		display: flex;
		flex-direction: column;
		gap: 0;
		font-family: var(--mono);
		background: var(--bg);
		color: var(--text);
		height: 100%;
	}

	.tabs {
		display: flex;
		border-bottom: 1px solid var(--border);
		background: var(--surface);
		flex-shrink: 0;
	}

	.tab {
		background: none;
		border: none;
		border-bottom: 2px solid transparent;
		color: var(--dim);
		font-family: var(--mono);
		font-size: 11px;
		font-weight: 600;
		letter-spacing: 0.1em;
		padding: 10px 18px;
		cursor: pointer;
		margin-bottom: -1px;
		transition:
			color 0.12s,
			border-color 0.12s;
	}
	.tab:hover {
		color: var(--text);
	}
	.tab.active {
		color: var(--accent);
		border-bottom-color: var(--accent);
	}

	.form {
		display: flex;
		flex-direction: column;
		gap: 12px;
		padding: 20px 24px;
		background: var(--surface);
		border-bottom: 1px solid var(--border);
	}

	.field {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}

	label {
		font-size: 11px;
		font-weight: 500;
		letter-spacing: 0.08em;
		color: var(--dim);
		text-transform: uppercase;
	}

	.hint {
		font-weight: 400;
		letter-spacing: 0;
		text-transform: none;
		font-size: 10px;
		color: var(--dim);
	}

	input[type='text'],
	input:not([type='checkbox']):not([type='number']),
	input[type='number'] {
		background: var(--bg);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		color: var(--text);
		font-family: var(--mono);
		font-size: 13px;
		padding: 8px 12px;
		outline: none;
		transition: border-color 0.15s;
		width: 100%;
		box-sizing: border-box;
	}
	input:focus {
		border-color: var(--accent);
	}

	.checkbox-label {
		display: flex;
		align-items: center;
		gap: 6px;
		font-size: 12px;
		color: var(--dim);
		cursor: pointer;
		text-transform: none;
		letter-spacing: 0;
	}

	.btn-exec {
		align-self: flex-start;
		background: var(--accent-dim);
		border: 1px solid var(--accent);
		border-radius: var(--radius);
		color: var(--accent);
		font-family: var(--mono);
		font-size: 11px;
		font-weight: 600;
		letter-spacing: 0.12em;
		padding: 9px 24px;
		cursor: pointer;
		transition:
			background 0.15s,
			color 0.15s;
		min-width: 100px;
	}
	.btn-exec:hover:not(:disabled) {
		background: var(--accent);
		color: #fff;
	}
	.btn-exec:disabled {
		opacity: 0.4;
		cursor: not-allowed;
	}

	.error-row {
		display: flex;
		align-items: center;
		gap: 8px;
		background: var(--error-bg);
		border: 1px solid var(--error-border);
		border-radius: var(--radius);
		color: var(--error);
		font-size: 12px;
		padding: 10px 14px;
		margin: 16px 24px 0;
	}
	.error-icon {
		font-size: 10px;
		font-weight: 700;
	}

	.header-strip {
		display: flex;
		flex-wrap: wrap;
		gap: 0;
		border-bottom: 1px solid var(--border);
		background: var(--surface);
		flex-shrink: 0;
	}

	.header-cell {
		display: flex;
		flex-direction: column;
		gap: 2px;
		padding: 8px 16px;
		border-right: 1px solid var(--border);
	}
	.header-cell:last-child {
		border-right: none;
	}

	.hdr-label {
		font-size: 9px;
		font-weight: 600;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--dim);
	}

	.hdr-val {
		font-size: 13px;
		font-weight: 500;
		color: var(--text);
	}

	.results-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		padding: 10px 24px;
		border-bottom: 1px solid var(--border);
		background: var(--bg);
		flex-shrink: 0;
	}

	.results-label {
		font-size: 10px;
		font-weight: 600;
		letter-spacing: 0.12em;
		text-transform: uppercase;
		color: var(--dim);
	}

	.empty {
		padding: 24px;
		font-size: 13px;
		color: var(--dim);
		font-style: italic;
	}

	.entry {
		border-bottom: 1px solid var(--border);
		padding: 12px 24px;
	}

	.entry-kv {
		display: flex;
		align-items: baseline;
		gap: 8px;
		margin-bottom: 8px;
		flex-wrap: wrap;
	}

	.entry-key {
		font-weight: 700;
		color: var(--text);
	}

	.entry-sep {
		color: var(--dim);
	}

	.entry-val {
		color: var(--text);
		word-break: break-all;
	}

	.entry-tombstone {
		color: var(--dim);
		font-style: italic;
	}

	.meta-grid {
		display: flex;
		flex-wrap: wrap;
		gap: 0;
		border: 1px solid var(--border);
		border-radius: var(--radius);
		overflow: hidden;
	}

	.meta-cell {
		display: flex;
		flex-direction: column;
		gap: 2px;
		padding: 6px 12px;
		border-right: 1px solid var(--border);
		background: var(--surface);
	}
	.meta-cell:last-child {
		border-right: none;
	}

	.meta-label {
		font-size: 9px;
		font-weight: 600;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--dim);
	}

	.meta-val {
		font-size: 12px;
		font-weight: 500;
		color: var(--text);
	}
	.meta-val.accent {
		color: var(--accent);
	}
	.meta-val.dim {
		color: var(--dim);
	}

	.mvcc-note {
		margin: 12px 24px;
		border-left: 3px solid var(--accent);
		background: var(--accent-dim);
		border-radius: 0 var(--radius) var(--radius) 0;
		padding: 10px 14px;
		font-size: 12px;
		color: var(--dim);
		line-height: 1.6;
	}
	.mvcc-note strong {
		color: var(--text);
	}

	.spinner {
		display: inline-block;
		width: 10px;
		height: 10px;
		border: 2px solid currentColor;
		border-top-color: transparent;
		border-radius: 50%;
		animation: spin 0.6s linear infinite;
	}
	@keyframes spin {
		to {
			transform: rotate(360deg);
		}
	}
</style>
