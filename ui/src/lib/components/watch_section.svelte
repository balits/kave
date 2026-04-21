<script lang="ts">
	import type { KaveClient } from '$lib/kave_client';
	import { WatchClient, type WatchEvent } from '$lib/watch';
	import { KaveError } from '$lib/kv';
	import { onDestroy } from 'svelte';

	let { client }: { client: KaveClient } = $props();

	// watch subscription inputs
	let watchKey = $state('');
	let watchPrefix = $state(false);
	let watchStartRev = $state(0);

	// write panel inputs
	let writeKey = $state('');
	let writeValue = $state('');
	let writeOp = $state<'put' | 'delete'>('put');

	// state
	let watchClient = $state<WatchClient | null>(null);
	let activeWatchId = $state<number | null>(null);
	let events = $state<WatchEvent[]>([]);
	let subError = $state<string | null>(null);
	let writeError = $state<string | null>(null);
	let subLoading = $state(false);
	let writeLoading = $state(false);

	onDestroy(() => watchClient?.close());

	async function subscribe() {
		if (!watchKey.trim()) {
			subError = 'key is required';
			return;
		}
		subError = null;
		subLoading = true;
		try {
			// close any existing session
			if (watchClient) {
				watchClient.close();
				watchClient = null;
				activeWatchId = null;
			}
			events = [];

			const wc = new WatchClient(client.baseURL, {
				onError: (msg) => {
					subError = msg;
				},
				onClose: () => {
					activeWatchId = null;
				}
			});
			await wc.ready();

			const res = await wc.watch(
				{
					key: watchKey,
					prefix: watchPrefix,
					startRevision: watchStartRev || undefined
				},
				(ev: WatchEvent) => {
					events = [ev, ...events];
				}
			);

			if (!res.success) {
				subError = res.error ?? 'watch creation failed';
				wc.close();
				return;
			}
			watchClient = wc;
			activeWatchId = res.watchId;
		} catch (e) {
			subError = e instanceof Error ? e.message : 'unknown error';
		} finally {
			subLoading = false;
		}
	}

	function unsubscribe() {
		watchClient?.close();
		watchClient = null;
		activeWatchId = null;
		subError = null;
	}

	async function write() {
		if (!writeKey.trim()) {
			writeError = 'key is required';
			return;
		}
		writeError = null;
		writeLoading = true;
		try {
			if (writeOp === 'put') {
				await client.kvPut(writeKey, writeValue);
			} else {
				await client.kvDelete(writeKey);
			}
		} catch (e) {
			if (e instanceof KaveError) writeError = e.cause ? `${e.message} — ${e.cause}` : e.message;
			else if (e instanceof Error) writeError = e.message;
			else writeError = 'unknown error';
		} finally {
			writeLoading = false;
		}
	}

	function clearEvents() {
		events = [];
	}
</script>

<section class="watch">
	<!-- subscription bar -->
	<div class="sub-bar">
		<div class="sub-inputs">
			<div class="field grow">
				<label for="wkey">watch key</label>
				<input
					id="wkey"
					bind:value={watchKey}
					placeholder="e.g. myapp/config"
					spellcheck="false"
					disabled={activeWatchId !== null}
				/>
			</div>

			<div class="field narrow">
				<label for="wrev">start revision <span class="hint">(0 = live only)</span></label>
				<input
					id="wrev"
					type="number"
					min="0"
					bind:value={watchStartRev}
					disabled={activeWatchId !== null}
				/>
			</div>

			<label class="checkbox-label" class:disabled={activeWatchId !== null}>
				<input type="checkbox" bind:checked={watchPrefix} disabled={activeWatchId !== null} />
				prefix
			</label>
		</div>

		<div class="sub-actions">
			{#if activeWatchId === null}
				<button class="btn-primary" onclick={subscribe} disabled={subLoading}>
					{#if subLoading}<span class="spinner"></span>{:else}SUBSCRIBE{/if}
				</button>
			{:else}
				<div class="active-badge">
					<span class="dot"></span>
					<span>watcher #{activeWatchId}</span>
				</div>
				<button class="btn-unsub" onclick={unsubscribe}>STOP</button>
			{/if}
		</div>
	</div>

	{#if subError}
		<div class="error-row"><span class="error-icon">✕</span><span>{subError}</span></div>
	{/if}

	<!--write panel + event log -->
	<div class="body">
		<!-- left: write panel -->
		<div class="write-panel">
			<div class="panel-title">WRITE</div>

			<div class="tab-row">
				{#each ['put', 'delete'] as o (o)}
					<button
						class="tab"
						class:active={writeOp === o}
						onclick={() => (writeOp = o as 'put' | 'delete')}
					>
						{o.toUpperCase()}
					</button>
				{/each}
			</div>

			<div class="write-form">
				<div class="field">
					<label for="wfkey">key</label>
					<input
						id="wfkey"
						bind:value={writeKey}
						placeholder="e.g. myapp/config"
						spellcheck="false"
					/>
				</div>

				{#if writeOp === 'put'}
					<div class="field">
						<label for="wfval">value</label>
						<input id="wfval" bind:value={writeValue} placeholder="e.g. hello" spellcheck="false" />
					</div>
				{/if}

				<button class="btn-write" onclick={write} disabled={writeLoading}>
					{#if writeLoading}<span class="spinner"></span>{:else}{writeOp.toUpperCase()}{/if}
				</button>

				{#if writeError}
					<div class="error-row small">
						<span class="error-icon">✕</span><span>{writeError}</span>
					</div>
				{/if}
			</div>

			<!-- explainer -->
			<div class="explainer">
				<span class="explainer-icon">⊙</span>
				<span>
					Writes here fire against the cluster immediately. If a watch is active on the same key,
					you'll see the event appear in real time on the right.
				</span>
			</div>
		</div>

		<!-- right: event log -->
		<div class="event-log">
			<div class="log-header">
				<span class="panel-title">EVENTS</span>
				{#if events.length > 0}
					<button class="btn-clear" onclick={clearEvents}>CLEAR</button>
				{/if}
			</div>

			{#if activeWatchId === null && events.length === 0}
				<div class="log-empty">subscribe to a key to start receiving events</div>
			{:else if events.length === 0}
				<div class="log-empty">
					waiting for events on <strong>{watchKey}{watchPrefix ? '*' : ''}</strong>…
				</div>
			{:else}
				<div class="log-scroll">
					{#each events as ev (ev.entry.mod_revision + ':' + ev.watcherId)}
						<div
							class="event-row"
							class:ev-put={ev.kind === 'PUT'}
							class:ev-delete={ev.kind === 'DELETE'}
						>
							<span class="ev-kind">{ev.kind}</span>
							<span class="ev-key">{ev.entry.key}</span>
							{#if ev.kind === 'PUT'}
								<span class="ev-sep">→</span>
								<span class="ev-val">{ev.entry.value}</span>
							{/if}
							<div class="ev-meta">
								<span>rev <strong>{ev.entry.mod_revision}</strong></span>
								<span>v<strong>{ev.entry.version}</strong></span>
								{#if ev.entry.lease_id}
									<span>lease <strong>{ev.entry.lease_id}</strong></span>
								{/if}
							</div>
						</div>
					{/each}
				</div>
			{/if}
		</div>
	</div>
</section>

<style>
	@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500&display=swap');

	:root {
		--bg: #0c0c0e;
		--surface: #141416;
		--border: #222226;
		--accent: #f0a500;
		--accent-dim: #f0a50033;
		--text: #e8e8e8;
		--dim: #555;
		--error: #e05555;
		--mono: 'IBM Plex Mono', monospace;
		--sans: 'IBM Plex Sans', system-ui, sans-serif;
		--radius: 4px;
	}

	.watch {
		display: flex;
		flex-direction: column;
		height: 100%;
		font-family: var(--mono);
		background: var(--bg);
		color: var(--text);
	}

	/* subscription bar */
	.sub-bar {
		display: flex;
		align-items: flex-end;
		gap: 12px;
		padding: 16px 24px;
		border-bottom: 1px solid var(--border);
		flex-wrap: wrap;
	}

	.sub-inputs {
		display: flex;
		align-items: flex-end;
		gap: 10px;
		flex: 1;
		flex-wrap: wrap;
	}

	.sub-actions {
		display: flex;
		align-items: center;
		gap: 10px;
		flex-shrink: 0;
	}

	.field {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}
	.field.grow {
		flex: 1;
		min-width: 180px;
	}
	.field.narrow {
		width: 140px;
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
		color: #444;
	}

	input[type='text'],
	input:not([type='checkbox']):not([type='number']),
	input[type='number'] {
		background: var(--surface);
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
	input:disabled {
		opacity: 0.4;
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
		padding-bottom: 10px;
	}
	.checkbox-label.disabled {
		opacity: 0.4;
		pointer-events: none;
	}

	/* buttons */
	.btn-primary {
		background: var(--accent-dim);
		border: 1px solid var(--accent);
		border-radius: var(--radius);
		color: var(--accent);
		font-family: var(--mono);
		font-size: 11px;
		font-weight: 600;
		letter-spacing: 0.12em;
		padding: 9px 20px;
		cursor: pointer;
		transition:
			background 0.15s,
			color 0.15s;
		white-space: nowrap;
	}
	.btn-primary:hover:not(:disabled) {
		background: var(--accent);
		color: var(--bg);
	}
	.btn-primary:disabled {
		opacity: 0.4;
		cursor: not-allowed;
	}

	.btn-unsub {
		background: #1a0a0a;
		border: 1px solid #5a2020;
		border-radius: var(--radius);
		color: var(--error);
		font-family: var(--mono);
		font-size: 11px;
		font-weight: 600;
		letter-spacing: 0.1em;
		padding: 8px 16px;
		cursor: pointer;
	}
	.btn-unsub:hover {
		background: #2a1010;
	}

	.btn-write {
		align-self: flex-start;
		background: var(--surface);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		color: var(--text);
		font-family: var(--mono);
		font-size: 11px;
		font-weight: 600;
		letter-spacing: 0.1em;
		padding: 8px 20px;
		cursor: pointer;
		transition: border-color 0.15s;
	}
	.btn-write:hover:not(:disabled) {
		border-color: var(--accent);
		color: var(--accent);
	}
	.btn-write:disabled {
		opacity: 0.4;
		cursor: not-allowed;
	}

	.btn-clear {
		background: none;
		border: none;
		color: var(--dim);
		font-family: var(--mono);
		font-size: 10px;
		letter-spacing: 0.1em;
		cursor: pointer;
		padding: 0;
	}
	.btn-clear:hover {
		color: var(--text);
	}

	/* active badge */
	.active-badge {
		display: flex;
		align-items: center;
		gap: 6px;
		font-size: 11px;
		color: #4caf60;
		border: 1px solid #3a5a3a;
		background: #0e1a0e;
		border-radius: var(--radius);
		padding: 6px 10px;
	}
	.dot {
		width: 6px;
		height: 6px;
		background: #4caf60;
		border-radius: 50%;
		box-shadow: 0 0 4px #4caf60aa;
		animation: pulse 2s ease-in-out infinite;
	}
	@keyframes pulse {
		0%,
		100% {
			opacity: 1;
		}
		50% {
			opacity: 0.3;
		}
	}

	/* error */
	.error-row {
		display: flex;
		align-items: center;
		gap: 8px;
		background: #2a1414;
		border: 1px solid #5c2020;
		border-radius: var(--radius);
		color: var(--error);
		font-size: 12px;
		padding: 8px 14px;
		margin: 0 24px;
	}
	.error-row.small {
		margin: 0;
		font-size: 11px;
	}
	.error-icon {
		font-size: 10px;
		font-weight: 700;
	}

	/* body split */
	.body {
		display: grid;
		grid-template-columns: 320px 1fr;
		flex: 1;
		overflow: hidden;
		border-top: 1px solid var(--border);
	}

	/* write panel */
	.write-panel {
		border-right: 1px solid var(--border);
		display: flex;
		flex-direction: column;
		gap: 16px;
		padding: 16px;
		overflow-y: auto;
	}

	.panel-title {
		font-size: 10px;
		font-weight: 600;
		letter-spacing: 0.14em;
		color: var(--dim);
	}

	.tab-row {
		display: flex;
		border-bottom: 1px solid var(--border);
		margin-bottom: 4px;
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
		padding: 6px 14px;
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

	.write-form {
		display: flex;
		flex-direction: column;
		gap: 10px;
	}

	.explainer {
		border-left: 3px solid var(--accent);
		background: var(--accent-dim);
		border-radius: 0 var(--radius) var(--radius) 0;
		padding: 10px 12px;
		font-family: var(--sans);
		font-size: 11px;
		color: #aaa;
		line-height: 1.6;
		display: flex;
		gap: 8px;
		align-items: flex-start;
		margin-top: auto;
	}
	.explainer-icon {
		color: var(--accent);
		flex-shrink: 0;
		margin-top: 1px;
	}

	/* event log */
	.event-log {
		display: flex;
		flex-direction: column;
		overflow: hidden;
	}

	.log-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		padding: 10px 16px;
		border-bottom: 1px solid var(--border);
		flex-shrink: 0;
	}

	.log-empty {
		padding: 24px 16px;
		font-size: 12px;
		color: var(--dim);
		font-style: italic;
	}
	.log-empty strong {
		color: var(--text);
		font-style: normal;
	}

	.log-scroll {
		flex: 1;
		overflow-y: auto;
		display: flex;
		flex-direction: column;
	}

	/* event rows */
	.event-row {
		display: flex;
		align-items: baseline;
		gap: 8px;
		padding: 8px 16px;
		border-bottom: 1px solid var(--border);
		font-size: 12px;
		flex-wrap: wrap;
	}
	.event-row:first-child {
		animation: flash 0.4s ease-out;
	}

	@keyframes flash {
		from {
			background: #1a1a10;
		}
		to {
			background: transparent;
		}
	}

	.ev-kind {
		font-size: 10px;
		font-weight: 700;
		letter-spacing: 0.1em;
		padding: 1px 6px;
		border-radius: 2px;
		flex-shrink: 0;
	}
	.ev-put .ev-kind {
		background: #0e1a0e;
		color: #4caf60;
		border: 1px solid #3a5a3a;
	}
	.ev-delete .ev-kind {
		background: #1a0a0a;
		color: var(--error);
		border: 1px solid #5a2020;
	}

	.ev-key {
		font-weight: 600;
	}
	.ev-sep {
		color: var(--dim);
	}
	.ev-val {
		color: #a0d4a0;
		word-break: break-all;
	}

	.ev-meta {
		display: flex;
		gap: 10px;
		font-size: 10px;
		color: var(--dim);
		margin-left: auto;
		flex-shrink: 0;
	}
	.ev-meta strong {
		color: var(--text);
	}

	/* spinner */
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
