<script lang="ts">
	import type { KaveClient } from '$lib/kave_client';
	import type { KaveStats, KavePeer } from '$lib/kv';
	import { KaveError } from '$lib/kv';
	import { onDestroy } from 'svelte';

	let { client }: { client: KaveClient } = $props();

	let stats = $state<KaveStats | null>(null);
	let peers = $state<KavePeer[]>([]);
	let pollError = $state<string | null>(null);
	let polling = false;

	let killLoading = $state(false);
	let killError = $state<string | null>(null);
	let killSuccess = $state<string | null>(null);

	let probeKey = $state('cluster/probe');
	let probeValue = $state('ok');
	let probeLoading = $state(false);
	let probeResult = $state<{ rev: number; nodeId: string } | null>(null);
	let probeError = $state<string | null>(null);

	let compactTarget = $state('');
	let compactLoading = $state(false);
	let compactError = $state<string | null>(null);
	let compactSuccess = $state<string | null>(null);

	async function poll() {
		if (polling) return;
		polling = true;
		try {
			const s = await client.stats();
			stats = s;
			pollError = null;
			try {
				peers = JSON.parse(s.readable_configuration ?? '[]');
			} catch {
				peers = [];
			}
		} catch (e) {
			pollError = e instanceof KaveError ? e.message : 'cluster unreachable';
		} finally {
			polling = false;
		}
	}

	const interval = setInterval(poll, 2000);
	poll();
	onDestroy(() => clearInterval(interval));

	async function killLeader() {
		if (!stats) return;
		killError = null;
		killSuccess = null;
		killLoading = true;
		probeResult = null;
		probeError = null;
		try {
			const res = await client.killNode(stats.leader_id);
			if (res.success) {
				killSuccess = `node ${stats.leader_id} killed, waiting for re-election`;
			}
		} catch (e) {
			killError = e instanceof KaveError ? (e.cause ?? e.message) : String(e);
		} finally {
			killLoading = false;
		}
	}

	async function writeProbe() {
		probeError = null;
		probeResult = null;
		probeLoading = true;
		try {
			const res = await client.kvPut(probeKey, probeValue);
			probeResult = { rev: res.header.revision, nodeId: res.header.node_id };
		} catch (e) {
			probeError = e instanceof KaveError ? (e.cause ?? e.message) : String(e);
		} finally {
			probeLoading = false;
		}
	}

	async function compact() {
		const rev = parseInt(compactTarget);
		if (isNaN(rev) || rev < 1) {
			compactError = 'enter a valid revision number';
			return;
		}
		compactError = null;
		compactSuccess = null;
		compactLoading = true;
		try {
			const res = await client.compact(rev);
			if (res.success) {
				compactSuccess = `compacted to revision ${rev}`;
			}
		} catch (e) {
			compactError = e instanceof KaveError ? (e.cause ?? e.message) : String(e);
		} finally {
			compactLoading = false;
		}
	}

	function setCompactToCurrent() {
		if (stats) compactTarget = String(stats.revision);
	}

	function peerRole(peerId: string): string {
		if (!stats) return 'unknown';
		if (peerId === stats.leader_id) return 'leader';
		return stats.state === 'Candidate' ? 'candidate' : 'follower';
	}
</script>

<section class="cluster">

	<div class="panel">
		<div class="panel-header">
			<span class="panel-title">CLUSTER STATE</span>
			{#if pollError}
				<span class="badge badge-err">UNREACHABLE</span>
			{:else if stats}
				<span class="badge badge-ok">LIVE</span>
			{/if}
		</div>

		{#if stats}
			<!-- global metrics row -->
			<div class="metrics-row">
				<div class="metric">
					<span class="metric-label">term</span>
					<span class="metric-val">{stats.term}</span>
				</div>
				<div class="metric">
					<span class="metric-label">commit index</span>
					<span class="metric-val">{stats.commit_index}</span>
				</div>
				<div class="metric">
					<span class="metric-label">applied index</span>
					<span class="metric-val">{stats.applied_index}</span>
				</div>
				<div class="metric">
					<span class="metric-label">revision</span>
					<span class="metric-val accent">{stats.revision}</span>
				</div>
				<div class="metric">
					<span class="metric-label">compacted rev</span>
					<span class="metric-val dim">{stats.compacted_revision}</span>
				</div>
			</div>

			<div class="nodes-grid">
				{#each peers as peer}
					{@const role = peerRole(peer.id)}
					<div class="node-chip" class:node-leader={role === 'leader'} class:node-follower={role === 'follower'} class:node-candidate={role === 'candidate'}>
						<div class="node-chip-header">
							<span class="node-id">{peer.id}</span>
							<span class="node-role-badge role-{role}">{role.toUpperCase()}</span>
						</div>
						<div class="node-addr">ADDR:&nbsp;{peer.address}</div>
						<div class="node-suffrage">SUFFRAGE:&nbsp;{peer.suffrage}</div>
					</div>
				{/each}

				{#if peers.length === 0 && stats}
					<div class="node-chip node-self node-leader">
						<div class="node-chip-header">
							<span class="node-id">{stats.leader_id || 'unknown'}</span>
							<span class="node-role-badge role-leader">LEADER</span>
						</div>
						<div class="node-addr">{stats.leader_addr}</div>
					</div>
				{/if}
			</div>
		{:else if pollError}
			<div class="err-row">
				<span class="err-icon">✕</span>
				<span>{pollError}</span>
			</div>
		{:else}
			<div class="loading-row">
				<span class="spinner"></span>
				<span class="dim-text">connecting…</span>
			</div>
		{/if}
	</div>

	<div class="panel">
		<div class="panel-header">
			<span class="panel-title">FAULT TOLERANCE</span>
			<span class="panel-hint">kill the leader, the cluster re-elects and stays writable</span>
		</div>

		<div class="kill-zone">
			<div class="kill-info">
				{#if stats?.leader_id}
					<span class="kill-label">current leader</span>
					<span class="kill-target">{stats.leader_id}</span>
					<span class="kill-addr dim-text">{stats.leader_addr}</span>
				{:else}
					<span class="dim-text">no leader detected</span>
				{/if}
			</div>

			<button
				class="btn-kill"
				onclick={killLeader}
				disabled={killLoading || !stats?.leader_id}
			>
				{#if killLoading}
					<span class="spinner"></span>
				{:else}KILL LEADER{/if}
			</button>
		</div>

		{#if killSuccess}
			<div class="success-row">
				<span class="success-icon">✓</span>
				<span>{killSuccess}</span>
			</div>
		{/if}
		{#if killError}
			<div class="err-row">
				<span class="err-icon">✕</span>
				<span>{killError}</span>
			</div>
		{/if}

		<div class="probe-zone">
			<div class="probe-header">
				<span class="meta-label">WRITE PROBE</span>
				<span class="probe-hint dim-text">verify writes succeed after re-election</span>
			</div>
			<div class="probe-row">
				<input
					class="probe-input"
					placeholder="key"
					bind:value={probeKey}
					spellcheck="false"
				/>
				<input
					class="probe-input"
					placeholder="value"
					bind:value={probeValue}
					spellcheck="false"
				/>
				<button class="btn-probe" onclick={writeProbe} disabled={probeLoading}>
					{#if probeLoading}<span class="spinner"></span>{:else}PUT{/if}
				</button>
			</div>

			{#if probeResult}
				<div class="probe-result">
					<span class="probe-ok">written</span>
					<span class="probe-detail">rev <strong>{probeResult.rev}</strong> on <strong>{probeResult.nodeId}</strong></span>
				</div>
			{/if}
			{#if probeError}
				<div class="err-row">
					<span class="err-icon">✕</span>
					<span>{probeError}</span>
				</div>
			{/if}
		</div>
	</div>

	<div class="panel">
		<div class="panel-header">
			<span class="panel-title">COMPACTION</span>
			<span class="panel-hint">
				discard history below a revision, older reads become <code>ErrCompacted</code>
			</span>
		</div>

		<div class="compact-form">
			<div class="compact-rev-ctx">
				<span class="meta-label">current revision</span>
				<span class="compact-current-rev">{stats?.revision ?? '—'}</span>
				{#if stats}
					<button class="btn-mini" onclick={setCompactToCurrent}>use current</button>
				{/if}
			</div>

			<div class="compact-input-row">
				<div class="field">
					<label for="compact-rev">compact to revision</label>
					<input
						id="compact-rev"
						type="number"
						min="1"
						placeholder="e.g. {stats ? stats.revision - 5 : 10}"
						bind:value={compactTarget}
					/>
				</div>
				<button
					class="btn-compact"
					onclick={compact}
					disabled={compactLoading || !compactTarget}
				>
					{#if compactLoading}
						<span class="spinner"></span>
					{:else}
						COMPACT
					{/if}
				</button>
			</div>

			{#if compactSuccess}
				<div class="success-row">
					<span class="success-icon">✓</span>
					<span>{compactSuccess}</span>
				</div>
			{/if}
			{#if compactError}
				<div class="err-row">
					<span class="err-icon">✕</span>
					<span>{compactError}</span>
				</div>
			{/if}
		</div>

		<div class="explainer">
			<span class="explainer-icon">ℹ</span>
			<span>
				Compaction is replicated through Raft so every node applies it atomically. After compaction
				any range query with <code>revision &lt; compacted_rev</code> returns <code>ErrCompacted</code>.
				The latest state is always preserved.
			</span>
		</div>
	</div>

</section>

<style>
	.cluster {
		display: flex;
		flex-direction: column;
		gap: 20px;
		font-family: var(--mono);
		background: var(--bg);
		padding: 24px;
		color: var(--text);
	}

	.panel {
		border: 1px solid var(--border);
		border-radius: var(--radius);
		overflow: hidden;
		display: flex;
		flex-direction: column;
		gap: 0;
	}

	.panel-header {
		display: flex;
		align-items: center;
		gap: 10px;
		padding: 10px 16px;
		background: var(--surface);
		border-bottom: 1px solid var(--border);
	}

	.panel-title {
		font-size: 10px;
		font-weight: 700;
		letter-spacing: 0.14em;
		color: var(--dim);
		flex-shrink: 0;
	}

	.panel-hint {
		font-size: 10px;
		color: var(--dim);
		font-weight: 400;
	}

	.metrics-row {
		display: flex;
		flex-wrap: wrap;
		border-bottom: 1px solid var(--border);
	}

	.metric {
		display: flex;
		flex-direction: column;
		gap: 3px;
		padding: 10px 16px;
		border-right: 1px solid var(--border);
	}
	.metric:last-child { border-right: none; }

	.metric-label {
		font-size: 9px;
		font-weight: 600;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--dim);
	}

	.metric-val {
		font-size: 18px;
		font-weight: 700;
		color: var(--text);
		line-height: 1;
	}
	.metric-val.accent { color: var(--accent); }
	.metric-val.dim    { color: var(--dim); }
	
	.nodes-grid {
		display: flex;
		flex-wrap: wrap;
		gap: 12px;
		padding: 16px;
		background: var(--bg);
	}

	.node-chip {
		border: 1px solid var(--border);
		border-radius: var(--radius);
		padding: 10px 14px;
		display: flex;
		flex-direction: column;
		gap: 4px;
		min-width: 160px;
		transition: border-color 0.2s;
	}

	.node-leader {
		border-color: var(--accent);
		background: var(--accent-dim);
	}

	.node-chip-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 8px;
	}

	.node-id {
		font-size: 13px;
		font-weight: 700;
		color: var(--text);
	}

	.node-role-badge {
		font-size: 8px;
		font-weight: 700;
		letter-spacing: 0.1em;
		padding: 2px 6px;
		border-radius: 2px;
	}

	.role-leader {
		background: var(--accent);
		color: #fff;
	}
	.role-follower {
		background: var(--surface);
		color: var(--dim);
		border: 1px solid var(--border);
	}
	.role-candidate {
		background: #fff3cd;
		color: #856404;
		border: 1px solid #ffc107;
	}

	.node-addr, .node-suffrage {
		font-size: 10px;
		color: var(--dim);
		word-break: break-all;
	}

	.kill-zone {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 16px;
		padding: 14px 16px;
		border-bottom: 1px solid var(--border);
	}

	.kill-info {
		display: flex;
		align-items: baseline;
		gap: 10px;
		flex-wrap: wrap;
	}

	.kill-label {
		font-size: 9px;
		font-weight: 600;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--dim);
	}

	.kill-target {
		font-size: 14px;
		font-weight: 700;
		color: var(--text);
	}

	.kill-addr {
		font-size: 11px;
	}

	.btn-kill {
		background: var(--error-bg);
		border: 1px solid var(--error-border);
		border-radius: var(--radius);
		color: var(--error);
		font-family: var(--mono);
		font-size: 11px;
		font-weight: 700;
		letter-spacing: 0.12em;
		padding: 10px 20px;
		cursor: pointer;
		transition: background 0.15s;
		white-space: nowrap;
		flex-shrink: 0;
	}
	.btn-kill:hover:not(:disabled) {
		background: #ffe4e6;
	}
	.btn-kill:disabled {
		opacity: 0.4;
		cursor: not-allowed;
	}

	.probe-zone {
		padding: 14px 16px;
		display: flex;
		flex-direction: column;
		gap: 10px;
	}

	.probe-header {
		display: flex;
		align-items: center;
		gap: 10px;
	}

	.probe-hint {
		font-size: 10px;
	}

	.probe-row {
		display: flex;
		gap: 8px;
		align-items: center;
	}

	.probe-input {
		background: var(--surface);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		color: var(--text);
		font-family: var(--mono);
		font-size: 12px;
		padding: 7px 10px;
		outline: none;
		transition: border-color 0.15s;
		flex: 1;
		min-width: 0;
	}
	.probe-input:focus { border-color: var(--accent); }

	.probe-result {
		display: flex;
		align-items: center;
		gap: 10px;
		font-size: 12px;
	}

	.probe-ok {
		color: var(--success);
		font-weight: 700;
	}

	.probe-detail {
		color: var(--dim);
	}

	.compact-form {
		padding: 14px 16px;
		display: flex;
		flex-direction: column;
		gap: 12px;
	}

	.compact-rev-ctx {
		display: flex;
		align-items: center;
		gap: 10px;
	}

	.compact-current-rev {
		font-size: 18px;
		font-weight: 700;
		color: var(--accent);
		line-height: 1;
	}

	.compact-input-row {
		display: flex;
		gap: 10px;
		align-items: flex-end;
	}

	.field {
		display: flex;
		flex-direction: column;
		gap: 4px;
		flex: 1;
	}

	label {
		font-size: 10px;
		font-weight: 600;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--dim);
	}

	input[type='number'],
	input[type='text'],
	input:not([type='checkbox']):not([type='number']):not([type='text']) {
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
	input:focus { border-color: var(--accent); }

	.btn-probe,
	.btn-compact {
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
		transition: background 0.15s, color 0.15s;
		white-space: nowrap;
		flex-shrink: 0;
	}
	.btn-probe:hover:not(:disabled),
	.btn-compact:hover:not(:disabled) {
		background: var(--accent);
		color: #fff;
	}
	.btn-probe:disabled,
	.btn-compact:disabled {
		opacity: 0.4;
		cursor: not-allowed;
	}

	.btn-mini {
		background: none;
		border: 1px solid var(--border);
		border-radius: var(--radius);
		color: var(--dim);
		font-family: var(--mono);
		font-size: 10px;
		padding: 2px 8px;
		cursor: pointer;
		transition: border-color 0.1s, color 0.1s;
	}
	.btn-mini:hover {
		border-color: var(--accent);
		color: var(--accent);
	}

	.badge {
		font-size: 9px;
		font-weight: 700;
		letter-spacing: 0.1em;
		padding: 2px 7px;
		border-radius: 2px;
	}
	.badge-ok  { background: rgba(55,189,141,0.15); color: #166534; border: 1px solid var(--success); }
	.badge-err { background: var(--error-bg); color: var(--error); border: 1px solid var(--error-border); }

	.success-row {
		display: flex;
		align-items: center;
		gap: 8px;
		background: rgba(55,189,141,0.08);
		border: 1px solid var(--success);
		border-radius: var(--radius);
		color: #166534;
		font-size: 12px;
		padding: 10px 14px;
		margin: 0 16px 14px;
	}
	.success-icon { font-size: 11px; font-weight: 700; }

	.err-row {
		display: flex;
		align-items: center;
		gap: 8px;
		background: var(--error-bg);
		border: 1px solid var(--error-border);
		border-radius: var(--radius);
		color: var(--error);
		font-size: 12px;
		padding: 10px 14px;
		margin: 0 16px 14px;
	}
	.err-icon { font-size: 10px; font-weight: 700; }

	.loading-row {
		display: flex;
		align-items: center;
		gap: 10px;
		padding: 16px;
	}

	.explainer {
		border-left: 3px solid var(--accent);
		background: var(--accent-dim);
		border-radius: 0 var(--radius) var(--radius) 0;
		padding: 10px 14px;
		font-size: 12px;
		color: var(--dim);
		line-height: 1.6;
		display: flex;
		gap: 8px;
		align-items: flex-start;
		margin: 0 16px 14px;
	}
	.explainer-icon { color: var(--accent); flex-shrink: 0; }

	code {
		background: var(--surface);
		border: 1px solid var(--border);
		border-radius: 3px;
		padding: 1px 5px;
		font-family: var(--mono);
		font-size: 11px;
	}

	.meta-label {
		font-size: 9px;
		font-weight: 600;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--dim);
	}

	.dim-text { color: var(--dim); }

	.spinner {
		display: inline-block;
		width: 10px;
		height: 10px;
		border: 2px solid currentColor;
		border-top-color: transparent;
		border-radius: 50%;
		animation: spin 0.6s linear infinite;
	}
	@keyframes spin { to { transform: rotate(360deg); } }
</style>
