<script lang="ts">
	import type { KaveClient } from '$lib/kave_client';
	import type { LeaseGrantResponse, LeaseLookupResponse } from '$lib/kv';
	import { KaveError } from '$lib/kv';
	import { onDestroy } from 'svelte';

	let { client }: { client: KaveClient } = $props();

	let grantTtlInput = $state(15);
	let grantAttachKey = $state('');
	let grantAttachValue = $state('');

	let activeLease = $state<LeaseGrantResponse | null>(null);
	let activeLiveLookup = $state<LeaseLookupResponse | null>(null);
	let activeRemainingSec = $state(0);
	let activeExpired = $state(false);
	let activeAttachedKeyGone = $state(false);

	let loading = $state<string | null>(null); // which action is loading
	let error = $state<string | null>(null);
	let lookupError = $state<string | null>(null);

	let countdownInterval: ReturnType<typeof setInterval> | null = null;
	let lookupInterval: ReturnType<typeof setInterval> | null = null;

	function clearIntervals() {
		if (countdownInterval) {
			clearInterval(countdownInterval);
			countdownInterval = null;
		}
		if (lookupInterval) {
			clearInterval(lookupInterval);
			lookupInterval = null;
		}
	}

	onDestroy(clearIntervals);

	function startCountdown(ttl: number) {
		clearIntervals();
		activeRemainingSec = ttl;
		activeExpired = false;

		countdownInterval = setInterval(() => {
			activeRemainingSec = Math.max(0, activeRemainingSec - 1);
			if (activeRemainingSec === 0) {
				activeExpired = true;
				clearIntervals();
			}
		}, 1000);

		lookupInterval = setInterval(async () => {
			if (!activeLease) return;
			try {
				activeLiveLookup = await client.leaseLookup(activeLease.id);
				activeRemainingSec = activeLiveLookup.remaining_ttl;
				if (activeRemainingSec <= 0) {
					activeExpired = true;
					clearIntervals();
				}
			} catch (e) {
				if (e instanceof KaveError) {
					// 404 server confirms lease is gone
					if (e.status === 404) {
						markExpired();
					} else {
						// transient error so dont mark expired, just show
						lookupError = e.cause ?? e.message;
					}
				}
			}
		}, 2000);
	}

	async function markExpired() {
		clearIntervals();
		activeExpired = true;
		activeRemainingSec = 0;
 
		if (grantAttachKey.trim()) {
			try {
				const res = await client.kvGet(grantAttachKey);
				activeAttachedKeyGone = res.entries.length === 0;
			} catch {
				activeAttachedKeyGone = true; // assume gone on error?
			}
		}
	}

	function catchErr(e: unknown): string {
		if (e instanceof KaveError) return e.cause ? `${e.message} — ${e.cause}` : e.message;
		if (e instanceof Error) return e.message;
		return 'unknown error';
	}

	async function grant() {
		error = null;
		loading = 'grant';
		try {
			clearIntervals();
			activeLease = await client.leaseGrant(grantTtlInput);
			activeLiveLookup = null;
			activeExpired = false;
			activeAttachedKeyGone = false;
			startCountdown(activeLease.ttl);

			// attach a key if provided
			if (grantAttachKey.trim()) {
				await client.kvPut(grantAttachKey, grantAttachValue || grantAttachKey, {
					leaseID: activeLease.id // TODO: plz dont send ID as number thats larger than 8 quintillion :D
				});
			}
		} catch (e) {
			error = catchErr(e);
			activeLease = null;
		} finally {
			loading = null;
		}
	}

	async function keepAlive() {
		if (!activeLease) return;
		error = null;
		loading = 'keepalive';
		try {
			const res = await client.leaseKeepAlive(activeLease.id);
			startCountdown(res.ttl);
		} catch (e) {
			error = catchErr(e);
		} finally {
			loading = null;
		}
	}

	async function revoke() {
		if (!activeLease) return;
		error = null;
		loading = 'revoke';
		try {
			await client.leaseRevoke(activeLease.id);
			clearIntervals();
			await markExpired()
		} catch (e) {
			error = catchErr(e);
		} finally {
			loading = null;
		}
	}

	async function doLookup() {
		if (!activeLease) return;
		loading = 'lookup';
		lookupError = null;
		try {
			activeLiveLookup = await client.leaseLookup(activeLease.id);
		} catch (e) {
			lookupError = catchErr(e);
		} finally {
			loading = null;
		}
	}

	function reset() {
		clearIntervals();
		activeLease = null;
		activeLiveLookup = null;
		activeExpired = false;
		activeAttachedKeyGone = false;
		activeRemainingSec = 0;
		error = null;
		lookupError = null;
	}

	const barWidth = $derived(
		activeLease ? Math.max(0, (activeRemainingSec / activeLease.ttl) * 100) : 0
	);

	const barColor = $derived(barWidth > 50 ? '#4caf60' : barWidth > 20 ? '#f0a500' : '#e05555');
</script>

<section class="lease">
	{#if !activeLease}
		<div class="form">
			<div class="field narrow">
				<label for="ttl">ttl <span class="hint">(seconds: min 15 on server)</span></label>
				<input id="ttl" type="number" min="1" max="3600" bind:value={grantTtlInput} />
			</div>

			<div class="field">
				<label for="akey">
					attach key <span class="hint">(optional: puts a key tied to this lease)</span>
				</label>
				<input
					id="akey"
					bind:value={grantAttachKey}
					placeholder="session/user-42"
					spellcheck="false"
				/>
			</div>

			{#if grantAttachKey.trim()}
				<div class="field">
					<label for="aval">value</label>
					<input id="aval" bind:value={grantAttachValue} placeholder="active" spellcheck="false" />
				</div>
			{/if}

			<button class="btn-primary" onclick={grant} disabled={loading !== null}>
				{#if loading === 'grant'}<span class="spinner"></span>{:else}GRANT LEASE{/if}
			</button>
		</div>
	{:else}
		<div class="lease-card" class:expired={activeExpired}>
			<div class="lease-header">
				<div class="lease-id-block">
					<span class="meta-label">lease id</span>
					<span class="lease-id">{activeLease.id}</span>
				</div>
				<div class="lease-ttl-block">
					<span class="meta-label">original ttl</span>
					<span class="lease-ttl-val">{activeLease.ttl}s</span>
				</div>
				<div class="lease-meta-cell">
					<span class="meta-label">status</span>
					<span class="lease-status" class:status-alive={!activeExpired} class:status-expired={activeExpired}>
						{activeExpired ? 'EXPIRED' : 'ALIVE'}
					</span>
				</div>
			</div>

			<!-- countdown -->
			<div class="countdown-wrap">
				<div class="countdown-bar-bg">
					<div class="countdown-bar" style="width: {barWidth}%; background: {barColor};"></div>
				</div>
				<div class="countdown-label">
					{#if activeExpired}
						<span class="expired-label">expired</span>
					{:else}
						<span class="remaining" style="color: {barColor};">{activeRemainingSec}s</span>
						<span class="remaining-hint">remaining</span>
					{/if}
				</div>
			</div>

			{#if grantAttachKey.trim()}
				<div class="attached-key">
					<span class="meta-label">attached key</span>
					<span class="attached-key-val">{grantAttachKey}</span>
					{#if activeExpired}
						{#if activeAttachedKeyGone}
							<span class="key-badge key-gone">deleted by server</span>
						{:else}
							<span class="key-badge key-maybe">checking...</span>
						{/if}
					{:else}
						<span class="key-badge key-alive">tied to lease</span>
					{/if}
				</div>
			{/if}

			<div class="lookup-strip">
				<div class="lookup-strip-header">
					<span class="meta-label">live lookup (polls /v1/lease/lookup every 2s)</span>
					{#if !activeExpired}
						<button class="btn-mini" onclick={doLookup} disabled={loading === 'lookup'}>
							{loading === 'lookup' ? '...' : 'refresh now'}
						</button>
					{/if}
				</div>
 
				{#if lookupError}
					<div class="lookup-error">lookup error: {lookupError}</div>
				{:else if activeLiveLookup}
					<div class="lookup-cells">
						<div class="lookup-cell">
							<span class="meta-label">id</span>
							<span class="lookup-val">{activeLiveLookup.id}</span>
						</div>
						<div class="lookup-cell">
							<span class="meta-label">original_ttl</span>
							<span class="lookup-val">{activeLiveLookup.original_ttl}s</span>
						</div>
						<div class="lookup-cell">
							<span class="meta-label">remaining_ttl</span>
							<span class="lookup-val accent">{activeLiveLookup.remaining_ttl}s</span>
						</div>
					</div>
				{:else}
					<span class="lookup-waiting">first poll in about 2 seconds</span>
				{/if}
			</div>
 
			<div class="actions">
				{#if !activeExpired}
					<button class="btn-keepalive" onclick={keepAlive} disabled={loading !== null}>
						{#if loading === 'keepalive'}<span class="spinner"></span>{:else}KEEP-ALIVE{/if}
					</button>
					<button class="btn-revoke" onclick={revoke} disabled={loading !== null}>
						{#if loading === 'revoke'}<span class="spinner"></span>{:else}REVOKE{/if}
					</button>
				{:else}
					<button class="btn-primary" onclick={reset}>NEW LEASE</button>
				{/if}
			</div>
		</div>
	{/if}

	{#if error}
		<div class="error-row">
			<span class="error-icon">✕</span>
			<span>{error}</span>
		</div>
	{/if}

	{#if !activeLease}
		<div class="explainer">
			<span>
				Leases are time-bounded tokens. Keys attached to a lease are automatically deleted when it
				expires or is revoked. Keep-Alive resets the TTL back to its original value.
			</span>
		</div>
	{/if}
</section>

<style>
	.lease {
		display: flex;
		flex-direction: column;
		gap: 16px;
		font-family: var(--mono);
		background: var(--bg);
		padding: 24px;
		color: var(--text);
	}

	.form {
		display: flex;
		flex-direction: column;
		gap: 12px;
	}

	.field {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}
	.field.narrow {
		max-width: 200px;
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

	.btn-primary {
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
		min-width: 130px;
		text-align: center;
	}
	.btn-primary:hover:not(:disabled) {
		background: var(--accent);
		color: #fff;
	}
	.btn-primary:disabled {
		opacity: 0.4;
		cursor: not-allowed;
	}

	.btn-keepalive {
		background: rgba(55, 189, 141, 0.1);
		border: 1px solid var(--success);
		border-radius: var(--radius);
		color: #166534;
		font-family: var(--mono);
		font-size: 11px;
		font-weight: 600;
		letter-spacing: 0.1em;
		padding: 8px 20px;
		cursor: pointer;
		transition: background 0.15s;
	}
	.btn-keepalive:hover:not(:disabled) {
		background: rgba(55, 189, 141, 0.2);
	}
	.btn-keepalive:disabled {
		opacity: 0.4;
		cursor: not-allowed;
	}

	.btn-keepalive {
		background: rgba(55,189,141,0.1);
		border: 1px solid var(--success);
		border-radius: var(--radius);
		color: #166534;
		font-family: var(--mono);
		font-size: 11px;
		font-weight: 600;
		letter-spacing: 0.1em;
		padding: 8px 20px;
		cursor: pointer;
		transition: background 0.15s;
	}
	.btn-keepalive:hover:not(:disabled) { background: rgba(55,189,141,0.2); }
	.btn-keepalive:disabled { opacity: 0.4; cursor: not-allowed; }
 

	.btn-revoke {
		background: var(--error-bg);
		border: 1px solid var(--error-border);
		border-radius: var(--radius);
		color: var(--error);
		font-family: var(--mono);
		font-size: 11px;
		font-weight: 600;
		letter-spacing: 0.1em;
		padding: 8px 20px;
		cursor: pointer;
		transition: background 0.15s;
	}
	.btn-revoke:hover:not(:disabled) {
		background: #ffe4e6;
	}
	.btn-revoke:disabled {
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
	.btn-mini:hover { border-color: var(--accent); color: var(--accent); }
	.btn-mini:disabled { opacity: 0.4; cursor: not-allowed; }


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
	}
	.explainer-icon {
		color: var(--accent);
		flex-shrink: 0;
	}

	.lease-header {
		display: flex;
		background: var(--surface);
		border-bottom: 1px solid var(--border);
	}
 
	.lease-meta-cell {
		display: flex;
		flex-direction: column;
		gap: 3px;
		padding: 10px 16px;
		border-right: 1px solid var(--border);
	}
	.lease-meta-cell:last-child { border-right: none; }
 

	/* lease card */
	.lease-card {
		border: 1px solid var(--border);
		border-radius: var(--radius);
		overflow: hidden;
		transition: border-color 0.3s;
	}
	.lease-card.expired {
		border-color: var(--error-border);
	}

	.lease-header {
		display: flex;
		gap: 0;
		background: var(--surface);
		border-bottom: 1px solid var(--border);
	}

	.lease-id-block,
	.lease-ttl-block {
		display: flex;
		flex-direction: column;
		gap: 2px;
		padding: 10px 16px;
		border-right: 1px solid var(--border);
	}
	.lease-ttl-block {
		border-right: none;
	}

	.lease-id {
		font-size: 14px;
		font-weight: 600;
		color: var(--text);
	}

	.lease-ttl-val {
		font-size: 14px;
		font-weight: 600;
		color: var(--dim);
	}

	.countdown-wrap {
		padding: 12px 16px;
		background: var(--bg);
	}

	.countdown-bar-bg {
		height: 6px;
		background: var(--border);
		border-radius: 3px;
		overflow: hidden;
		margin-bottom: 6px;
	}

	.countdown-bar {
		height: 100%;
		border-radius: 3px;
		transition:
			width 0.8s linear,
			background 0.5s;
	}

	.countdown-label {
		display: flex;
		align-items: baseline;
		gap: 6px;
		font-size: 12px;
	}

	.remaining {
		font-size: 20px;
		font-weight: 700;
	}

	.remaining-hint {
		color: var(--dim);
		font-size: 11px;
	}

	.expired-label {
		color: var(--error);
		font-weight: 600;
		font-size: 13px;
	}

	.attached-key {
		display: flex;
		align-items: center;
		gap: 10px;
		padding: 8px 16px;
		background: var(--surface);
		border-top: 1px solid var(--border);
	}

	.attached-key-val {
		font-weight: 600;
		color: var(--text);
	}

	.attached-key-row {
		display: flex;
		align-items: center;
		gap: 10px;
		padding: 8px 16px;
		background: var(--surface);
		border-bottom: 1px solid var(--border);
	}
 
	.attached-key-name {
		font-weight: 600;
		color: var(--text);
		flex: 1;
	}

	.key-gone {
		font-size: 10px;
		color: var(--error);
		font-style: italic;
	}

	.lookup-strip {
		display: flex;
		border-top: 1px solid var(--border);
	}

	.lookup-cell {
		display: flex;
		flex-direction: column;
		gap: 2px;
		padding: 8px 16px;
		border-right: 1px solid var(--border);
	}
	.lookup-cell:last-child {
		border-right: none;
	}

	.lookup-val {
		font-size: 13px;
		font-weight: 500;
	}
	.lookup-val.accent {
		color: var(--accent);
	}

	.actions {
		display: flex;
		gap: 10px;
		padding: 12px 16px;
		background: var(--surface);
		border-top: 1px solid var(--border);
	}

	.meta-label {
		font-size: 9px;
		font-weight: 600;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--dim);
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
	}
	.error-icon {
		font-size: 10px;
		font-weight: 700;
	}


	.key-badge {
		font-size: 9px;
		font-weight: 700;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		border-radius: 2px;
		padding: 2px 6px;
	}
	.key-alive {
		background: rgba(55,189,141,0.1);
		color: #166534;
		border: 1px solid var(--success);
	}
	.key-gone {
		background: var(--error-bg);
		color: var(--error);
		border: 1px solid var(--error-border);
	}
	.key-maybe {
		background: var(--warn-bg);
		color: var(--warn);
		border: 1px solid var(--warn-border);
	}
 
	/* lookup strip */
	.lookup-strip {
		padding: 10px 16px;
		background: var(--bg);
		border-bottom: 1px solid var(--border);
		display: flex;
		flex-direction: column;
		gap: 8px;
	}
 
	.lookup-strip-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
	}
 
	.lookup-cells {
		display: flex;
		gap: 0;
		border: 1px solid var(--border);
		border-radius: var(--radius);
		overflow: hidden;
	}
 
	.lookup-cell {
		display: flex;
		flex-direction: column;
		gap: 2px;
		padding: 6px 14px;
		background: var(--surface);
		border-right: 1px solid var(--border);
	}
	.lookup-cell:last-child { border-right: none; }
 
	.lookup-val {
		font-size: 13px;
		font-weight: 500;
		color: var(--text);
	}
	.lookup-val.accent { color: var(--accent); }
 
	.lookup-waiting {
		font-size: 11px;
		color: var(--dim);
		font-style: italic;
	}
 
	.lookup-error {
		font-size: 11px;
		color: var(--error);
	}
 
	/* actions */
	.actions {
		display: flex;
		gap: 10px;
		padding: 12px 16px;
		background: var(--surface);
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
