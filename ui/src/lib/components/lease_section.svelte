<script lang="ts">
	import type { KaveClient } from '$lib/kave_client';
	import type { LeaseGrantResponse, LeaseLookupResponse } from '$lib/kv';
	import { KaveError } from '$lib/kv';
	import { onDestroy } from 'svelte';

	let { client }: { client: KaveClient } = $props();

	let grantTtlInput = $state(15);
	let grantAttachKey = $state('');
	let grantAttachValue = $state('');

	// ── active lease ──────────────────────────────────────────
	let activeLease = $state<LeaseGrantResponse | null>(null);
	let actveLookup = $state<LeaseLookupResponse | null>(null);
	let activeRemainingSec = $state(0);
	let activeExpired = $state(false);

	let loading = $state<string | null>(null); // which action is loading
	let error = $state<string | null>(null);

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
				actveLookup = await client.leaseLookup(activeLease.id);
				activeRemainingSec = actveLookup.remaining_ttl;
				if (activeRemainingSec <= 0) {
					activeExpired = true;
					clearIntervals();
				}
			} catch (e) {
				if (e instanceof KaveError) {
					console.log('error while lookup tick', e);
				}
				// lease probably expired on server
				activeExpired = true;
				clearIntervals();
			}
		}, 2000);
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
			actveLookup = null;
			activeExpired = false;
			startCountdown(activeLease.ttl);

			// attach a key if provided
			if (grantAttachKey.trim()) {
				await client.kvPut(grantAttachKey, grantAttachValue || grantAttachKey, {
					leaseId: activeLease.id
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
			activeRemainingSec = res.ttl;
			activeExpired = false;
			// restart countdown from new ttl
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
			activeExpired = true;
			activeRemainingSec = 0;
		} catch (e) {
			error = catchErr(e);
		} finally {
			loading = null;
		}
	}

	function reset() {
		clearIntervals();
		activeLease = null;
		actveLookup = null;
		activeExpired = false;
		activeRemainingSec = 0;
		error = null;
	}

	// countdown bar width as a percentage
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
				<label for="akey"
					>attach key <span class="hint">(optional: puts a key tied to this lease)</span></label
				>
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
		<div class="lease-card" class:activeExpired>
			<!-- lease id + ttl strip -->
			<div class="lease-header">
				<div class="lease-id-block">
					<span class="meta-label">lease id</span>
					<span class="lease-id">{activeLease.id}</span>
				</div>
				<div class="lease-ttl-block">
					<span class="meta-label">original ttl</span>
					<span class="lease-ttl-val">{activeLease.ttl}s</span>
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

			<!-- attached key -->
			{#if grantAttachKey.trim()}
				<div class="attached-key">
					<span class="meta-label">attached key</span>
					<span class="attached-key-val">{grantAttachKey}</span>
					{#if activeExpired}
						<span class="key-gone">deleted on expiry</span>
					{/if}
				</div>
			{/if}

			<!-- lookup strip -->
			{#if actveLookup}
				<div class="lookup-strip">
					<div class="lookup-cell">
						<span class="meta-label">original_ttl</span>
						<span class="lookup-val">{actveLookup.original_ttl}s</span>
					</div>
					<div class="lookup-cell">
						<span class="meta-label">remaining_ttl</span>
						<span class="lookup-val accent">{actveLookup.remaining_ttl}s</span>
					</div>
				</div>
			{/if}

			<!-- actions -->
			<div class="actions">
				{#if !activeExpired}
					<button class="btn-keepalive" onclick={keepAlive} disabled={loading !== null}>
						{#if loading === 'keepalive'}<span class="spinner"></span>{:else}↺ KEEP-ALIVE{/if}
					</button>
					<button class="btn-revoke" onclick={revoke} disabled={loading !== null}>
						{#if loading === 'revoke'}<span class="spinner"></span>{:else}✕ REVOKE{/if}
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

	.lease {
		display: flex;
		flex-direction: column;
		gap: 16px;
		font-family: var(--mono);
		background: var(--bg);
		padding: 24px;
		color: var(--text);
	}

	/* form */
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

	/* buttons */
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
		color: var(--bg);
	}
	.btn-primary:disabled {
		opacity: 0.4;
		cursor: not-allowed;
	}

	.btn-keepalive {
		background: #0e1a0e;
		border: 1px solid #3a5a3a;
		border-radius: var(--radius);
		color: #4caf60;
		font-family: var(--mono);
		font-size: 11px;
		font-weight: 600;
		letter-spacing: 0.1em;
		padding: 8px 20px;
		cursor: pointer;
		transition: background 0.15s;
	}
	.btn-keepalive:hover:not(:disabled) {
		background: #1a2e1a;
	}
	.btn-keepalive:disabled {
		opacity: 0.4;
		cursor: not-allowed;
	}

	.btn-revoke {
		background: #1a0a0a;
		border: 1px solid #5a2020;
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
		background: #2a1010;
	}
	.btn-revoke:disabled {
		opacity: 0.4;
		cursor: not-allowed;
	}

	/* lease card */
	.lease-card {
		border: 1px solid var(--border);
		border-radius: var(--radius);
		overflow: hidden;
		transition: border-color 0.3s;
	}
	.lease-card.expired {
		border-color: #5a2020;
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
		color: var(--accent);
		letter-spacing: 0.04em;
	}
	.lease-ttl-val {
		font-size: 13px;
		font-weight: 500;
	}

	/* countdown */
	.countdown-wrap {
		padding: 14px 16px 10px;
		background: var(--bg);
		border-bottom: 1px solid var(--border);
	}

	.countdown-bar-bg {
		height: 4px;
		background: #222;
		border-radius: 2px;
		overflow: hidden;
		margin-bottom: 8px;
	}

	.countdown-bar {
		height: 100%;
		border-radius: 2px;
		transition:
			width 1s linear,
			background 0.5s;
	}

	.countdown-label {
		display: flex;
		align-items: baseline;
		gap: 6px;
	}

	.remaining {
		font-size: 28px;
		font-weight: 600;
		line-height: 1;
		transition: color 0.5s;
	}

	.remaining-hint {
		font-size: 11px;
		color: var(--dim);
		letter-spacing: 0.06em;
	}

	.expired-label {
		font-size: 13px;
		color: var(--error);
		font-weight: 500;
	}

	/* attached key */
	.attached-key {
		display: flex;
		align-items: center;
		gap: 10px;
		padding: 8px 16px;
		background: var(--surface);
		border-bottom: 1px solid var(--border);
		font-size: 12px;
	}

	.attached-key-val {
		color: var(--text);
		font-weight: 500;
	}
	.key-gone {
		color: #884444;
		font-size: 11px;
		font-style: italic;
	}

	/* lookup strip */
	.lookup-strip {
		display: flex;
		background: var(--bg);
		border-bottom: 1px solid var(--border);
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

	/* actions */
	.actions {
		display: flex;
		gap: 10px;
		padding: 12px 16px;
		background: var(--surface);
	}

	/* shared meta labels */
	.meta-label {
		font-size: 9px;
		font-weight: 600;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--dim);
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
		padding: 10px 14px;
	}
	.error-icon {
		font-size: 10px;
		font-weight: 700;
	}

	/* explainer */
	.explainer {
		border-left: 3px solid var(--accent);
		background: var(--accent-dim);
		border-radius: 0 var(--radius) var(--radius) 0;
		padding: 10px 14px;
		font-family: var(--sans);
		font-size: 12px;
		color: #ccc;
		line-height: 1.6;
		display: flex;
		gap: 10px;
		align-items: flex-start;
	}
	.explainer-icon {
		color: var(--accent);
		font-size: 14px;
		flex-shrink: 0;
		margin-top: 1px;
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
