<script lang="ts">
	import type { KaveClient } from '$lib/kave_client';
	import type { OTInitResponse, OTTransferResponse } from '$lib/kv';
	import { KaveError } from '$lib/kv';
	import * as otlib from '$lib/ot';
	import { bytesToB64, b64ToStr } from '$lib/kv';

	let { client }: { client: KaveClient } = $props();
	const slotCount = client.otOptions.slotCount;
	const slotSize  = client.otOptions.slotSize;

	let slots = $state<string[]>(Array.from({ length: slotCount }, (_, i) => `secret-${i}`));
	let writeLoading = $state(false);
	let writeError   = $state<string | null>(null);
	let writeSuccess = $state(false);

	// step 0 = idle, 1 = chose, 2 = init done, 3 = blind done, 4 = transfer done, 5 = done
	let step = $state(0);
	let choice = $state(0);

	let initResp   = $state<OTInitResponse | null>(null);
	let pointB     = $state<Uint8Array | null>(null);
	let scalarB    = $state<bigint | null>(null);
	let ciphertexts = $state<Uint8Array[]>([]);
	let plaintext  = $state<string | null>(null);
	let expectedFailedSlots = $state<number[]>([]);

	let stepLoading = $state(false);
	let stepError   = $state<string | null>(null);

	function reset() {
		step = 0;
		initResp = null;
		pointB = null;
		scalarB = null;
		ciphertexts = [];
		plaintext = null;
		expectedFailedSlots = [];
		stepError = null;
	}

	function slotOverflow(s: string) {
		return new TextEncoder().encode(s).length > slotSize;
	}


	async function stepWriteAll() {
		writeError = null;
		writeSuccess = false;
		writeLoading = true;
		try {
			await client.otWriteAll(slots);
			writeSuccess = true;
			reset();
		} catch (e) {
			console.log("write failed: ")
			writeError = e instanceof KaveError ? (e.cause ?? e.message) : String(e);
		} finally {
			writeLoading = false;
		}
	}

	// otInit, point_a
	async function stepDoInit() {
		stepLoading = true;
		stepError = null;
		try {
			initResp = await client.otInit();
			step = 2;
		} catch (e) {
			stepError = e instanceof KaveError ? (e.cause ?? e.message) : String(e);
		} finally {
			stepLoading = false;
		}
	}

	function doBlind() {
		if (!initResp) return;
		const { pointBBytes, scalarB: sb } = otlib.blindedChoice(initResp.point_a, choice);
		pointB  = pointBBytes;
		scalarB = sb;
		step = 3;
	}

	// point_b to server, receive N ciphertexts
	async function doTransfer() {
		if (!initResp || !pointB) return;
		stepLoading = true;
		stepError = null;
		try {
			const resp: OTTransferResponse = await client.otTransfer(initResp.token, pointB);
			ciphertexts = resp.ciphertexts;
			step = 4;
		} catch (e) {
			stepError = e instanceof KaveError ? (e.cause ?? e.message) : String(e);
		} finally {
			stepLoading = false;
		}
	}

	//decrypt chosen slot; try all others to show they fail
	async function doDecrypt() {
		if (!initResp || !scalarB) return;
		stepLoading = true;
		stepError = null;
		expectedFailedSlots = [];
		try {
			// decrypt chosen slot must succeed
			const raw = await otlib.tryDecrypt(initResp.point_a, scalarB, ciphertexts[choice]);
			// strip null padding, read as utf-8
			const trimmed = raw.subarray(0, raw.findIndex((b, i) => b === 0 && i > 0) || raw.length);
			plaintext = new TextDecoder().decode(trimmed.length > 0 ? trimmed : raw);

			// try every other slot must all fail
			const failed: number[] = [];
			for (let i = 0; i < ciphertexts.length; i++) {
				if (i === choice) continue;
				try {
					await otlib.tryDecrypt(initResp.point_a, scalarB, ciphertexts[i]);
					// if it didn't throw, that's unexpected and still show as wrong
				} catch {
					failed.push(i);
				}
			}
			expectedFailedSlots = failed;
			step = 5;
		} catch (e) {
			stepError = `Decryption of chosen slot failed (should not happen): ${e}`;
		} finally {
			stepLoading = false;
		}
	}

	function hexSnippetBytes(bytes: Uint8Array, n = 12): string {
		return Array.from(bytes.slice(0, n))
			.map((b) => b.toString(16).padStart(2, '0'))
			.join('') + '...';
	}

	function hexSnippetBigint(int: bigint, n = 12): string {
		let hex = int.toString(16).padStart(64, "0");
		return Uint8Array.
			from(hex.match(/../g)!
			.map(b => parseInt(b, 16)))
			.join('') + '...';
	}

	function catchErr(e: unknown): string {
		if (e instanceof KaveError) return e.cause ?? e.message;
		if (e instanceof Error) return e.message;
		return String(e);
	}

	const canProceed1 = $derived(writeSuccess || step >= 1);
	const stepLabels  = ['CHOOSE', 'INIT', 'BLIND', 'TRANSFER', 'DECRYPT'];
</script>

<section class="ot">
	<div class="intro">
		<div class="intro-title">OBLIVIOUS TRANSFER  <span class="intro-sub">1-out-of-{slotCount}</span></div>
		<p class="intro-body">
			The server holds <strong>{slotCount} secret slots</strong>. The client retrieves exactly one
			without the server learning <em>which</em> slot was chosen, and without the client learning
			any other slots content. This is cryptographically enforced via an elliptic curve
			point blinding protocol.
		</p>
	</div>

	<div class="panel">
		<div class="panel-header">
			<span class="panel-title">① ADMIN — LOAD SECRETS INTO SERVER</span>
			<span class="panel-hint">{slotCount} slots × {slotSize} bytes max each</span>
		</div>

		<div class="slots-grid">
			{#each slots as _, i}
				<div class="slot-field" class:overflow={slotOverflow(slots[i])}>
					<label class="slot-label">slot {i}</label>
					<input
						class="slot-input"
						type="text"
						bind:value={slots[i]}
						maxlength={slotSize}
						spellcheck="false"
						placeholder="secret-{i}"
					/>
				</div>
			{/each}
		</div>

		<div class="panel-footer">
			<button class="btn-primary" onclick={stepWriteAll} disabled={writeLoading}>
				{#if writeLoading}<span class="spinner"></span>{:else}WRITE ALL SLOTS{/if}
			</button>
			{#if writeSuccess}
				<span class="inline-ok">{slotCount} slots written to cluster</span>
			{/if}
			{#if writeError}
				<span class="inline-err">{writeError}</span>
			{/if}
		</div>
	</div>

	<div class="panel">
		<div class="panel-header">
			<span class="panel-title">② CLIENT - OT PROTOCOL</span>
			{#if step > 0 && step < 5}
				<button class="btn-mini" onclick={reset}>↺ restart</button>
			{/if}
		</div>

		<div class="step-track">
			{#each stepLabels as label, i}
				<div class="step-pip"
					class:done={step > i + 1}
					class:active={step === i + 1}
					class:pending={step < i + 1}
				>
					<div class="pip-dot">
						{#if step > i + 1}✓{:else}{i + 1}{/if}
					</div>
					<span class="pip-label">{label}</span>
				</div>
				{#if i < stepLabels.length - 1}
					<div class="pip-line" class:done={step > i + 1}></div>
				{/if}
			{/each}
		</div>

		{#if step === 0}
			<div class="step-body">
				<div class="step-heading">
					<span class="step-num">STEP 1</span>
					<span class="step-name">Client picks a secret slot</span>
				</div>
				<p class="step-desc">
					Choose which slot you want to retrieve. This choice stays
					<strong>hidden from the server</strong>, it never sees which index you select.
				</p>

				<div class="choice-wrap">
					<label class="meta-label">slot index (0 – {slotCount - 1})</label>
					<div class="choice-row">
						<input type="range" min="0" max={slotCount - 1} bind:value={choice} class="choice-slider" />
						<span class="choice-val">{choice}</span>
					</div>
					{#if writeSuccess}
						<div class="choice-preview">
							previewing slot {choice}: <em>"{slots[choice]}"</em>
							<span class="preview-note">(local only, not sent to server)</span>
						</div>
					{/if}
				</div>

				<button class="btn-step" onclick={() => { step = 1; stepDoInit(); }} disabled={stepLoading || !canProceed1}>
					{#if stepLoading}<span class="spinner"></span>{:else}CONFIRM CHOICE → INIT{/if}
				</button>
				{#if !canProceed1}
					<span class="step-warn">write slots first using the admin panel above</span>
				{/if}
			</div>

		<!-- step 2: init done, show pointA, blind -->
		{:else if step === 2}
			<div class="step-body">
				<div class="step-heading">
					<span class="step-num">STEP 2</span>
					<span class="step-name">Server generates a commitment point</span>
				</div>
				<p class="step-desc">
					The server responded with <code>point_a</code>, an ephemeral EC point used to
					set up the blinding. The server also issued a short-lived <code>token</code>
					binding this session. <strong>Your choice ({choice}) has not been sent yet.</strong>
				</p>

				{#if initResp}
					<div class="crypto-card">
						<div class="crypto-row">
							<span class="crypto-label">point_a</span>
							<span class="crypto-val">{hexSnippetBytes(initResp.point_a)}</span>
						</div>
						<div class="crypto-row">
							<span class="crypto-label">token</span>
							<span class="crypto-val">{hexSnippetBytes(initResp.token)}</span>
						</div>
					</div>
				{/if}

				<button class="btn-step" onclick={doBlind}>
					BLIND MY CHOICE LOCALLY →
				</button>
			</div>

		<!-- step 3: blind done, show pointB, transfer -->
		{:else if step === 3}
			<div class="step-body">
				<div class="step-heading">
					<span class="step-num">STEP 3</span>
					<span class="step-name">Client blinds the choice — no network call</span>
				</div>
				<p class="step-desc">
					Locally, the client computed <code>point_b = point_a · H(choice) · r</code> where
					<code>r</code> is a random scalar. The server will receive <code>point_b</code> but
					<strong>cannot reverse it to learn {choice}</strong>. The scalar <code>r</code>
					(scalarB) stays client-side.
				</p>

				{#if pointB && scalarB}
					<div class="crypto-card">
						<div class="crypto-row">
							<span class="crypto-label">point_b (sent to server)</span>
							<span class="crypto-val">{hexSnippetBytes(pointB)}</span>
						</div>
						<div class="crypto-row client-only">
							<span class="crypto-label">scalar_b (stays local 🔒)</span>
							<span class="crypto-val">{hexSnippetBigint(scalarB)}</span>
						</div>
					</div>
				{/if}

				<button class="btn-step" onclick={doTransfer} disabled={stepLoading}>
					{#if stepLoading}<span class="spinner"></span>{:else}SEND POINT_B → GET CIPHERTEXTS{/if}
				</button>
			</div>

		<!-- step 4: ciphertexts received, decrypt -->
		{:else if step === 4}
			<div class="step-body">
				<div class="step-heading">
					<span class="step-num">STEP 4</span>
					<span class="step-name">Server returns {slotCount} encrypted slots</span>
				</div>
				<p class="step-desc">
					The server encrypted each of its {slotCount} slots under a key derived from
					<code>point_b</code>. It has <strong>no idea</strong> which one the client can
					decrypt. All {slotCount} ciphertexts look identical in size and distribution.
				</p>

				<div class="ct-grid">
					{#each ciphertexts as ct, i}
						<div class="ct-chip" class:ct-chosen={i === choice}>
							<span class="ct-idx">{i}</span>
							<span class="ct-bytes">{hexSnippetBytes(ct, 8)}</span>
							{#if i === choice}
								<span class="ct-target">← my slot</span>
							{/if}
						</div>
					{/each}
				</div>

				<button class="btn-step" onclick={doDecrypt} disabled={stepLoading}>
					{#if stepLoading}<span class="spinner"></span>{:else}DECRYPT WITH SCALAR_B →{/if}
				</button>
			</div>

		<!-- step 5: result at last -->
		{:else if step === 5}
			<div class="step-body">
				<div class="step-heading">
					<span class="step-num">STEP 5</span>
					<span class="step-name">Decryption result</span>
				</div>

				<div class="result-block result-ok">
					<div class="result-header">
						<span class="result-badge badge-ok">DECRYPTED</span>
						<span class="result-slot-label">slot {choice}</span>
					</div>
					<div class="result-plaintext">{plaintext}</div>
					<div class="result-note">
						Client used <code>scalar_b</code> to unwrap slot {choice}.
						The server never learned this was the chosen slot.
					</div>
				</div>

				<div class="result-block result-fail">
					<div class="result-header">
						<span class="result-badge badge-fail">ALL OTHERS FAILED</span>
						<span class="result-slot-label">{expectedFailedSlots.length} of {slotCount - 1} attempted</span>
					</div>
					<div class="fail-chips">
						{#each expectedFailedSlots.slice(0, 16) as idx}
							<span class="fail-chip">slot {idx}</span>
						{/each}
						{#if expectedFailedSlots.length > 16}
							<span class="fail-chip fail-more">+{expectedFailedSlots.length - 16} more</span>
						{/if}
					</div>
					<div class="result-note">
						<code>tryDecrypt</code> threw for every other slot: the elliptic curve math produces garbage
						without the correct scalar.
					</div>
				</div>

				<button class="btn-step" onclick={reset}>RUN AGAIN</button>
			</div>
		{/if}

		{#if stepError}
			<div class="err-row">
				<span class="err-icon">✕</span>
				<span>{stepError}</span>
			</div>
		{/if}
	</div>

</section>

<style>
	.ot {
		display: flex;
		flex-direction: column;
		gap: 20px;
		font-family: var(--mono);
		background: var(--bg);
		padding: 24px;
		color: var(--text);
	}

	/* ── intro ── */
	.intro {
		border-left: 3px solid var(--accent);
		padding: 12px 16px;
		background: var(--accent-dim);
		border-radius: 0 var(--radius) var(--radius) 0;
	}

	.intro-title {
		font-size: 13px;
		font-weight: 700;
		letter-spacing: 0.1em;
		color: var(--text);
		margin-bottom: 6px;
	}

	.intro-sub {
		font-size: 11px;
		font-weight: 400;
		color: var(--accent);
		margin-left: 8px;
	}

	.intro-body {
		font-size: 12px;
		color: var(--dim);
		line-height: 1.7;
		margin: 0;
	}

	/* ── panels ── */
	.panel {
		border: 1px solid var(--border);
		border-radius: var(--radius);
		overflow: hidden;
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
	}

	.panel-hint {
		font-size: 10px;
		color: var(--dim);
	}

	.panel-footer {
		display: flex;
		align-items: center;
		gap: 14px;
		padding: 12px 16px;
		background: var(--surface);
		border-top: 1px solid var(--border);
	}

	/* ── slot grid ── */
	.slots-grid {
		display: grid;
		grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
		gap: 8px;
		padding: 14px 16px;
	}

	.slot-field {
		display: flex;
		flex-direction: column;
		gap: 3px;
	}
	.slot-field.overflow .slot-input {
		border-color: var(--error-border);
	}

	.slot-label {
		font-size: 9px;
		font-weight: 600;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--dim);
	}

	.slot-input {
		background: var(--surface);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		color: var(--text);
		font-family: var(--mono);
		font-size: 11px;
		padding: 5px 8px;
		outline: none;
		transition: border-color 0.15s;
		width: 100%;
		box-sizing: border-box;
	}
	.slot-input:focus { border-color: var(--accent); }

	/* ── step tracker ── */
	.step-track {
		display: flex;
		align-items: center;
		padding: 16px 20px;
		background: var(--surface);
		border-bottom: 1px solid var(--border);
		overflow-x: auto;
	}

	.step-pip {
		display: flex;
		flex-direction: column;
		align-items: center;
		gap: 4px;
		flex-shrink: 0;
	}

	.pip-dot {
		width: 24px;
		height: 24px;
		border-radius: 50%;
		display: flex;
		align-items: center;
		justify-content: center;
		font-size: 9px;
		font-weight: 700;
		transition: background 0.2s, color 0.2s;
	}

	.step-pip.done .pip-dot {
		background: var(--success);
		color: #fff;
		font-size: 11px;
	}
	.step-pip.active .pip-dot {
		background: var(--accent);
		color: #fff;
	}
	.step-pip.pending .pip-dot {
		background: var(--border);
		color: var(--dim);
	}

	.pip-label {
		font-size: 8px;
		font-weight: 600;
		letter-spacing: 0.08em;
		color: var(--dim);
	}
	.step-pip.active .pip-label { color: var(--accent); }
	.step-pip.done .pip-label   { color: var(--success); }

	.pip-line {
		flex: 1;
		height: 2px;
		background: var(--border);
		margin: 0 4px;
		margin-bottom: 14px;
		transition: background 0.2s;
		min-width: 20px;
	}
	.pip-line.done { background: var(--success); }

	/* ── step body ── */
	.step-body {
		padding: 20px;
		display: flex;
		flex-direction: column;
		gap: 14px;
	}

	.step-heading {
		display: flex;
		align-items: baseline;
		gap: 10px;
	}

	.step-num {
		font-size: 9px;
		font-weight: 700;
		letter-spacing: 0.12em;
		color: var(--accent);
	}

	.step-name {
		font-size: 14px;
		font-weight: 700;
		color: var(--text);
	}

	.step-desc {
		font-size: 12px;
		color: var(--dim);
		line-height: 1.7;
		margin: 0;
	}

	.step-warn {
		font-size: 11px;
		color: var(--error);
		font-style: italic;
	}

	/* ── choice selector ── */
	.choice-wrap {
		display: flex;
		flex-direction: column;
		gap: 8px;
	}

	.choice-row {
		display: flex;
		align-items: center;
		gap: 14px;
	}

	.choice-slider {
		flex: 1;
		accent-color: var(--accent);
	}

	.choice-val {
		font-size: 28px;
		font-weight: 700;
		color: var(--accent);
		min-width: 40px;
		text-align: right;
		line-height: 1;
	}

	.choice-preview {
		font-size: 12px;
		color: var(--dim);
		background: var(--surface);
		border: 1px solid var(--border);
		border-radius: var(--radius);
		padding: 8px 12px;
	}

	.preview-note {
		font-size: 10px;
		color: var(--accent);
		font-style: italic;
		margin-left: 6px;
	}

	/* ── crypto card ── */
	.crypto-card {
		border: 1px solid var(--border);
		border-radius: var(--radius);
		overflow: hidden;
		background: var(--surface);
	}

	.crypto-row {
		display: flex;
		align-items: center;
		gap: 12px;
		padding: 8px 14px;
		border-bottom: 1px solid var(--border);
	}
	.crypto-row:last-child { border-bottom: none; }
	.crypto-row.client-only { background: rgba(55,189,141,0.05); }

	.crypto-label {
		font-size: 9px;
		font-weight: 600;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--dim);
		min-width: 160px;
		flex-shrink: 0;
	}

	.crypto-val {
		font-size: 12px;
		font-weight: 500;
		color: var(--text);
		font-family: var(--mono);
		word-break: break-all;
	}

	/* ── ciphertext grid ── */
	.ct-grid {
		display: flex;
		flex-wrap: wrap;
		gap: 6px;
	}

	.ct-chip {
		display: flex;
		align-items: center;
		gap: 6px;
		border: 1px solid var(--border);
		border-radius: var(--radius);
		padding: 4px 10px;
		background: var(--surface);
		font-size: 10px;
		transition: border-color 0.2s;
	}

	.ct-chip.ct-chosen {
		border-color: var(--accent);
		background: var(--accent-dim);
	}

	.ct-idx {
		font-size: 9px;
		font-weight: 700;
		color: var(--dim);
		min-width: 16px;
	}

	.ct-bytes {
		color: var(--text);
		font-family: var(--mono);
	}

	.ct-target {
		font-size: 9px;
		color: var(--accent);
		font-weight: 600;
	}

	/* ── result blocks ── */
	.result-block {
		border: 1px solid var(--border);
		border-radius: var(--radius);
		overflow: hidden;
	}

	.result-ok  { border-color: var(--success); }
	.result-fail { border-color: var(--error-border); }

	.result-header {
		display: flex;
		align-items: center;
		gap: 10px;
		padding: 8px 14px;
		border-bottom: 1px solid var(--border);
		background: var(--surface);
	}

	.result-badge {
		font-size: 9px;
		font-weight: 700;
		letter-spacing: 0.1em;
		padding: 2px 7px;
		border-radius: 2px;
	}

	.badge-ok {
		background: rgba(55,189,141,0.15);
		color: #166534;
		border: 1px solid var(--success);
	}
	.badge-fail {
		background: var(--error-bg);
		color: var(--error);
		border: 1px solid var(--error-border);
	}

	.result-slot-label {
		font-size: 11px;
		color: var(--dim);
	}

	.result-plaintext {
		font-size: 22px;
		font-weight: 700;
		color: var(--text);
		padding: 16px;
		word-break: break-all;
	}

	.result-note {
		font-size: 11px;
		color: var(--dim);
		padding: 0 14px 12px;
		line-height: 1.6;
	}

	.fail-chips {
		display: flex;
		flex-wrap: wrap;
		gap: 5px;
		padding: 10px 14px;
	}

	.fail-chip {
		font-size: 9px;
		font-weight: 600;
		padding: 2px 8px;
		border-radius: 2px;
		background: var(--error-bg);
		color: var(--error);
		border: 1px solid var(--error-border);
	}
	.fail-more {
		background: var(--surface);
		color: var(--dim);
		border-color: var(--border);
	}

	/* ── buttons ── */
	.btn-step {
		align-self: flex-start;
		background: var(--accent);
		border: none;
		border-radius: var(--radius);
		color: #fff;
		font-family: var(--mono);
		font-size: 11px;
		font-weight: 700;
		letter-spacing: 0.12em;
		padding: 10px 24px;
		cursor: pointer;
		transition: opacity 0.15s;
		min-width: 140px;
		text-align: center;
	}
	.btn-step:hover:not(:disabled) { opacity: 0.85; }
	.btn-step:disabled { opacity: 0.4; cursor: not-allowed; }

	.btn-primary {
		background: var(--accent-dim);
		border: 1px solid var(--accent);
		border-radius: var(--radius);
		color: var(--accent);
		font-family: var(--mono);
		font-size: 11px;
		font-weight: 600;
		letter-spacing: 0.12em;
		padding: 8px 20px;
		cursor: pointer;
		transition: background 0.15s, color 0.15s;
		white-space: nowrap;
	}
	.btn-primary:hover:not(:disabled) { background: var(--accent); color: #fff; }
	.btn-primary:disabled { opacity: 0.4; cursor: not-allowed; }

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
		margin-left: auto;
	}
	.btn-mini:hover { border-color: var(--accent); color: var(--accent); }

	/* ── status ── */
	.inline-ok  { font-size: 12px; color: var(--success); font-weight: 600; }
	.inline-err { font-size: 12px; color: var(--error); }

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
		margin: 0 16px 16px;
	}
	.err-icon { font-size: 10px; font-weight: 700; }

	/* ── misc ── */
	.meta-label {
		font-size: 9px;
		font-weight: 600;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--dim);
	}

	code {
		background: var(--surface);
		border: 1px solid var(--border);
		border-radius: 3px;
		padding: 1px 5px;
		font-family: var(--mono);
		font-size: 11px;
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
	@keyframes spin { to { transform: rotate(360deg); } }
</style>
