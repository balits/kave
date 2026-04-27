import * as otlib from './ot';
import * as kv from './kv';

// NOTE: some reads use POST, because the browser does not supoert
// bodies on GET (eventhough its valid http)
export class KaveClient {
	private readonly adminToken: string = import.meta.env.VITE_KAVE_CLUSTER_URL as string;

	constructor(
		public readonly baseURL: string,
		public readonly otOptions: otlib.OTOptions
	) {}

	private async do<T>(method: string, path: string, body?: unknown): Promise<T> {
		let resp: Response;
		let headers: any = { 'Content-Type': 'application/json' }
		if (path.includes("/admin/")) {
			headers["X-Kave-Admin-Token"] = this.adminToken
		}
		try {
			resp = await fetch(`${this.baseURL}${path}`, {
				method,
				headers,
				body: body !== undefined ? JSON.stringify(body) : undefined
			});
		} catch (e: unknown) {
			if (e instanceof Error) throw new kv.KaveError(e.message, 'fetch failed');
			throw new kv.KaveError('unknown network error', 'fetch failed');
		}

		if (!resp.ok) {
			let msg = `HTTP ${resp.status}`;
			let cause: string | undefined;
			try {
				const err = await resp.json();
				msg = err.error ?? msg;
				cause = err.cause;
			} catch (e) {
				if (e instanceof Error) {
					msg = e.message ?? msg;
					cause = `${e.cause}`;
				}
			}

			throw new kv.KaveError(msg, cause, resp.status);
		}

		return resp.json() as Promise<T>;
	}

	async stats(): Promise<kv.KaveStats> {
		return this.do('GET', '/stats');
	}

	async killNode(nodeId: string): Promise<void> {
		throw new kv.KaveError('/v1/admin/cluster/kill is unimplemented', 'WIP');
		await this.do('DELETE', `/admin/cluster/kill?node_id=${encodeURIComponent(nodeId)}`);
	}

	async kvPut(key: string, value: string, opts: kv.PutOptions = {}): Promise<kv.PutResponse> {
		const raw = await this.do<{
			header: kv.ResponseHeader;
			prev_entry?: kv.RawEntry;
		}>('POST', '/v1/kv/put', {
			key: kv.strToB64(key),
			value: kv.strToB64(value),
			lease_id: opts.leaseId ?? 0,
			prev_kv: opts.prevEntry ?? false,
			ignore_value: opts.ignoreValue ?? false,
			ignore_lease: opts.ignoreLease ?? false
		});

		return {
			header: raw.header,
			prev_entry: raw.prev_entry ? kv.decodeEntry(raw.prev_entry) : undefined
		};
	}

	async kvGet(
		key: string,
		opts: Pick<kv.RangeOptions, 'revision' | 'serializable'> = {}
	): Promise<kv.RangeResponse> {
		return this.kvRange(key, opts);
	}

	async kvRange(key: string, opts: kv.RangeOptions = {}): Promise<kv.RangeResponse> {
		const raw = await this.do<{
			header: kv.ResponseHeader;
			entries: kv.RawEntry[];
			count: number;
		}>('POST', '/v1/kv/range', {
			key: kv.strToB64(key),
			end: opts.end ? kv.strToB64(opts.end) : undefined,
			limit: opts.limit ?? 0,
			revision: opts.revision ?? 0,
			serializable: opts.serializable ?? false,
			prefix: opts.prefix ?? false,
			count_only: opts.countOnly ?? false
		});

		return {
			header: raw.header,
			entries: (raw.entries ?? []).map(kv.decodeEntry),
			count: raw.count
		};
	}

	async kvDelete(key: string, opts: kv.DeleteOptions = {}): Promise<kv.DeleteResponse> {
		const raw = await this.do<{
			header: kv.ResponseHeader;
			num_deleted: number;
			prev_entries?: kv.RawEntry[];
		}>('DELETE', '/v1/kv/delete', {
			key: kv.strToB64(key),
			end: opts.end ? kv.strToB64(opts.end) : undefined,
			prev_entries: opts.prevEntries ?? false
		});

		return {
			header: raw.header,
			num_deleted: raw.num_deleted,
			prev_entries: raw.prev_entries?.map(kv.decodeEntry)
		};
	}

	async kvTxn(req: kv.TxnRequest): Promise<kv.TxnResponse> {
		const raw = await this.do<{
			header: kv.ResponseHeader;
			success: boolean;
			results: Array<{
				put?: { prev_entry?: kv.RawEntry };
				delete?: { num_deleted: number; prev_entries?: kv.RawEntry[] };
				range?: { entries: kv.RawEntry[]; count: number };
			}>;
		}>('POST', '/v1/kv/txn', {
			comparisons: req.comparisons.map(kv.encodeComparison),
			success: req.success.map(kv.encodeTxnOp),
			failure: req.failure.map(kv.encodeTxnOp)
		});

		return {
			header: raw.header,
			success: raw.success,
			results: raw.results.map((r) => ({
				put: r.put
					? {
							header: raw.header,
							prev_entry: r.put.prev_entry ? kv.decodeEntry(r.put.prev_entry) : undefined
						}
					: undefined,
				delete: r.delete
					? {
							header: raw.header,
							num_deleted: r.delete.num_deleted,
							prev_entries: r.delete.prev_entries?.map(kv.decodeEntry)
						}
					: undefined,
				range: r.range
					? {
							header: raw.header,
							entries: (r.range.entries ?? []).map(kv.decodeEntry),
							count: r.range.count
						}
					: undefined
			}))
		};
	}

	async leaseGrant(ttl: number, id = 0): Promise<kv.LeaseGrantResponse> {
		return this.do('POST', '/v1/lease/grant', { ttl, id });
	}

	async leaseRevoke(id: number): Promise<kv.LeaseRevokeResponse> {
		return this.do('DELETE', '/v1/lease/revoke', { id });
	}

	async leaseKeepAlive(id: number): Promise<kv.LeaseKeepAliveResponse> {
		return this.do('POST', '/v1/lease/keep-alive', { id });
	}

	async leaseLookup(id: number): Promise<kv.LeaseLookupResponse> {
		return this.do('POST', '/v1/lease/lookup', { id });
	}

	async otInit(): Promise<kv.OTInitResponse> {
		const raw = await this.do<{
			header: kv.ResponseHeader;
			point_a: string; // b64
			token: string; // b64
		}>('POST', '/v1/ot/init', {}); // empty body to be open for extension

		return {
			header: raw.header,
			point_a: kv.b64ToBytes(raw.point_a),
			token: kv.b64ToBytes(raw.token)
		};
	}

	async otTransfer(
		token: Uint8Array,
		pointB: Uint8Array,
		serializable = false
	): Promise<kv.OTTransferResponse> {
		const raw = await this.do<{
			header: kv.ResponseHeader;
			ciphertexts: string[];
		}>('POST', '/v1/ot/transfer', {
			token: kv.bytesToB64(token),
			point_b: kv.bytesToB64(pointB),
			serializable
		});

		return {
			header: raw.header,
			ciphertexts: raw.ciphertexts.map(kv.b64ToBytes)
		};
	}

	async otWriteAll(slots: string[]): Promise<void> {
		if (slots.length !== this.otOptions.slotCount) {
			throw new kv.KaveError(
				'otWriteAll failed',
				`expected ${this.otOptions.slotCount} slots, got ${slots.length}`
			);
		}
		console.log("otWriteAll: length matches slot cound")

		const blob = new Uint8Array(this.otOptions.slotCount * this.otOptions.slotSize);
		const enc = new TextEncoder();

		for (let i = 0; i < slots.length; i++) {
			const encoded = enc.encode(slots[i]);
			if (encoded.length > this.otOptions.slotSize) {
				throw new kv.KaveError(
					'otWriteAll failed',
					`slot ${i} is ${encoded.length} bytes, max is ${this.otOptions.slotSize}`
				);
			}
			blob.set(encoded, i * this.otOptions.slotSize); // remaining bytes stay 0x00
		}

		console.log("otWriteAll: slots encoded to bytes")
		await this.do('POST', '/v1/ot/write-all', { blob: kv.bytesToB64(blob) });
	}

	/**
	 * High level call to OT:
	 *
	 * Performs the full otInit -> blindedChoice -> otTransfer -> decrypt pipeline in one call.
	 * Returns everything so the UI can display each step of the protocol.
	 */
	async otFetch(choice: number): Promise<kv.OTFetchResult> {
		const { point_a, token } = await this.otInit();
		const { pointBBytes, scalarB } = otlib.blindedChoice(point_a, choice);
		const { ciphertexts } = await this.otTransfer(token, pointBBytes);
		const plaintext = await otlib.tryDecrypt(point_a, scalarB, ciphertexts[choice]);
		return { point_a, point_b: pointBBytes, ciphertexts, plaintext };
	}
}
