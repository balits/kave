export class KaveError extends Error {
	constructor(
		message: string,
		public readonly cause?: string | undefined,
		public readonly status?: number | undefined
	) {
		super(message);
		this.name = 'KaveError';
	}
}

// api types
export interface ResponseHeader {
	revision: number;
	compacted_revision: number;
	node_id: string;
	raft_term: number;
	raft_index: number;
}

// Decoded entry with plain strings as fields
export interface Entry {
	key: string;
	value: string;
	create_revision: number;
	mod_revision: number;
	version: number;
	lease_id: string;
}

// Raw entry that arrives over the network, b64 encoded strings
export interface RawEntry {
	key: string;
	value: string;
	create_revision: number;
	mod_revision: number;
	version: number;
	lease_id?: string;
}

export function decodeEntry(raw: RawEntry): Entry {
	return {
		key: b64ToStr(raw.key),
		value: raw.value ? b64ToStr(raw.value) : '', // tombstones -> empty value
		create_revision: raw.create_revision,
		mod_revision: raw.mod_revision,
		version: raw.version,
		lease_id: raw.lease_id ?? "0"
	};
}

export interface PutOptions {
	leaseId?: string; // json floats cant handle go int64s
	prevEntry?: boolean;
	ignoreValue?: boolean;
	ignoreLease?: boolean;
}

export interface PutResponse {
	header: ResponseHeader;
	prev_entry?: Entry;
}

export interface RangeOptions {
	end?: string;
	limit?: number;
	revision?: number;
	serializable?: boolean;
	prefix?: boolean;
	countOnly?: boolean;
}

export interface RangeResponse {
	header: ResponseHeader;
	entries: Entry[];
	count: number;
}

export interface DeleteOptions {
	end?: string;
	prevEntries?: boolean;
}

export interface DeleteResponse {
	header: ResponseHeader;
	num_deleted: number;
	prev_entries?: Entry[];
}

export type ComparisonOperator = '=' | '!=' | '>' | '>=' | '<' | '<=';
export type CompareTargetField = 'VALUE' | 'CREATE' | 'MOD' | 'VERSION';

export interface CompareTargetValue {
	value?: string; // plain string here then base64 to the wire
	create_revision?: number;
	mod_revision?: number;
	version?: number;
}

export interface Comparison {
	key: string;
	operator: ComparisonOperator;
	target_field: CompareTargetField;
	target_value: CompareTargetValue;
}

export function encodeComparison(c: Comparison): unknown {
	return {
		key: strToB64(c.key),
		operator: c.operator,
		target_field: c.target_field,
		target_value: {
			value: c.target_value.value ? strToB64(c.target_value.value) : undefined,
			create_revision: c.target_value.create_revision,
			mod_revision: c.target_value.mod_revision,
			version: c.target_value.version
		}
	};
}

export type TxnOp =
	| { type: 'PUT'; put: { key: string; value: string; leaseId?: string } }
	| { type: 'DEL'; delete: { key: string; end?: string } }
	| { type: 'RANGE'; range: { key: string; end?: string; revision?: number } };

export interface TxnRequest {
	comparisons: Comparison[];
	success: TxnOp[];
	failure: TxnOp[];
}

export interface TxnOpResult {
	put?: Omit<PutResponse, 'header'>;
	delete?: Omit<DeleteResponse, 'header'>;
	range?: Omit<RangeResponse, 'header'>;
}

export function encodeTxnOp(op: TxnOp): unknown {
	switch (op.type) {
		case 'PUT':
			return {
				type: 'PUT',
				put: {
					key: strToB64(op.put.key),
					value: strToB64(op.put.value),
					lease_id: op.put.leaseId ?? "0"
				}
			};
		case 'DEL':
			return {
				type: 'DEL',
				delete: {
					key: strToB64(op.delete.key),
					end: op.delete.end ? strToB64(op.delete.end) : undefined
				}
			};
		case 'RANGE':
			return {
				type: 'RANGE',
				range: {
					key: strToB64(op.range.key),
					end: op.range.end ? strToB64(op.range.end) : undefined,
					revision: op.range.revision ?? 0
				}
			};
	}
}

export interface TxnResponse {
	header: ResponseHeader;
	success: boolean;
	results: TxnOpResult[];
}

export interface LeaseGrantResponse {
	// header: ResponseHeader
	ttl: number;
	id: string; // json floats cant handle go int64s
}

export interface LeaseRevokeResponse {
	// header: ResponseHeader
	found: boolean;
	revoked: boolean;
}

export interface LeaseKeepAliveResponse {
	// header: ResponseHeader
	ttl: number;
	id: string; // json floats cant handle go int64s
}

export interface LeaseLookupResponse {
	// header: ResponseHeader
	id: string; // json floats cant handle go int64s
	original_ttl: number;
	remaining_ttl: number;
}

export interface OTWriteAllRequest {
	blob: string;
}

export interface OTWriteAllResponse {
	header: ResponseHeader;
}

export type OTInitRequest = unknown;
export interface OTInitResponse {
	header: ResponseHeader;
	point_a: Uint8Array<ArrayBufferLike>;
	token: Uint8Array<ArrayBufferLike>;
}

export interface OTTransferRequest {
	token: string;
	pointB: string;
	serializable?: boolean;
}

export interface OTTransferResponse {
	header: ResponseHeader;
	ciphertexts: Uint8Array[];
}
export interface OTFetchResult {
	point_a: Uint8Array; // server's public point
	point_b: Uint8Array; // client's blinded choice point sent to server
	ciphertexts: Uint8Array[]; // all N encrypted slots returned by server
	plaintext: Uint8Array; // decrypted chosen slot
}

export interface CompactionRespone {
	success: boolean;
}

export interface KillNodeResponse {
	success: boolean;
}


export interface KavePeer {
	suffrage: string;
	id: string;
	address: string;
}

export interface KaveStats {
	state: string;
	term: number;
	last_log_index: number;
	last_log_term: number;
	commit_index: number;
	applied_index: number;
	fsm_pending: number;
	last_snapshot_index: number;
	last_snapshot_term: number;
	protocol_version: number;
	protocol_version_min: number;
	protocol_version_max: number;
	snapshot_version_min: number;
	snapshot_version_max: number;
	leader_id: string;
	leader_addr: string;
	num_peers: number,
	latest_configuration_index: number,
	latest_configuration: string,
	readable_configuration: string, // json of Array<KavePeer>
	revision: number;
	compacted_revision: number;
}

export function b64ToStr(bs: string): string {
	return new TextDecoder().decode(Uint8Array.from(atob(bs), (c) => c.charCodeAt(0)));
}

export function strToB64(str: string): string {
	return btoa(String.fromCharCode(...new TextEncoder().encode(str)));
}

export function bytesToB64(bytes: Uint8Array): string {
	return btoa(String.fromCharCode(...bytes));
}

export function b64ToBytes(b64: string): Uint8Array {
	return Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
}
