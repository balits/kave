import { ristretto255 } from '@noble/curves/ed25519.js';
import { KaveError } from './kv';

export interface OTOptions {
	slotCount: number;
	slotSize: number;
}

// required to ensure our random scalars are within the group order, copied from circl/group.
const OT_SCALAR_ORDER = BigInt(
	'0x1000000000000000000000000000000014def9dea2f79cd65812631a5cf5d3ed'
);

export function bytesToBigintLE(bytes: Uint8Array): bigint {
	let res = 0n;
	for (let i = 0; i < bytes.length; i++) {
		res += BigInt(bytes[i]) << BigInt(8 * i);
	}
	return res;
}

export function randomScalar(): bigint {
	const randomBytes = crypto.getRandomValues(new Uint8Array(64));
	const randomBigInt = bytesToBigintLE(randomBytes);
	const sc = randomBigInt % OT_SCALAR_ORDER;
	if (sc === 0n) return randomScalar(); 
    return sc;
}

export function fakeBlob(slotCount: number, slotSize: number): Uint8Array {
	const blob = new Uint8Array(slotCount * slotSize);
	for (let i = 0; i < slotCount; i++) {
		const offset = i * slotSize;
		blob[offset] = i;
		for (let j = 0; j < slotSize; j++) {
			blob[offset + j] = (i + j) & 0xff;
		}
	}
	return blob;
}

/**
 * Receiver side:
 * Given servers point A and desired choice index c:
 *	b       = random scalar
 *	B       = b*G + c*A       // blinded choice point
 */
export function blindedChoice(
	pointABytes: Uint8Array,
	choice: number
): { pointBBytes: Uint8Array; scalarB: bigint } {
	const A = ristretto255.Point.fromBytes(pointABytes);

	const scalarB = randomScalar();
	const bG = ristretto255.Point.BASE.multiply(scalarB);

	let B;
	if (choice === 0) {
		B = bG
	} else {
		const cScalar = BigInt(choice);
		const cA = A.multiply(cScalar);
		B = bG.add(cA);
	}


	return {
		pointBBytes: B.toBytes(),
		scalarB: scalarB
	};
}

/**
 * After receiving ciphertexts from Transfer:
 *	key     = b*A              // only matches slot c's encryption key
 *	hash    = sha256(key)
 *	plain   = GCM_Open(hash, ciphertexts[c])
 */
export async function tryDecrypt(
	pointABytes: Uint8Array<ArrayBufferLike>,
	scalarB: bigint,
	ct: Uint8Array<ArrayBufferLike>
): Promise<Uint8Array<ArrayBufferLike>> {
	const A = ristretto255.Point.fromBytes(pointABytes);

	const key = A.multiply(scalarB);
	const keyBytes = key.toBytes();

	const hashBuffer = await crypto.subtle.digest('SHA-256', keyBytes);
	const hashedKey = new Uint8Array(hashBuffer);

	if (hashedKey.length !== 32) {
		throw new Error('Hashed key size mismatch');
	}

	const aesKey = await crypto.subtle.importKey('raw', hashedKey, { name: 'AES-GCM' }, false, [
		'decrypt'
	]);

	const nonceSize = 12; // go's cipher.NewGCM uses 12byte nonce by default
	if (ct.length < nonceSize) {
		throw new Error('Ciphertext too short to contain a valid nonce');
	}

	const nonce = ct.slice(0, nonceSize);
	const ciphertext = ct.slice(nonceSize);

	try {
		const plaintextBuffer = await crypto.subtle.decrypt(
			{ name: 'AES-GCM', iv: nonce },
			aesKey,
			ciphertext
		);
		return new Uint8Array(plaintextBuffer);
	} catch (err) {
		throw new KaveError('tryDecrypt failed', `${err}`);
	}
}

export async function decrypt(
	pointABytes: Uint8Array,
	scalarB: bigint,
	ciphertexts: Uint8Array[],
	choice: number
): Promise<Uint8Array> {
	if (choice < 0 || choice >= ciphertexts.length) {
		throw new Error('Choice index out of bounds');
	}
	return await tryDecrypt(pointABytes, scalarB, ciphertexts[choice]);
}
