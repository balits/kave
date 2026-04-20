
function b64ToStr(bs: string): string {
    return new TextDecoder()
        .decode(Uint8Array.from(
            atob(bs), (c) => c.charCodeAt(0)
        ))
}

function strToB64(str: string): string {
    return btoa(
        String.fromCharCode(...new TextEncoder().encode(str))
    )
}

function bytesToB64(bytes: Uint8Array): string {
  return btoa(String.fromCharCode(...bytes))
}
 
function b64ToBytes(b64: string): Uint8Array {
  return Uint8Array.from(atob(b64), (c) => c.charCodeAt(0))
}

