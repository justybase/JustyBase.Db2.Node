import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";

// DRDA EUSRIDPWD (security mechanism 9): Diffie-Hellman key agreement with a
// 256-bit prime and DES-CBC password/userid encryption. The parameters match
// the values published for DRDA and implemented by Apache Derby's
// EncryptionManager/DecryptionManager.
const PRIME = BigInt("0xC62112D73EE613F0947AB31F0F6846A1BFF5B3A4CA0D60BC1E4C7A0D8C16B3E3");
const BASE = BigInt("0x4690FA1F7B9E1D4442C86C9114603FDECF071EDCEC5F626E21E256AED9EA34E4");

function modPow(base: bigint, exponent: bigint, modulus: bigint): bigint {
  let result = 1n;
  let value = base % modulus;
  let power = exponent;
  while (power > 0n) {
    if (power & 1n) result = (result * value) % modulus;
    value = (value * value) % modulus;
    power >>= 1n;
  }
  return result;
}

function toFixed32(value: bigint): Uint8Array {
  const output = new Uint8Array(32);
  for (let i = 31; i >= 0 && value > 0n; i--) {
    output[i] = Number(value & 0xffn);
    value >>= 8n;
  }
  return output;
}

function fromFixed32(value: Uint8Array): bigint {
  let result = 0n;
  for (const byte of value) result = (result << 8n) | BigInt(byte);
  return result;
}

function normalizePeerPublicKey(publicKey: Uint8Array): Uint8Array {
  if (publicKey.length > 32) throw new RangeError("DB2 EUSRIDPWD peer public key is longer than 32 bytes");
  if (publicKey.length === 32) return publicKey;
  const padded = new Uint8Array(32);
  padded.set(publicKey, 32 - publicKey.length);
  return padded;
}

/** Generates a 255-bit Diffie-Hellman private exponent for DRDA SECMEC 9. */
export function generateSecmec9PrivateKey(): bigint {
  for (;;) {
    const candidate = fromFixed32(randomBytes(32)) & ((1n << 255n) - 1n);
    if (candidate >= 2n && candidate < PRIME - 1n) return candidate;
  }
}

/** Computes g^private mod p as the 32-byte big-endian DRDA public key. */
export function secmec9PublicKey(privateKey: bigint): Uint8Array {
  if (privateKey < 2n || privateKey >= PRIME - 1n) throw new RangeError("DB2 EUSRIDPWD private key is out of range");
  return toFixed32(modPow(BASE, privateKey, PRIME));
}

/** Derives the 32-byte shared secret from the server public key and client private key. */
export function secmec9SessionKey(serverPublicKey: Uint8Array, privateKey: bigint): Uint8Array {
  if (privateKey < 2n || privateKey >= PRIME - 1n) throw new RangeError("DB2 EUSRIDPWD private key is out of range");
  const peer = normalizePeerPublicKey(serverPublicKey);
  const peerValue = fromFixed32(peer);
  if (peerValue <= 1n || peerValue >= PRIME - 1n) throw new RangeError("DB2 EUSRIDPWD server public key is out of range");
  return toFixed32(modPow(peerValue, privateKey, PRIME));
}

// Node builds with OpenSSL 3 no longer load the single-DES provider by
// default. Triple DES with three identical keys performs E_K(D_K(E_K(x))) =
// E_K(x), so its CBC ciphertext is identical to DES-CBC and produces the exact
// DRDA wire format when des-cbc itself is unavailable.
function desCbcEncrypt(key: Uint8Array, iv: Uint8Array, plaintext: Uint8Array): Uint8Array {
  try {
    const cipher = createCipheriv("des-cbc", key, iv);
    return new Uint8Array(Buffer.concat([cipher.update(plaintext), cipher.final()]));
  } catch {
    const triple = Buffer.concat([key, key, key]);
    const cipher = createCipheriv("des-ede3-cbc", triple, iv);
    return new Uint8Array(Buffer.concat([cipher.update(plaintext), cipher.final()]));
  }
}

/** Encrypts one SECMEC 9 field with DES-CBC/PKCS5 using the DRDA derived key and server-key IV. */
export function encryptSecmec9(plaintext: Uint8Array, serverPublicKey: Uint8Array, privateKey: bigint): Uint8Array {
  const peer = normalizePeerPublicKey(serverPublicKey);
  if (peer.length < 20) throw new RangeError("DB2 EUSRIDPWD server security token is too short");
  const sessionKey = secmec9SessionKey(peer, privateKey);
  return desCbcEncrypt(sessionKey.subarray(12, 20), peer.subarray(12, 20), plaintext);
}

/** Exposes the DES-CBC primitive so tests can mirror the server-side decryption. */
export function desCbcDecryptWith(key: Uint8Array, iv: Uint8Array, ciphertext: Uint8Array): Uint8Array {
  try {
    const decipher = createDecipheriv("des-cbc", key, iv);
    return new Uint8Array(Buffer.concat([decipher.update(ciphertext), decipher.final()]));
  } catch {
    const triple = Buffer.concat([key, key, key]);
    const decipher = createDecipheriv("des-ede3-cbc", triple, iv);
    return new Uint8Array(Buffer.concat([decipher.update(ciphertext), decipher.final()]));
  }
}
