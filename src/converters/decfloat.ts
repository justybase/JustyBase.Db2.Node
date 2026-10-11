/** Decodes IEEE 754 decimal floating point using the Densely Packed Decimal encoding used by Db2. */
export function decodeDecfloat(data: Uint8Array): string {
  if (data.length !== 8 && data.length !== 16) throw new Error(`invalid DFP byte length: ${data.length} (must be 8 or 16)`);
  const small = data.length === 8;
  const bias = small ? 398 : 6176;
  const groups = small ? 5 : 11;
  const exponentBits = small ? 8 : 12;
  const coefficientBits = groups * 10;
  const totalBits = data.length * 8;
  let word = 0n;
  for (const byte of data) word = (word << 8n) | BigInt(byte);

  const sign = ((word >> BigInt(totalBits - 1)) & 1n) !== 0n;
  const combination = Number((word >> BigInt(totalBits - 6)) & 0x1fn);
  const exponentMask = (1n << BigInt(exponentBits)) - 1n;
  const exponentContinuation = Number((word >> BigInt(coefficientBits)) & exponentMask);
  const coefficientMask = (1n << BigInt(coefficientBits)) - 1n;
  const coefficient = word & coefficientMask;

  if (combination >= 0x1e) {
    if (combination === 0x1e) return sign ? "-Infinity" : "Infinity";
    // IEEE 754-2008 decimal specials: combination 0x1e is Infinity, 0x1f is NaN.
    // For NaN, the bit immediately following the combination field is the
    // signaling flag (1 = sNaN, 0 = quiet NaN), matching Db2's text rendering.
    const signaling = (word >> BigInt(totalBits - 7)) & 1n;
    return signaling !== 0n ? "sNaN" : "NaN";
  }

  const biasedExponent = combination >= 0x18
    ? (((combination >> 1) & 0x03) << exponentBits) | exponentContinuation
    : ((combination >> 3) << exponentBits) | exponentContinuation;
  const digits = [combination >= 0x18 ? 8 + (combination & 1) : combination & 0x07];
  for (let index = groups - 1; index >= 0; index--) {
    const encoded = Number((coefficient >> BigInt(index * 10)) & 0x3ffn);
    digits.push(...decodeDpd(encoded));
  }
  return formatDecfloat(sign, digits, biasedExponent - bias);
}

/** Encodes a numeric value to IEEE 754 DPD bytes for Db2 DECFLOAT(16) or DECFLOAT(34). */
export function encodeDecfloat(value: unknown, byteLength: number): Uint8Array {
  if (byteLength !== 8 && byteLength !== 16) throw new Error(`invalid DFP byte length: ${byteLength} (must be 8 or 16)`);
  const small = byteLength === 8;
  const bias = small ? 398 : 6176;
  const groups = small ? 5 : 11;
  const exponentBits = small ? 8 : 12;
  const maxDigits = small ? 16 : 34;
  const coefficientBits = groups * 10;
  const totalBits = byteLength * 8;
  let text = value === null || value === undefined ? "0" : String(value).trim();
  if (text === "" || text === "<nil>") text = "0";

  let negative = false;
  if (text.startsWith("-")) { negative = true; text = text.slice(1); }
  else if (text.startsWith("+")) text = text.slice(1);
  if (/^infinity$|^inf$/iu.test(text)) return encodeSpecial(byteLength, 0x1e, negative);
  if (/^nan$/iu.test(text)) return encodeSpecial(byteLength, 0x1f, false, false);
  if (/^snan$/iu.test(text)) return encodeSpecial(byteLength, 0x1f, false, true);

  const parts = text.match(/^((?:\d+(?:\.\d*)?)|(?:\.\d+))(?:[eE]([+-]?\d+))?$/u);
  if (!parts) throw new Error(`invalid decimal floating-point value ${JSON.stringify(value)}`);
  const exponentText = parts[2] ?? "0";
  const parsedExponent = Number(exponentText);
  if (!Number.isSafeInteger(parsedExponent)) throw new Error(`invalid decimal floating-point exponent in ${JSON.stringify(value)}`);
  const mantissa = parts[1]!;
  const point = mantissa.indexOf(".");
  let exponent = parsedExponent - (point < 0 ? 0 : mantissa.length - point - 1);
  let digitsText = mantissa.replace(".", "").replace(/^0+/u, "");
  if (digitsText === "") {
    digitsText = "0";
    // Canonical zero exponent; -0 preserves its sign via `negative`.
    exponent = 0;
  }
  if (!/^\d+$/u.test(digitsText)) throw new Error(`invalid decimal floating-point value ${JSON.stringify(value)}`);
  if (digitsText.length > maxDigits) {
    const rounded = roundDecfloatToPrecision(digitsText, maxDigits);
    digitsText = rounded.digits;
    exponent += rounded.exponentDelta;
  }
  const digits = digitsText.padStart(maxDigits, "0").split("").map((digit) => digit.charCodeAt(0) - 48);
  const biasedExponent = exponent + bias;
  const maxBiasedExponent = 3 * (1 << exponentBits) - 1;
  if (!Number.isInteger(biasedExponent) || biasedExponent < 0 || biasedExponent > maxBiasedExponent)
    throw new Error(`decimal floating-point exponent is out of range in ${JSON.stringify(value)}`);

  const leading = digits[0]!;
  const highExponent = biasedExponent >> exponentBits;
  const combination = leading >= 8
    ? 0x18 | ((highExponent & 0x03) << 1) | (leading - 8)
    : (highExponent << 3) | leading;
  const exponentContinuation = biasedExponent & ((1 << exponentBits) - 1);
  let coefficient = 0n;
  for (let index = 0; index < groups; index++) {
    coefficient = (coefficient << 10n) | BigInt(encodeDpd(digits[index * 3 + 1]!, digits[index * 3 + 2]!, digits[index * 3 + 3]!));
  }
  let word = (BigInt(combination) << BigInt(totalBits - 6)) |
    (BigInt(exponentContinuation) << BigInt(coefficientBits)) | coefficient;
  if (negative) word |= 1n << BigInt(totalBits - 1);
  return toFixedBytes(word, byteLength);
}

function encodeSpecial(byteLength: number, combination: number, negative: boolean, signaling = false): Uint8Array {
  const totalBits = byteLength * 8;
  let word = BigInt(combination) << BigInt(totalBits - 6);
  // Signaling NaNs set the bit immediately after the combination field (0x7e00..),
  // quiet NaNs leave it clear (0x7c00..), Infinity (0x1e) ignores it.
  if (signaling) word |= 1n << BigInt(totalBits - 7);
  if (negative) word |= 1n << BigInt(totalBits - 1);
  return toFixedBytes(word, byteLength);
}

/**
 * Rounds a significant-digit string to maxDigits using round-half-even
 * (IEEE 754 default, Db2 compatible) and returns the exponent adjustment.
 * Discarded low-order digits increase the exponent; carry overflow that
 * produces an extra digit shifts the coefficient and adds one more.
 */
function roundDecfloatToPrecision(digitsText: string, maxDigits: number): { digits: string; exponentDelta: number } {
  const excess = digitsText.length - maxDigits;
  const keep = digitsText.slice(0, maxDigits);
  const discard = digitsText.slice(maxDigits);
  let roundUp = false;
  const first = discard.charCodeAt(0) - 48;
  if (first > 5) roundUp = true;
  else if (first < 5) roundUp = false;
  else {
    let restNonZero = false;
    for (let i = 1; i < discard.length; i++) {
      if (discard.charCodeAt(i) !== 48) { restNonZero = true; break; }
    }
    if (restNonZero) roundUp = true;
    else {
      // Exactly half: round to even (odd last kept digit rounds up).
      const lastKeep = keep.charCodeAt(keep.length - 1) - 48;
      roundUp = (lastKeep & 1) === 1;
    }
  }
  if (!roundUp) return { digits: keep, exponentDelta: excess };
  // Add one to the kept prefix.
  const chars = keep.split("");
  let carry = 1;
  for (let i = chars.length - 1; i >= 0 && carry > 0; i--) {
    const d = (chars[i]!.charCodeAt(0) - 48) + carry;
    if (d === 10) { chars[i] = "0"; carry = 1; }
    else { chars[i] = String.fromCharCode(48 + d); carry = 0; }
  }
  if (carry === 1) {
    // 999..9 + 1 overflow: 1000..0 (maxDigits+1 chars). Shift to
    // 100..0 (maxDigits chars) with one additional exponent increment.
    return { digits: "1" + "0".repeat(maxDigits - 1), exponentDelta: excess + 1 };
  }
  return { digits: chars.join(""), exponentDelta: excess };
}

function encodeDpd(d2: number, d1: number, d0: number): number {
  const h2 = d2 >= 8, h1 = d1 >= 8, h0 = d0 >= 8;
  const v2 = d2 & 7, v1 = d1 & 7, v0 = d0 & 7;
  if (!h2 && !h1 && !h0) return (v2 << 7) | (v1 << 4) | v0;
  if (!h2 && !h1 && h0) return (v2 << 7) | (v1 << 4) | 8 | (v0 & 1);
  if (!h2 && h1 && !h0) return (v2 << 7) | (((v0 >> 2) & 1) << 6) | (((v0 >> 1) & 1) << 5) | ((v1 & 1) << 4) | (1 << 3) | (1 << 1) | (v0 & 1);
  if (h2 && !h1 && !h0) return (((v0 >> 2) & 1) << 9) | (((v0 >> 1) & 1) << 8) | ((v2 & 1) << 7) | (v1 << 4) | (1 << 3) | (1 << 2) | (v0 & 1);
  if (h2 && h1 && !h0) return (((v0 >> 2) & 1) << 9) | (((v0 >> 1) & 1) << 8) | ((v2 & 1) << 7) | ((v1 & 1) << 4) | (1 << 3) | (1 << 2) | (1 << 1) | (v0 & 1);
  if (h2 && !h1 && h0) return (((v1 >> 2) & 1) << 9) | (((v1 >> 1) & 1) << 8) | ((v2 & 1) << 7) | (1 << 5) | ((v1 & 1) << 4) | (1 << 3) | (1 << 2) | (1 << 1) | (v0 & 1);
  if (!h2 && h1 && h0) return (v2 << 7) | (1 << 6) | ((v1 & 1) << 4) | (1 << 3) | (1 << 2) | (1 << 1) | (v0 & 1);
  return ((v2 & 1) << 7) | (1 << 6) | (1 << 5) | ((v1 & 1) << 4) | (1 << 3) | (1 << 2) | (1 << 1) | (v0 & 1);
}

function toFixedBytes(value: bigint, count: number): Uint8Array {
  const result = new Uint8Array(count);
  for (let index = count - 1; index >= 0; index--) { result[index] = Number(value & 0xffn); value >>= 8n; }
  return result;
}

function decodeDpd(encoded: number): [number, number, number] {
  const b9=(encoded>>9)&1,b8=(encoded>>8)&1,b7=(encoded>>7)&1,b6=(encoded>>6)&1,b5=(encoded>>5)&1;
  const b4=(encoded>>4)&1,b3=(encoded>>3)&1,b2=(encoded>>2)&1,b1=(encoded>>1)&1,b0=encoded&1;
  if (b3 === 0) return [b9*4+b8*2+b7, b6*4+b5*2+b4, b2*4+b1*2+b0];
  if (b2 === 0 && b1 === 0) return [b9*4+b8*2+b7, b6*4+b5*2+b4, 8+b0];
  if (b2 === 0 && b1 === 1) return [b9*4+b8*2+b7, 8+b4, b6*4+b5*2+b0];
  if (b2 === 1 && b1 === 0) return [8+b7, b6*4+b5*2+b4, b9*4+b8*2+b0];
  if (b6 === 0 && b5 === 0) return [8+b7, 8+b4, b9*4+b8*2+b0];
  if (b6 === 0 && b5 === 1) return [8+b7, b9*4+b8*2+b4, 8+b0];
  if (b6 === 1 && b5 === 0) return [b9*4+b8*2+b7, 8+b4, 8+b0];
  return [8+b7, 8+b4, 8+b0];
}

function formatDecfloat(negative: boolean, digits: readonly number[], exponent: number): string {
  let first = 0;
  while (first < digits.length - 1 && digits[first] === 0) first++;
  const significant = digits.slice(first).join("");
  if (significant === "0") return negative ? "-0" : "0";
  const prefix = negative ? "-" : "";
  const decimalPosition = significant.length + exponent;
  if (decimalPosition > 0 && decimalPosition <= significant.length) {
    return prefix + (decimalPosition === significant.length
      ? significant
      : `${significant.slice(0, decimalPosition)}.${significant.slice(decimalPosition)}`);
  }
  if (decimalPosition > significant.length && decimalPosition <= significant.length + 10)
    return prefix + significant + "0".repeat(decimalPosition - significant.length);
  if (decimalPosition <= 0 && decimalPosition >= -6)
    return prefix + `0.${"0".repeat(-decimalPosition)}${significant}`;
  const mantissa = significant.length === 1 ? significant : `${significant[0]}.${significant.slice(1)}`;
  const scientificExponent = exponent + significant.length - 1;
  return `${prefix}${mantissa}E${scientificExponent >= 0 ? "+" : ""}${scientificExponent}`;
}
