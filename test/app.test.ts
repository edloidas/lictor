import { describe, expect, it } from 'bun:test';
import { createVerify, generateKeyPairSync } from 'node:crypto';
import { normalizePem } from '../src/config.ts';
import { createAppJwt } from '../src/github/app.ts';

const { privateKey, publicKey } = generateKeyPairSync('rsa', {
  modulusLength: 2048,
  privateKeyEncoding: { type: 'pkcs1', format: 'pem' },
  publicKeyEncoding: { type: 'spki', format: 'pem' },
});

const nowSeconds = 1_700_000_000;
const claimsOf = (jwt: string) => {
  const [, claims] = jwt.split('.');
  return JSON.parse(Buffer.from(claims ?? '', 'base64url').toString());
};

describe('createAppJwt', () => {
  const jwt = createAppJwt({ appId: '12345', privateKey, nowSeconds });

  it('emits three base64url segments', () => {
    expect(jwt.split('.')).toHaveLength(3);
    expect(jwt).not.toContain('=');
    expect(jwt).not.toContain('+');
    expect(jwt).not.toContain('/');
  });

  it('declares RS256', () => {
    const [header] = jwt.split('.');

    expect(JSON.parse(Buffer.from(header ?? '', 'base64url').toString())).toEqual({
      alg: 'RS256',
      typ: 'JWT',
    });
  });

  it('issues from the app id', () => {
    expect(claimsOf(jwt).iss).toBe('12345');
  });

  // GitHub rejects a JWT whose `iat` is ahead of its own clock.
  it('backdates iat by a minute', () => {
    expect(claimsOf(jwt).iat).toBe(nowSeconds - 60);
  });

  it('expires inside GitHub’s ten-minute ceiling', () => {
    expect(claimsOf(jwt).exp - claimsOf(jwt).iat).toBeLessThanOrEqual(600);
    expect(claimsOf(jwt).exp).toBeGreaterThan(nowSeconds);
  });

  it('signs the header and claims with the private key', () => {
    const [header, claims, signature] = jwt.split('.');
    const verified = createVerify('RSA-SHA256')
      .update(`${header}.${claims}`)
      .verify(publicKey, Buffer.from(signature ?? '', 'base64url'));

    expect(verified).toBe(true);
  });

  it('rejects a signature checked against the wrong payload', () => {
    const [, , signature] = jwt.split('.');
    const verified = createVerify('RSA-SHA256')
      .update('tampered.payload')
      .verify(publicKey, Buffer.from(signature ?? '', 'base64url'));

    expect(verified).toBe(false);
  });

  // A PEM pasted into a single-line `.env` value arrives escaped.
  it('accepts a key whose newlines arrived escaped', () => {
    const escaped = privateKey.replace(/\n/g, '\\n');

    expect(createAppJwt({ appId: '12345', privateKey: escaped, nowSeconds })).toBe(jwt);
  });
});

describe('normalizePem', () => {
  it('turns escaped newline pairs into real newlines', () => {
    expect(normalizePem('a\\nb')).toBe('a\nb');
  });

  it('leaves a real newline alone', () => {
    expect(normalizePem('a\nb')).toBe('a\nb');
  });
});
