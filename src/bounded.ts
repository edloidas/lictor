/** Whether `bounded` would cut this value at `max`. */
export const exceeds = (value: string, max: number): boolean => Buffer.byteLength(value) > max;

/**
 * Truncate to a byte budget, not a character count, cutting only on a character
 * boundary so no partial sequence is ever decoded.
 *
 * ! The boundary is found, not repaired afterwards. Slicing and then stripping a
 * ! trailing U+FFFD cannot tell the one a broken sequence decoded to from one the
 * ! sender typed, and this bounds a record of what was accepted: deleting a real
 * ! character would make the stored request differ from the accepted request
 * ! while `exceeds` still reports that nothing was lost.
 */
export const bounded = (value: string, max: number): string => {
  const buffer = Buffer.from(value);
  if (buffer.length <= max) return value;
  let end = max;
  // A continuation byte (0b10xxxxxx) at the cut means it fell inside a
  // character; walk back to that character's lead byte and drop it whole.
  while (end > 0 && ((buffer[end] ?? 0) & 0xc0) === 0x80) end--;
  return buffer.subarray(0, end).toString('utf8');
};
