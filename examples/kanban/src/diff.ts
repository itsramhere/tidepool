// diffText computes the minimal edit between two strings by common
// prefix/suffix (code-point-wise, via Array.from — never UTF-16 positions).
// The result translates one textarea change into RGA ops: delete delCount
// visible elements starting at pos, then insertText insert at pos.
export interface TextDiff {
  pos: number;
  delCount: number;
  insert: string;
}

export function diffText(oldStr: string, newStr: string): TextDiff {
  const a = Array.from(oldStr);
  const b = Array.from(newStr);
  let p = 0;
  const min = Math.min(a.length, b.length);
  while (p < min && a[p] === b[p]) {
    p++;
  }
  let s = 0;
  while (s < min - p && a[a.length - 1 - s] === b[b.length - 1 - s]) {
    s++;
  }
  return {
    pos: p,
    delCount: a.length - p - s,
    insert: b.slice(p, b.length - s).join(""),
  };
}
