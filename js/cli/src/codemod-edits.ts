/**
 * Minimal text edits over an unchanged original source.
 *
 * The codemod never reprints a syntax tree. It records replacements of
 * original ranges and applies them once, so formatting and comments outside
 * the rewritten expressions survive byte for byte.
 */

interface Edit {
  readonly end: number;
  /** Tie-breaker keeping zero-width insertions at one position in order. */
  readonly order: number;
  readonly start: number;
  readonly text: string;
}

/** Non-overlapping replacements of ranges in one original source text. */
export class SourceEdits {
  readonly #source: string;
  #edits: Edit[] = [];
  #order = 0;

  constructor(source: string) {
    this.#source = source;
  }

  /** Apply every edit and return the rewritten text. */
  apply(): string {
    return this.render(0, this.#source.length);
  }

  /**
   * The replacement whose range strictly contains `position`, so that text
   * inserted there would land inside rewritten code.
   */
  containing(
    position: number
  ): { readonly end: number; readonly start: number } | undefined {
    return this.#edits.find(
      (edit) => edit.start < position && position < edit.end
    );
  }

  /** Insert `text` at `position` without replacing anything. */
  insert(position: number, text: string): void {
    this.replace(position, position, text);
  }

  /** Whether `[start, end)` overlaps or touches the inside of an edit. */
  intersects(start: number, end: number): boolean {
    return this.#edits.some(
      (edit) => edit.start !== edit.end && edit.start < end && start < edit.end
    );
  }

  /**
   * The original text of `[start, end)` with the edits inside it applied.
   * Callers build a replacement for an outer node from this, which is why
   * {@link SourceEdits.replace} may then drop those inner edits.
   */
  render(start: number, end: number): string {
    let output = "";
    let cursor = start;
    for (const edit of this.#sorted()) {
      if (edit.start < start || edit.end > end) continue;
      if (edit.start === edit.end && edit.start === end && start !== end) {
        // An insertion at the end boundary belongs to the following text.
        continue;
      }
      output += this.#source.slice(cursor, edit.start) + edit.text;
      cursor = edit.end;
    }
    return output + this.#source.slice(cursor, end);
  }

  /**
   * Replace `[start, end)` with `text`. Edits strictly inside the range are
   * superseded, because the caller rendered them into `text`; a partial
   * overlap is a bug in the caller.
   */
  replace(start: number, end: number, text: string): void {
    const kept: Edit[] = [];
    for (const edit of this.#edits) {
      const inside =
        start !== end &&
        edit.start >= start &&
        edit.end <= end &&
        !(edit.start === edit.end && edit.start === end);
      if (inside) continue;
      const overlaps = edit.start < end && start < edit.end && start !== end;
      const splits = start === end && edit.start < start && start < edit.end;
      if (overlaps || splits) {
        throw new Error(
          `internal codemod error: overlapping edits at ${start}-${end} and ${edit.start}-${edit.end}`
        );
      }
      kept.push(edit);
    }
    kept.push({ end, order: this.#order++, start, text });
    this.#edits = kept;
  }

  #sorted(): Edit[] {
    return [...this.#edits].sort(
      (left, right) =>
        left.start - right.start ||
        // Insertions come before a replacement that starts at the same place.
        left.end - left.start - (right.end - right.start) ||
        left.order - right.order
    );
  }
}
