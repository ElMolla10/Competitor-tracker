"""
differ.py — Snapshot comparison logic.

Uses difflib to produce a unified diff between two text snapshots.
Minor whitespace-only differences are ignored; only meaningful content
changes above a configurable threshold are reported.

Noise filtering runs on both snapshots before any comparison so that
cookie banners, chat widgets, and other dynamic UI strings never
pollute the diff or the changes table.
"""

import difflib
import logging
import re

logger = logging.getLogger(__name__)

# Minimum ratio of changed lines to total lines before we treat it as a
# meaningful change. 0.02 = at least 2 % of lines must differ.
CHANGE_THRESHOLD = 0.02

# Maximum number of diff lines to include in the summary (keeps DB rows small)
MAX_DIFF_LINES = 200

# ---------------------------------------------------------------------------
# Noise filter
# ---------------------------------------------------------------------------

# Exact strings (case-insensitive) that indicate dynamic UI / consent noise.
# Matched against the full normalised line.
_NOISE_PHRASES: frozenset[str] = frozenset({
    # Generic consent
    "accept", "decline", "reject", "dismiss",
    "we use cookies", "this site uses cookies", "our website uses cookies",
    "by continuing", "by using this site", "privacy policy", "cookie policy",
    "manage cookies", "cookie settings", "cookie preferences",
    "necessary cookies", "functional cookies", "analytics cookies",
    "allow all", "allow cookies", "allow selection",
    "accept all", "accept all cookies", "reject all",
    # Chat widgets — Intercom
    "chat with us", "start a conversation", "send us a message",
    "hi there", "hi! how can we help", "how can we help you",
    "powered by intercom", "intercom",
    # Crisp
    "powered by crisp", "crisp",
    # Zendesk
    "powered by zendesk", "zendesk",
    # Tidio
    "powered by tidio", "tidio",
    # LiveChat / generic
    "live chat", "start chat", "end chat", "minimize chat",
    "chat support", "chat now",
    # Misc dynamic UI
    "close", "close banner", "got it", "ok", "okay", "i understand",
    "learn more", "read more", "show more", "hide",
    "loading", "please wait",
})

# Regex patterns applied to individual lines (after whitespace normalisation).
_NOISE_PATTERNS: list[re.Pattern] = [
    # Lines made up almost entirely of repeated punctuation / symbols
    re.compile(r"^[\W_]{4,}$"),
    # Lines that are just a URL
    re.compile(r"^https?://\S+$"),
    # Lines that look like timestamps / date-time strings
    re.compile(r"^\d{1,2}[:/\-]\d{2}([:/\-]\d{2,4})?(\s+[AP]M)?$", re.IGNORECASE),
    # Lines of repeated single words (the "word word word …" pattern seen in
    # chat-widget injections)
    re.compile(r"^(\b\w+\b\s+)\1{4,}"),
]


def _is_noise_line(line: str) -> bool:
    """Return True if *line* should be discarded before diffing."""
    # Drop lines shorter than 4 whitespace-separated tokens
    words = line.split()
    if len(words) < 4:
        return True

    lower = line.lower()

    # Exact-phrase match
    if lower in _NOISE_PHRASES:
        return True

    # Substring match for multi-word phrases (e.g. "we use cookies" embedded
    # in a longer sentence that is still just consent boilerplate)
    if any(phrase in lower for phrase in _NOISE_PHRASES if len(phrase) > 6):
        return True

    # Regex patterns
    return any(pattern.search(line) for pattern in _NOISE_PATTERNS)


def _filter_noise(text: str) -> str:
    """Remove noise lines from *text* and return the cleaned result."""
    kept = []
    removed = 0
    for line in text.splitlines():
        normalised = re.sub(r"\s+", " ", line).strip()
        if not normalised:
            continue
        if _is_noise_line(normalised):
            removed += 1
            logger.debug("Noise filtered: %r", normalised[:80])
        else:
            kept.append(normalised)
    if removed:
        logger.debug("Noise filter removed %d line(s)", removed)
    return "\n".join(kept)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalise(text: str) -> str:
    """Collapse whitespace and strip blank lines (after noise filtering)."""
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def compute_diff(old_text: str, new_text: str) -> str | None:
    """
    Compare *old_text* and *new_text*.

    Returns a human-readable diff summary string if a meaningful change is
    detected, or None if the content is effectively identical.
    """
    # Strip noise before any comparison — keeps cookie banners, chat widgets,
    # and other dynamic UI strings out of the diff and the changes table.
    old_norm = _normalise(_filter_noise(old_text))
    new_norm = _normalise(_filter_noise(new_text))

    if old_norm == new_norm:
        return None

    old_lines = old_norm.splitlines(keepends=True)
    new_lines = new_norm.splitlines(keepends=True)

    # SequenceMatcher ratio: 0.0 (totally different) → 1.0 (identical)
    matcher = difflib.SequenceMatcher(None, old_lines, new_lines)
    similarity = matcher.ratio()
    change_ratio = 1.0 - similarity

    if change_ratio < CHANGE_THRESHOLD:
        logger.debug(
            "Change ratio %.4f is below threshold %.4f — treating as no change",
            change_ratio,
            CHANGE_THRESHOLD,
        )
        return None

    # Build unified diff for the summary
    diff_lines = list(
        difflib.unified_diff(
            old_lines,
            new_lines,
            fromfile="previous",
            tofile="current",
            lineterm="",
        )
    )

    if not diff_lines:
        return None

    # Truncate if enormous
    truncated = False
    if len(diff_lines) > MAX_DIFF_LINES:
        diff_lines = diff_lines[:MAX_DIFF_LINES]
        truncated = True

    summary_lines = [
        f"Change detected (similarity: {similarity:.1%}, delta: {change_ratio:.1%})",
        f"Lines in previous snapshot: {len(old_lines)}",
        f"Lines in new snapshot:      {len(new_lines)}",
        "",
        "--- Diff (unified) ---",
    ]
    summary_lines.extend(diff_lines)

    if truncated:
        summary_lines.append(f"\n[... diff truncated at {MAX_DIFF_LINES} lines ...]")

    return "\n".join(summary_lines)
