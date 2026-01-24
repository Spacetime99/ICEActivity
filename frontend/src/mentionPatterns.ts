type TripletLike = {
  what?: string | null;
  title?: string | null;
  who?: string | null;
};

export const buildTripletBlob = (triplet: TripletLike): string =>
  [triplet.what, triplet.title, triplet.who].filter(Boolean).join(" ");

export const matchesPatterns = (text: string, patterns: RegExp[]): boolean =>
  patterns.some((pattern) => pattern.test(text));

export const CHILD_PATTERNS = [
  /\bchild(ren)?\b/i,
  /\bminor(s)?\b/i,
  /\bjuvenile(s)?\b/i,
  /\bteen\b/i,
  /\bteenager(s)?\b/i,
  /\byouth\b/i,
  /\binfant(s)?\b/i,
  /\bbaby\b/i,
  /\bbabies\b/i,
  /\btoddler(s)?\b/i,
  /\bkid(s)?\b/i,
  /\bunaccompanied\b/i,
  /\b([1-9]|1[0-7])[- ]year[- ]old\b/i,
];

export const US_STATUS_PATTERNS = [
  /\bU\.?S\.?\s+(citizen|citizens|national|nationals|born)\b/i,
  /\bUnited States\s+(citizen|citizens|national|nationals)\b/i,
  /\bAmerican\s+citizen(s)?\b/i,
  /\bbirthright\s+citizen(s)?\b/i,
  /\bbirthright\s+citizenship\b/i,
  /\bcitizen(s)?\s+by\s+birth\b/i,
  /\bnative[- ]born\s+citizen(s)?\b/i,
  /\bborn\s+in\s+the\s+U\.?S\.?\b/i,
  /\bborn\s+in\s+the\s+United\s+States\b/i,
  /\bborn\s+in\s+America\b/i,
  /\bU\.?S\.?-?born\b/i,
  /\bnaturalized\s+(citizen|citizens|american|americans)\b/i,
  /\bU\.?S\.?\s+naturalized\b/i,
  /\bnaturalization\b/i,
  /\bnatural[- ]born\s+citizen(s)?\b/i,
  /\bgreen\s+card\s+holder(s)?\b/i,
  /\blawful\s+permanent\s+resident(s)?\b/i,
  /\bpermanent\s+resident(s)?\b/i,
  /\bLPRs?\b/i,
];

export const isChildMention = (text: string): boolean =>
  matchesPatterns(text, CHILD_PATTERNS);

export const isUsStatusMention = (text: string): boolean =>
  matchesPatterns(text, US_STATUS_PATTERNS);
