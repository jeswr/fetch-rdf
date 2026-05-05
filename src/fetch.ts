/**
 * fetchRdf — GET an RDF resource and parse the body in one call.
 *
 * Wraps the caller's `fetch` (default `globalThis.fetch`), forces a
 * Solid-appropriate `Accept` header, and streams the response body
 * straight into the parser so large responses don't have to be
 * buffered into a single string.
 *
 * @packageDocumentation
 */

import { parseRdf } from './parse.js';
import { RdfFetchError } from './errors.js';
import type { FetchRdfOptions, FetchedRdf } from './types.js';

/**
 * `Accept` header sent on every request. Turtle preferred (q=1.0
 * implicit) with JSON-LD as a fallback, matching Solid Protocol §5.2
 * (the only two RDF media types Solid servers must support). Not a
 * caller-tunable option: `fetchRdf` only knows how to parse what's in
 * {@link SUPPORTED_RDF_MEDIA_TYPES}, so any other `Accept` would be
 * inviting a parse failure.
 */
const ACCEPT = 'text/turtle, application/ld+json;q=0.9';

/**
 * Fetch an RDF resource and parse it.
 *
 * @param url - The resource URL.
 * @param options - Optional. See {@link FetchRdfOptions}.
 * @returns A {@link FetchedRdf} containing the parsed dataset and the
 *   response headers. ETag, Content-Type, Link, etc. all live on
 *   `headers` rather than as separate fields, so the surface stays
 *   tight.
 * @throws {@link RdfFetchError} on transport failure, a non-2xx
 *   response, or parse failure. The error's `status`, `url`, and
 *   `contentType` fields are populated where known so callers can
 *   branch without string-matching the message.
 */
export async function fetchRdf(
  url: string,
  options: FetchRdfOptions = {},
): Promise<FetchedRdf> {
  const fetchImpl = options.fetch ?? globalThis.fetch;

  const headers = new Headers(options.headers);
  // Always force the canonical Solid `Accept`; any caller-supplied
  // `accept` in `headers` is ignored.
  headers.set('accept', ACCEPT);

  let response: Response;
  try {
    response = await fetchImpl(url, {
      headers,
      ...(options.signal !== undefined && { signal: options.signal }),
    });
  } catch (cause) {
    throw new RdfFetchError(
      `Network error fetching ${url}: ${errorMessage(cause)}`,
      { cause, url },
    );
  }

  if (!response.ok) {
    throw new RdfFetchError(
      `HTTP ${response.status} ${response.statusText || ''} fetching ${url}.`.trim(),
      {
        status: response.status,
        url: response.url || url,
        contentType: response.headers.get('content-type') ?? undefined,
      },
    );
  }

  const rawContentType = response.headers.get('content-type');
  const finalUrl = response.url || url;
  const body = response.body;

  if (body === null) {
    // No body — parse the empty string so we still return a (empty)
    // dataset rather than blowing up. The Content-Type still
    // determines whether the format is supported.
    const dataset = await parseRdf('', rawContentType, { baseIRI: finalUrl });
    return { dataset, headers: response.headers };
  }

  const dataset = await parseRdf(body, rawContentType, { baseIRI: finalUrl });
  return { dataset, headers: response.headers };
}

function errorMessage(cause: unknown): string {
  if (cause instanceof Error) return cause.message;
  return String(cause);
}
