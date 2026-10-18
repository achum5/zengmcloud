// A delta's chunks read over plain HTTPS (Firestore's REST API) instead of the
// SDK's streaming channel.
//
// Every catch-up stall in the field captures has the same shape: one read on
// the SDK channel never comes back, the walk sits on it for the full timeout,
// and the cure is tearing the channel down. A plain request does not ride
// that channel, so it can fetch the same document while the channel is stuck.
// Committed delta chunks are immutable, so either copy is the same data.
//
// Same security rules as the SDK: the request carries the signed-in user's ID
// token, and control docs are readable by any signed-in user.

type RestValue = {
	stringValue?: string;
	integerValue?: string;
	doubleValue?: number;
	booleanValue?: boolean;
	nullValue?: null;
};

// The handful of field types a delta chunk carries, as plain values. Anything
// else (timestamps, maps) is left out - nothing here reads them.
export const fromRestFields = (doc: unknown): Record<string, unknown> => {
	const fields = (doc as { fields?: Record<string, RestValue> } | undefined)
		?.fields;
	const out: Record<string, unknown> = {};
	if (!fields || typeof fields !== "object") {
		return out;
	}
	for (const [key, value] of Object.entries(fields)) {
		if (typeof value?.stringValue === "string") {
			out[key] = value.stringValue;
		} else if (typeof value?.integerValue === "string") {
			out[key] = Number(value.integerValue);
		} else if (typeof value?.doubleValue === "number") {
			out[key] = value.doubleValue;
		} else if (typeof value?.booleanValue === "boolean") {
			out[key] = value.booleanValue;
		} else if (value && "nullValue" in value) {
			out[key] = null;
		}
	}
	return out;
};

export type RestDelta = {
	serialized: string;
	authorId: string;
	action: string;
	at: number;
};

// Undefined for anything short of a whole, consistent payload: a missing
// chunk, a refused request, chunks from two different writers. The caller
// treats that as "no rescue" and goes on waiting for the SDK read.
export const fetchDeltaOverRest = async ({
	projectId,
	code,
	token,
	version,
	docId,
	fetchFn = fetch,
	signal,
}: {
	projectId: string;
	code: string;
	token: string;
	version: number;
	docId: (version: number, index: number) => string;
	fetchFn?: typeof fetch;
	signal?: AbortSignal;
}): Promise<RestDelta | undefined> => {
	const base = `https://firestore.googleapis.com/v1/projects/${encodeURIComponent(
		projectId,
	)}/databases/(default)/documents/leagues/${encodeURIComponent(code)}/control/`;
	const get = async (index: number) => {
		const response = await fetchFn(base + docId(version, index), {
			headers: { Authorization: `Bearer ${token}` },
			signal,
		});
		if (!response.ok) {
			return undefined;
		}
		return fromRestFields(await response.json());
	};

	const head = await get(0);
	if (
		!head ||
		typeof head.data !== "string" ||
		typeof head.chunkCount !== "number" ||
		!(head.chunkCount >= 1) ||
		(head.version !== undefined && head.version !== version)
	) {
		return undefined;
	}
	const rest = await Promise.all(
		Array.from({ length: head.chunkCount - 1 }, (_, i) => get(i + 1)),
	);
	let serialized = head.data;
	for (const chunk of rest) {
		// Same agreement the SDK read demands: one writer, one chunk count.
		if (
			!chunk ||
			typeof chunk.data !== "string" ||
			chunk.authorId !== head.authorId ||
			chunk.chunkCount !== head.chunkCount
		) {
			return undefined;
		}
		serialized += chunk.data;
	}
	return {
		serialized,
		authorId: typeof head.authorId === "string" ? head.authorId : "",
		action: typeof head.action === "string" ? head.action : "",
		at: typeof head.at === "number" ? head.at : 0,
	};
};
