// Handing something off to another person, however the device can. The share
// sheet is the right thing on a phone; the clipboard is the right thing
// everywhere else. Returns what happened so the button can say so.
export const shareOrCopy = async (
	text: string,
): Promise<"shared" | "copied" | "failed"> => {
	if (typeof navigator !== "undefined" && navigator.share) {
		try {
			await navigator.share({ text });
			return "shared";
		} catch {
			// A cancelled share sheet is not a failure - fall through to the
			// clipboard rather than reporting an error the user caused on purpose.
		}
	}
	try {
		await navigator.clipboard.writeText(text);
		return "copied";
	} catch {
		return "failed";
	}
};
