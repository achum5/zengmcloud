// The recap prompts ask the AI to put its WHOLE reply inside one fenced code
// block, so it can be copied in a single click as raw Markdown. If a user
// selects-all instead of using the code block's copy button, they'll paste the
// surrounding ``` fences too. Peel a single wrapping fence off before parsing so
// those stray backticks never end up inside a stored recap. No-op when there's
// no wrapping fence (the copy button already hands back just the inner text).
export const stripOuterCodeFence = (text: string): string => {
	const trimmed = text.trim();
	const open = trimmed.match(/^`{3,}[^\n]*\n/);
	const close = trimmed.match(/\n`{3,}\s*$/);
	if (
		open &&
		close &&
		close.index !== undefined &&
		close.index >= open[0].length
	) {
		return trimmed.slice(open[0].length, close.index);
	}
	return text;
};
