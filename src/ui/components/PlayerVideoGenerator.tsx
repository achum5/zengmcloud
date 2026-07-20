import { useState } from "react";

// One ready-to-copy AI-VIDEO prompt for a real moment in this player's career
// (clutch plays, statistical-feat highlight reels, award tributes, career
// montage), built in the worker with full on-court context.
export type VideoMoment = { key: string; label: string; prompt: string };

// A place to send people to actually generate the video. First is the default.
const VIDEO_TOOLS: { label: string; href: string }[] = [
	{ label: "Sora", href: "https://sora.chatgpt.com/" },
	{ label: "Veo", href: "https://labs.google/fx/tools/flow" },
	{ label: "Runway", href: "https://runwayml.com/" },
];

// Compact AI-video helper for the player editor: pick a moment (or Customize),
// copy the fully-detailed prompt, and open an AI video generator. Mirrors
// PlayerImageGenerator, but there's nothing to upload back onto the player -
// videos aren't stored on the record - so it's copy-and-go.
const PlayerVideoGenerator = ({
	moments,
	customSeed,
}: {
	moments: VideoMoment[];
	customSeed: string;
}) => {
	const [selectedKey, setSelectedKey] = useState(moments[0]?.key ?? "custom");
	const [customPrompt, setCustomPrompt] = useState(customSeed);
	const [copied, setCopied] = useState(false);

	const isCustom = selectedKey === "custom";
	const prompt = isCustom
		? customPrompt
		: (moments.find((m) => m.key === selectedKey)?.prompt ?? "");

	return (
		<div className="border rounded p-2">
			<div className="d-flex flex-wrap align-items-end gap-2">
				<div style={{ flex: "1 1 220px", minWidth: 200 }}>
					<label className="form-label mb-1 small text-body-secondary">
						Generate a highlight video
					</label>
					<select
						className="form-select form-select-sm"
						value={selectedKey}
						onChange={(event) => {
							setSelectedKey(event.target.value);
							setCopied(false);
						}}
					>
						{moments.map((m) => (
							<option key={m.key} value={m.key}>
								{m.label}
							</option>
						))}
						<option value="custom">Customize (write your own)</option>
					</select>
				</div>
				<button
					type="button"
					className="btn btn-secondary btn-sm"
					disabled={!prompt.trim()}
					onClick={async () => {
						try {
							await navigator.clipboard.writeText(prompt);
							setCopied(true);
						} catch {
							setCopied(false);
						}
					}}
				>
					{copied ? "Copied!" : "Copy prompt"}
				</button>
				{VIDEO_TOOLS.map((tool, i) => (
					<a
						key={tool.href}
						className={`btn btn-sm ${i === 0 ? "btn-primary" : "btn-light-bordered"}`}
						href={tool.href}
						target="_blank"
						rel="noreferrer"
					>
						{tool.label}
					</a>
				))}
			</div>

			{/* Show the full prompt for review (it's long and detailed); editable in
			    Customize, read-only for a preset so it's easy to eyeball before copy. */}
			<textarea
				className="form-control form-control-sm mt-2"
				rows={5}
				value={prompt}
				readOnly={!isCustom}
				placeholder="Describe the video…"
				onChange={
					isCustom
						? (event) => {
								setCustomPrompt(event.target.value);
								setCopied(false);
							}
						: undefined
				}
			/>
		</div>
	);
};

export default PlayerVideoGenerator;
