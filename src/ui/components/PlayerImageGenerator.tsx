import { useState } from "react";
import ImageUploader from "./ImageUploader.tsx";

// One ready-to-copy prompt for a real moment in this player's career (built in
// the worker from draft/trades/awards/big games). All prompts are cartoon
// faces.js-style, matching the worker's getPlayerImageMoments.
export type ImageMoment = { key: string; label: string; prompt: string };

// Compact AI-image helper for the player editor: pick a moment (or Customize),
// copy the prompt, open ChatGPT, generate, then paste/upload the result. The
// full prompt text only shows for Customize; presets just expose a Copy button.
const PlayerImageGenerator = ({
	moments,
	customSeed,
	onImageUploaded,
}: {
	moments: ImageMoment[];
	customSeed: string;
	onImageUploaded: (url: string) => void;
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
						Generate a player image
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
				<a
					className="btn btn-primary btn-sm"
					href="https://chatgpt.com/"
					target="_blank"
					rel="noreferrer"
				>
					ChatGPT
				</a>
			</div>

			{isCustom ? (
				<textarea
					className="form-control form-control-sm mt-2"
					rows={3}
					value={customPrompt}
					placeholder="Describe the image (kept in cartoon style)…"
					onChange={(event) => {
						setCustomPrompt(event.target.value);
						setCopied(false);
					}}
				/>
			) : null}

			<div className="mt-2">
				<ImageUploader onUploaded={onImageUploaded} />
			</div>
		</div>
	);
};

export default PlayerImageGenerator;
