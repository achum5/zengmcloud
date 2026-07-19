import { useState } from "react";
import ImageUploader from "./ImageUploader.tsx";

// The "same flow" as the game-recap AI helper: the app builds a text prompt you
// take to an image model (ChatGPT etc.), then you paste the generated image
// back and it's uploaded to imgbb. Prompt text only - no images are sent for
// you; attach any reference screenshots yourself in the chat.
type Ctx = { name: string; pos: string; team: string };

const SCENARIOS: {
	key: string;
	label: string;
	build: (ctx: Ctx) => string;
}[] = [
	{
		key: "draft",
		label: "Draft night",
		build: ({ name, pos, team }) =>
			`Photorealistic image of ${name}, a professional basketball ${pos}, on draft night: smiling on stage shaking the commissioner's hand, wearing the ${team} team cap and holding up a ${team} jersey with their name on the back. Bright arena lighting, celebratory mood.`,
	},
	{
		key: "postgame",
		label: "Post-game interview",
		build: ({ name, pos, team }) =>
			`Photorealistic image of ${name}, a professional basketball ${pos}, at a post-game press conference in a ${team} uniform, towel around the neck, seated at a podium with microphones.`,
	},
	{
		key: "action",
		label: "In-game action",
		build: ({ name, pos, team }) =>
			`Photorealistic action shot of ${name}, a professional basketball ${pos}, driving to the basket in a ${team} uniform during a packed arena game.`,
	},
	{
		key: "portrait",
		label: "Studio portrait",
		build: ({ name, pos, team }) =>
			`Photorealistic studio headshot of ${name}, a professional basketball ${pos}, in a ${team} uniform against a neutral backdrop.`,
	},
	{ key: "custom", label: "Custom", build: () => "" },
];

const PlayerImageGenerator = ({
	ctx,
	onImageUploaded,
}: {
	ctx: Ctx;
	onImageUploaded: (url: string) => void;
}) => {
	const [open, setOpen] = useState(false);
	const [scenarioKey, setScenarioKey] = useState("draft");
	const [prompt, setPrompt] = useState(() => SCENARIOS[0]!.build(ctx));
	const [copied, setCopied] = useState(false);

	const applyScenario = (key: string) => {
		setScenarioKey(key);
		const scenario = SCENARIOS.find((s) => s.key === key);
		if (scenario && key !== "custom") {
			setPrompt(scenario.build(ctx));
		}
	};

	if (!open) {
		return (
			<button
				type="button"
				className="btn btn-link p-0"
				onClick={() => setOpen(true)}
			>
				Generate an image with AI…
			</button>
		);
	}

	return (
		<div className="border rounded p-3 mt-2">
			<div className="d-flex justify-content-between align-items-center mb-2">
				<b>AI image generator</b>
				<button
					type="button"
					className="btn-close"
					aria-label="Close"
					onClick={() => setOpen(false)}
				/>
			</div>
			<div className="mb-2" style={{ maxWidth: 220 }}>
				<label className="form-label mb-1">Scenario</label>
				<select
					className="form-select form-select-sm"
					value={scenarioKey}
					onChange={(event) => applyScenario(event.target.value)}
				>
					{SCENARIOS.map((s) => (
						<option key={s.key} value={s.key}>
							{s.label}
						</option>
					))}
				</select>
			</div>
			<div className="mb-2">
				<label className="form-label mb-1">Prompt</label>
				<textarea
					className="form-control"
					rows={4}
					value={prompt}
					onChange={(event) => {
						setPrompt(event.target.value);
						setCopied(false);
					}}
				/>
			</div>
			<div className="d-flex gap-2 mb-3">
				<button
					type="button"
					className="btn btn-secondary btn-sm"
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
					Open ChatGPT
				</a>
			</div>
			<label className="form-label mb-1">Paste the generated image</label>
			<ImageUploader
				onUploaded={(url) => {
					onImageUploaded(url);
					setOpen(false);
				}}
			/>
		</div>
	);
};

export default PlayerImageGenerator;
