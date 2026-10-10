import { useMemo, useRef, useState, type RefObject } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { helpers } from "../util/helpers.ts";
import { showNotification } from "../util/showNotification.ts";
import type { ScoreBugStyle, View } from "../../common/types.ts";
import { parseScoreBug, SCORE_BUG_PRESETS } from "../../common/scoreBug.ts";
import { PIC, pictureFromFile } from "../components/CourtImageField.tsx";
import { ScoreBug as DefaultBug } from "./LiveGame/court3d/ScoreBug.tsx";
import { CustomScoreBug } from "./LiveGame/court3d/CustomScoreBug.tsx";

const SAMPLE = {
	clock: "2:41",
	shot: "14",
	fouls: ["FOULS 3", "BONUS"] as [string, string],
	ball: 1 as const,
};

// THE LEAGUE'S SCORE BUG for the 3D game: the default, or its own (see
// ScoreBugStyle) - started from a preset, edited as JSON.
const ScoreBugPage = ({
	bug: saved,
	pictures: picturesSaved,
	home,
	away,
}: View<"scoreBug">) => {
	useTitleBar({ title: "Score Bug" });

	const [bug, setBug] = useState<ScoreBugStyle | null>(saved);
	const [text, setText] = useState(saved ? JSON.stringify(saved, null, 2) : "");
	const [error, setError] = useState<string | undefined>();
	const [pictures, setPictures] =
		useState<Record<string, string>>(picturesSaved);
	const [uploaded, setUploaded] = useState<string[]>([]);
	const [saving, setSaving] = useState(false);

	const choose = (next: ScoreBugStyle | null) => {
		setBug(next);
		setText(next ? JSON.stringify(next, null, 2) : "");
		setError(undefined);
	};

	// The bug drawn: its uploaded pictures filled in.
	const shown = useMemo(() => {
		if (!bug) {
			return null;
		}
		const fill = (u: string | undefined) =>
			u?.startsWith(PIC) ? pictures[u.slice(PIC.length)] : u;
		return {
			...bug,
			image: fill(bug.image),
			pieces: bug.pieces.map((p) =>
				p.image === undefined ? p : { ...p, image: fill(p.image) },
			),
		};
	}, [bug, pictures]);

	const refs = {
		clock: useRef<HTMLSpanElement | null>(null),
		shot: useRef<HTMLSpanElement | null>(null),
		fouls: [
			useRef<HTMLSpanElement | null>(null),
			useRef<HTMLSpanElement | null>(null),
		] as [RefObject<HTMLSpanElement | null>, RefObject<HTMLSpanElement | null>],
		ball: [
			useRef<HTMLSpanElement | null>(null),
			useRef<HTMLSpanElement | null>(null),
		] as [RefObject<HTMLSpanElement | null>, RefObject<HTMLSpanElement | null>],
	};
	const sides = {
		away: away && { ...away, pts: 84, timeouts: 3 },
		home: home && { ...home, pts: 87, timeouts: 5 },
	};

	const upload = async (file: File) => {
		try {
			const url = await pictureFromFile(file);
			const id = await toWorker("main", "storeCourtPicture", url);
			setPictures((p) => ({ ...p, [id]: url }));
			setUploaded((u) => [...u, id]);
			if (bug) {
				choose({ ...bug, image: `${PIC}${id}` });
			}
		} catch (error_) {
			showNotification({
				type: "error",
				text: `Could not upload: ${(error_ as Error).message}`,
			});
		}
	};

	const save = async () => {
		setSaving(true);
		try {
			await toWorker("main", "updateScoreBug", { bug, uploaded });
			setUploaded([]);
			showNotification({ type: "success", text: "Score bug saved." });
		} catch (error_) {
			showNotification({
				type: "error",
				text: `Could not save: ${(error_ as Error).message}`,
				persistent: true,
			});
		} finally {
			setSaving(false);
		}
	};

	return (
		<>
			<div
				className="mb-3 rounded"
				style={{
					position: "relative",
					maxWidth: 900,
					aspectRatio: "16 / 9",
					containerType: "inline-size",
					overflow: "hidden",
					background:
						"linear-gradient(#0b0c10 0 38%, #3a2a1e 38% 46%, #c9a165 46%)",
				}}
			>
				{shown ? (
					<CustomScoreBug
						style={shown}
						{...sides}
						quarter="4th"
						totalTimeouts={7}
						refs={refs}
						sample={SAMPLE}
					/>
				) : (
					<DefaultBugPreview sides={sides} />
				)}
			</div>

			<div className="d-flex flex-wrap gap-2 mb-3">
				<button
					type="button"
					className={`btn btn-sm ${bug ? "btn-light-bordered" : "btn-secondary"}`}
					onClick={() => choose(null)}
				>
					Default
				</button>
				{SCORE_BUG_PRESETS.map(({ name, style }) => (
					<button
						key={name}
						type="button"
						className="btn btn-sm btn-light-bordered"
						onClick={() => choose(structuredClone(style))}
					>
						{name}
					</button>
				))}
				{bug ? (
					<>
						<label className="btn btn-sm btn-light-bordered mb-0">
							Background picture
							<input
								type="file"
								accept="image/*"
								className="d-none"
								onChange={(e) => {
									const file = e.target.files?.[0];
									e.target.value = "";
									if (file) {
										void upload(file);
									}
								}}
							/>
						</label>
						{bug.image ? (
							<button
								type="button"
								className="btn btn-sm btn-light-bordered"
								onClick={() => {
									const { image: _, ...rest } = bug;
									choose(rest);
								}}
							>
								Remove picture
							</button>
						) : null}
					</>
				) : null}
			</div>

			{bug ? (
				<div className="mb-3" style={{ maxWidth: 900 }}>
					<textarea
						className="form-control font-monospace small"
						rows={16}
						spellCheck={false}
						value={text}
						onChange={(e) => setText(e.target.value)}
					/>
					{error ? <div className="text-danger small mt-1">{error}</div> : null}
					<button
						type="button"
						className="btn btn-sm btn-secondary mt-2"
						onClick={() => {
							let raw: unknown;
							try {
								raw = JSON.parse(text);
							} catch {
								setError("Not valid JSON.");
								return;
							}
							const parsed = parseScoreBug(raw);
							if (typeof parsed === "string") {
								setError(parsed);
							} else {
								setError(undefined);
								setBug(parsed);
							}
						}}
					>
						Apply
					</button>
				</div>
			) : null}

			<div className="d-flex gap-2">
				<button
					type="button"
					className="btn btn-primary"
					onClick={save}
					disabled={saving}
				>
					Save score bug
				</button>
				<a
					className="btn btn-light-bordered"
					href={helpers.leagueUrl(["manage_teams"])}
				>
					Back
				</a>
			</div>
		</>
	);
};

// The default bug, with the sample written in.
const DefaultBugPreview = ({
	sides,
}: {
	sides: Pick<Parameters<typeof DefaultBug>[0], "away" | "home">;
}) => {
	const clock = useRef<HTMLSpanElement | null>(null);
	const shot = useRef<HTMLSpanElement | null>(null);
	const f0 = useRef<HTMLSpanElement | null>(null);
	const f1 = useRef<HTMLSpanElement | null>(null);
	const b0 = useRef<HTMLSpanElement | null>(null);
	const b1 = useRef<HTMLSpanElement | null>(null);
	const set = () => {
		if (clock.current) {
			clock.current.textContent = SAMPLE.clock;
		}
		if (shot.current) {
			shot.current.textContent = SAMPLE.shot;
			shot.current.style.display = "flex";
		}
		[f0, f1].forEach((r, i) => {
			if (r.current) {
				r.current.textContent = SAMPLE.fouls[i]!;
			}
		});
		[b0, b1].forEach((r, i) => {
			if (r.current) {
				r.current.style.visibility = i === SAMPLE.ball ? "visible" : "hidden";
			}
		});
	};
	return (
		<div ref={() => set()}>
			<DefaultBug
				{...sides}
				quarter="4th"
				totalTimeouts={7}
				refs={{ clock, shot, fouls: [f0, f1], ball: [b0, b1] }}
			/>
		</div>
	);
};

export default ScoreBugPage;
