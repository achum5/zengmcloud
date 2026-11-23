import { useMemo, useRef, useState, type RefObject } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { helpers } from "../util/helpers.ts";
import { showNotification } from "../util/showNotification.ts";
import type { ScoreBugStyle, View } from "../../common/types.ts";
import {
	FOOTBALL_SCORE_BUG_PRESETS,
	SCORE_BUG_PRESETS,
} from "../../common/scoreBug.ts";
import { PIC, pictureFromFile } from "../components/CourtImageField.tsx";
import { ScoreBug as DefaultBug } from "./LiveGame/court3d/ScoreBug.tsx";
import { CustomScoreBug } from "./LiveGame/court3d/CustomScoreBug.tsx";
import {
	fontFromFile,
	keepScoreBugPictures,
	parseScoreBugText,
} from "./scoreBugFile.ts";

const FOOTBALL = __SPORT === "football";
const SAMPLE = FOOTBALL
	? {
			clock: "2:41",
			shot: "",
			low: false,
			fouls: ["", ""] as [string, string],
			ball: 1 as const,
			down: "3rd & 7",
		}
	: {
			clock: "2:41",
			shot: "14",
			low: false,
			fouls: ["FOULS 5", "BONUS"] as [string, string],
			counts: [5, 2] as [number, number],
			bonus: [false, true] as [boolean, boolean],
			ball: 1 as const,
		};
const PRESETS = FOOTBALL ? FOOTBALL_SCORE_BUG_PRESETS : SCORE_BUG_PRESETS;
const TIMEOUTS = FOOTBALL ? 3 : 7;

// A SCORE BUG for the 3D game - the league's, or (`tid`) a team's, its
// broadcast's: the default (for a team, the league's), or its own (see
// ScoreBugStyle) - started from a preset or a file, edited as JSON.
const ScoreBugPage = ({
	tid,
	teamName,
	league,
	bug: saved,
	pictures: picturesSaved,
	home,
	away,
}: View<"scoreBug">) => {
	useTitleBar({ title: teamName ? `${teamName} Score Bug` : "Score Bug" });

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

	// The bug drawn: its uploaded pictures and fonts filled in (a team's
	// without one of its own: the league's).
	const shown = useMemo(() => {
		if (!bug) {
			return league;
		}
		const fill = (u: string | undefined) =>
			u?.startsWith(PIC) ? pictures[u.slice(PIC.length)] : u;
		return {
			...bug,
			image: fill(bug.image),
			...(bug.fonts
				? {
						fonts: bug.fonts.flatMap((f) => {
							const src = fill(f.src);
							return src ? [{ ...f, src }] : [];
						}),
					}
				: {}),
			pieces: bug.pieces.map((p) =>
				p.image === undefined ? p : { ...p, image: fill(p.image) },
			),
		};
	}, [bug, league, pictures]);

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
		bonus: [
			useRef<HTMLSpanElement | null>(null),
			useRef<HTMLSpanElement | null>(null),
		] as [RefObject<HTMLSpanElement | null>, RefObject<HTMLSpanElement | null>],
		down: useRef<HTMLSpanElement | null>(null),
		bug: useRef<HTMLDivElement | null>(null),
	};
	const sides = FOOTBALL
		? {
				away: away && { ...away, pts: 21, timeouts: 1, record: "9-5" },
				home: home && { ...home, pts: 24, timeouts: 2, record: "11-3" },
			}
		: {
				away: away && { ...away, pts: 84, timeouts: 3, record: "38-21" },
				home: home && { ...home, pts: 87, timeouts: 5, record: "44-15" },
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

	const uploadFont = async (file: File) => {
		if (!bug) {
			return;
		}
		try {
			const { name, url } = await fontFromFile(file);
			const id = await toWorker("main", "storeScoreBugFont", url);
			setPictures((p) => ({ ...p, [id]: url }));
			setUploaded((u) => [...u, id]);
			choose({
				...bug,
				font: bug.font ?? name,
				fonts: [
					...(bug.fonts ?? []).filter((f) => f.name !== name),
					{ name, src: `${PIC}${id}` },
				],
			});
		} catch (error_) {
			showNotification({
				type: "error",
				text: `Could not upload: ${(error_ as Error).message}`,
			});
		}
	};

	// A bug from a file keeps its pictures and fonts in the league, as
	// uploads do.
	const loadFile = async (file: File) => {
		const parsed = parseScoreBugText(await file.text());
		if (typeof parsed === "string") {
			showNotification({ type: "error", text: parsed });
			return;
		}
		const kept = await keepScoreBugPictures(parsed);
		setPictures((p) => ({ ...p, ...kept.pictures }));
		setUploaded((u) => [...u, ...kept.ids]);
		choose(kept.bug);
	};

	const save = async () => {
		setSaving(true);
		try {
			await toWorker("main", "updateScoreBug", { tid, bug, uploaded });
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
						quarter="Q4"
						totalTimeouts={TIMEOUTS}
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
					{tid === undefined ? "Default" : "League's"}
				</button>
				{PRESETS.map(({ name, style }) => (
					<button
						key={name}
						type="button"
						className="btn btn-sm btn-light-bordered"
						onClick={() => choose(structuredClone(style))}
					>
						{name}
					</button>
				))}
				<label className="btn btn-sm btn-light-bordered mb-0">
					Load file
					<input
						type="file"
						accept=".json,application/json"
						className="d-none"
						onChange={(e) => {
							const file = e.target.files?.[0];
							e.target.value = "";
							if (file) {
								loadFile(file).catch(() => {
									showNotification({
										type: "error",
										text: "Could not read that file.",
									});
								});
							}
						}}
					/>
				</label>
				{bug ? (
					<>
						<label className="btn btn-sm btn-light-bordered mb-0">
							Font
							<input
								type="file"
								accept=".woff2,.woff,.ttf,.otf"
								className="d-none"
								onChange={(e) => {
									const file = e.target.files?.[0];
									e.target.value = "";
									if (file) {
										void uploadFont(file);
									}
								}}
							/>
						</label>
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
							const parsed = parseScoreBugText(text);
							if (typeof parsed === "string") {
								setError(parsed);
							} else {
								setError(undefined);
								void keepScoreBugPictures(parsed).then((kept) => {
									setPictures((p) => ({ ...p, ...kept.pictures }));
									setUploaded((u) => [...u, ...kept.ids]);
									setBug(kept.bug);
									if (kept.ids.length > 0) {
										setText(JSON.stringify(kept.bug, null, 2));
									}
								});
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
	const down = useRef<HTMLSpanElement | null>(null);
	const set = () => {
		if (clock.current) {
			clock.current.textContent = SAMPLE.clock;
		}
		if (shot.current && SAMPLE.shot) {
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
		if (SAMPLE.down !== undefined && down.current) {
			down.current.textContent = SAMPLE.down;
		}
	};
	return (
		<div ref={() => set()}>
			<DefaultBug
				{...sides}
				quarter="Q4"
				totalTimeouts={TIMEOUTS}
				refs={{
					clock,
					shot,
					fouls: [f0, f1],
					ball: [b0, b1],
					...(FOOTBALL ? { down } : {}),
				}}
			/>
		</div>
	);
};

// (A page a team, a fresh one - not the last team's bug in it.)
export default (props: View<"scoreBug">) => (
	<ScoreBugPage key={props.tid ?? "league"} {...props} />
);
