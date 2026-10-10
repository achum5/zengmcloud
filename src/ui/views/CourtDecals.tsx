import { useMemo, useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { helpers } from "../util/helpers.ts";
import { showNotification } from "../util/showNotification.ts";
import type {
	CourtDecal,
	CourtDecalPlaced,
	CourtImageAdjust,
	View,
} from "../../common/types.ts";
import { parseCourtDecals } from "../../common/courtDecals.ts";
import LiveCourt, { type CourtTeam } from "./LiveGame/LiveCourt.tsx";
import {
	ImageField,
	PIC,
	pictureFromFile,
} from "../components/CourtImageField.tsx";

const WHEN: [CourtDecal["when"], string][] = [
	["always", "Every game"],
	["openingNight", "Opening night"],
	["playoffs", "Playoffs"],
	["finals", "Finals"],
];

// THE LEAGUE'S COURT DECALS: pictures laid on every court on an occasion,
// in the seasons given (see CourtDecal).
const CourtDecals = ({
	decals: saved,
	pictures: picturesSaved,
	season,
	team,
}: View<"courtDecals">) => {
	useTitleBar({ title: "Court Decals" });

	const [decals, setDecals] = useState<CourtDecal[]>(saved);
	const [pictures, setPictures] =
		useState<Record<string, string>>(picturesSaved);
	const [uploaded, setUploaded] = useState<string[]>([]);
	const [preview, setPreview] = useState<CourtDecal["when"]>("always");
	const [saving, setSaving] = useState(false);
	const [json, setJSON] = useState<string | undefined>();
	const [jsonError, setJSONError] = useState<string | undefined>();

	const upload = async (file: File) => {
		try {
			const url = await pictureFromFile(file);
			const id = await toWorker("main", "storeCourtPicture", url);
			setPictures((p) => ({ ...p, [id]: url }));
			setUploaded((u) => [...u, id]);
			return `${PIC}${id}`;
		} catch (error) {
			showNotification({
				type: "error",
				text: `Could not upload: ${(error as Error).message}`,
			});
			return undefined;
		}
	};

	const change = (i: number, next: Partial<CourtDecal>) => {
		setDecals((ds) =>
			ds.map((d, j) => {
				if (j !== i) {
					return d;
				}
				const out: CourtDecal = { ...d, ...next };
				for (const key of Object.keys(next) as (keyof CourtDecal)[]) {
					if (next[key] === undefined) {
						delete out[key];
					}
				}
				return out;
			}),
		);
	};

	// The court shown: the user's team's, with the decals laid on for the
	// occasion previewed (whatever their seasons).
	const shown = useMemo((): CourtDecalPlaced[] => {
		const out: CourtDecalPlaced[] = [];
		for (const d of decals) {
			if (
				d.when !== "always" &&
				d.when !== preview &&
				!(preview === "finals" && d.when === "playoffs")
			) {
				continue;
			}
			const href = d.image.startsWith(PIC)
				? pictures[d.image.slice(PIC.length)]
				: d.image;
			if (href) {
				out.push({ href, adjust: d.adjust, pair: d.pair });
			}
		}
		return out;
	}, [decals, pictures, preview]);

	const save = async () => {
		setSaving(true);
		try {
			await toWorker("main", "updateCourtDecals", { decals, uploaded });
			setUploaded([]);
			showNotification({ type: "success", text: "Decals saved." });
		} catch (error) {
			showNotification({
				type: "error",
				text: `Could not save decals: ${(error as Error).message}`,
				persistent: true,
			});
		} finally {
			setSaving(false);
		}
	};

	const homeTeam: CourtTeam = team
		? { ...team, court: { ...team.court, decals: shown } }
		: { tid: -1, court: { decals: shown } };

	return (
		<div className="row">
			<div className="col-lg-7 mb-3">
				<LiveCourt
					scene={undefined}
					teams={[undefined, homeTeam]}
					finals={preview === "finals"}
					season={undefined}
					sceneMs={undefined}
				/>
				<div className="btn-group btn-group-sm">
					{WHEN.map(([when, label]) => (
						<button
							key={when}
							type="button"
							className={`btn ${preview === when ? "btn-secondary" : "btn-light-bordered"}`}
							onClick={() => setPreview(when)}
						>
							{label}
						</button>
					))}
				</div>
			</div>

			<div className="col-lg-5">
				{decals.map((d, i) => (
					<div key={i} className="border rounded p-2 mb-3">
						<ImageField
							label={`Decal ${i + 1}`}
							slot={`decal${i}`}
							url={d.image}
							onURL={(v) => change(i, { image: v })}
							adjust={d.adjust}
							onAdjust={(_, next: CourtImageAdjust | undefined) =>
								change(i, { adjust: next })
							}
							pictures={pictures}
							onUpload={upload}
						/>
						<div className="d-flex flex-wrap align-items-center gap-2">
							<select
								className="form-select form-select-sm w-auto"
								value={d.when}
								onChange={(e) =>
									change(i, { when: e.target.value as CourtDecal["when"] })
								}
							>
								{WHEN.map(([when, label]) => (
									<option key={when} value={when}>
										{label}
									</option>
								))}
							</select>
							{(["from", "to"] as const).map((key) => (
								<input
									key={key}
									type="number"
									className="form-control form-control-sm"
									style={{ width: "6.5rem" }}
									placeholder={key === "from" ? "From season" : "To season"}
									value={d[key] ?? ""}
									onChange={(e) =>
										change(i, {
											[key]:
												e.target.value === ""
													? undefined
													: Number.parseInt(e.target.value),
										})
									}
								/>
							))}
							<div className="form-check mb-0">
								<input
									className="form-check-input"
									type="checkbox"
									id={`decal${i}-pair`}
									checked={d.pair ?? false}
									onChange={(e) =>
										change(i, { pair: e.target.checked ? true : undefined })
									}
								/>
								<label
									className="form-check-label small"
									htmlFor={`decal${i}-pair`}
								>
									One on each half
								</label>
							</div>
							<button
								type="button"
								className="btn btn-sm btn-light-bordered ms-auto"
								onClick={() => setDecals((ds) => ds.filter((_, j) => j !== i))}
							>
								Remove
							</button>
						</div>
					</div>
				))}

				<button
					type="button"
					className="btn btn-light-bordered mb-3"
					onClick={() =>
						setDecals((ds) => [
							...ds,
							{
								image: "",
								when: preview,
								from: season,
							},
						])
					}
				>
					Add decal
				</button>

				<div>
					<button
						type="button"
						className="btn btn-sm btn-light-bordered"
						onClick={() => {
							setJSONError(undefined);
							setJSON(
								json === undefined
									? JSON.stringify(decals, null, 2)
									: undefined,
							);
						}}
					>
						{json === undefined ? "Edit as JSON" : "Close JSON"}
					</button>
					{json !== undefined ? (
						<div className="mt-2">
							<textarea
								className="form-control font-monospace small"
								rows={12}
								spellCheck={false}
								value={json}
								onChange={(e) => setJSON(e.target.value)}
							/>
							{jsonError ? (
								<div className="text-danger small mt-1">{jsonError}</div>
							) : null}
							<button
								type="button"
								className="btn btn-sm btn-secondary mt-2"
								onClick={() => {
									let raw: unknown;
									try {
										raw = JSON.parse(json);
									} catch {
										setJSONError("Not valid JSON.");
										return;
									}
									const parsed = parseCourtDecals(raw);
									if (typeof parsed === "string") {
										setJSONError(parsed);
									} else {
										setJSONError(undefined);
										setDecals(parsed);
									}
								}}
							>
								Apply
							</button>
						</div>
					) : null}
				</div>

				<div className="d-flex gap-2 mt-3">
					<button
						type="button"
						className="btn btn-primary"
						onClick={save}
						disabled={saving || decals.some((d) => d.image === "")}
					>
						Save decals
					</button>
					<a
						className="btn btn-light-bordered"
						href={helpers.leagueUrl(["manage_teams"])}
					>
						Back
					</a>
				</div>
			</div>
		</div>
	);
};

export default CourtDecals;
