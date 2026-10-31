import { lazy, Suspense, useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { helpers } from "../util/helpers.ts";
import { showNotification } from "../util/showNotification.ts";
import type { HelmetLook, StadiumLook, View } from "../../common/types.ts";
import { PIC, pictureFromFile } from "../components/CourtImageField.tsx";
import {
	BOARD_TEXT_MAX,
	cleanHelmet,
	cleanStadium,
	END_ZONE_TEXT_MAX,
} from "../../common/stadiumLook.ts";
import { toHex } from "./LiveGame/field3d/color.ts";
import { fieldStyleFor } from "./LiveGame/field3d/fieldPaint.ts";
import { fieldKit } from "./LiveGame/field3d/kit.ts";
import { stadiumStyleFor } from "./LiveGame/field3d/stadium.ts";

const StadiumPreview = lazy(
	() => import("./LiveGame/field3d/StadiumPreview.tsx"),
);

// A stadium and uniform as JSON (the page's own, or a file): each setting
// kept to what it can be - or what is wrong with it.
const parseLook = (
	text: string,
): { stadium: StadiumLook; helmet: HelmetLook } | string => {
	let raw: unknown;
	try {
		raw = JSON.parse(text);
	} catch {
		return "Not valid JSON.";
	}
	if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
		return "Expected a JSON object.";
	}
	const r = raw as Record<string, unknown>;
	return {
		stadium: cleanStadium(r.stadium) ?? {},
		helmet: cleanHelmet(r.helmet) ?? {},
	};
};

// A setting picked from a few, "" for the team's own pick.
const Choice = <K extends "roof" | "turf" | "endZones" | "sky">({
	label,
	value,
	options,
	onChange,
}: {
	label: string;
	value: StadiumLook[K];
	options: [NonNullable<StadiumLook[K]>, string][];
	onChange: (value: StadiumLook[K]) => void;
}) => (
	<div className="mb-3">
		<label className="form-label mb-1">{label}</label>
		<select
			className="form-select"
			value={value ?? ""}
			onChange={(e) =>
				onChange(
					e.target.value === ""
						? undefined
						: (e.target.value as StadiumLook[K]),
				)
			}
		>
			<option value="">Auto</option>
			{options.map(([v, text]) => (
				<option key={v} value={v}>
					{text}
				</option>
			))}
		</select>
	</div>
);

// A picture of the team's own - a link, or one uploaded - in place of what
// the 3D game picks.
const PictureField = ({
	label,
	url,
	pictures,
	onChange,
	onUpload,
}: {
	label: string;
	url: string | undefined;
	pictures: Record<string, string>;
	onChange: (value: string | undefined) => void;
	onUpload: (file: File) => Promise<string | undefined>;
}) => {
	const uploaded = url?.startsWith(PIC) ?? false;
	const shown = uploaded ? pictures[url!.slice(PIC.length)] : url;
	const [busy, setBusy] = useState(false);
	return (
		<div className="mb-3">
			<label className="form-label mb-1">{label}</label>
			<div className="d-flex align-items-center gap-2">
				{shown ? (
					<img
						src={shown}
						alt=""
						style={{ width: 31, height: 31, objectFit: "contain" }}
					/>
				) : null}
				<input
					type="text"
					className="form-control"
					placeholder="URL"
					value={uploaded ? "Uploaded" : (url ?? "")}
					disabled={uploaded}
					onChange={(e) =>
						onChange(e.target.value === "" ? undefined : e.target.value)
					}
				/>
				<label
					className={`btn btn-sm btn-light-bordered mb-0${busy ? " disabled" : ""}`}
				>
					Upload
					<input
						type="file"
						accept="image/*"
						hidden
						disabled={busy}
						onChange={(e) => {
							const file = e.target.files?.[0];
							e.target.value = "";
							if (!file) {
								return;
							}
							setBusy(true);
							void onUpload(file).then((value) => {
								setBusy(false);
								if (value !== undefined) {
									onChange(value);
								}
							});
						}}
					/>
				</label>
				<button
					type="button"
					className="btn btn-sm btn-light-bordered"
					onClick={() => onChange(undefined)}
					disabled={url === undefined}
				>
					Auto
				</button>
			</div>
		</div>
	);
};

// A helmet color: the team's own pick until one is chosen - and, for a
// stripe, none at all.
const HelmetColor = ({
	label,
	value,
	fallback,
	onChange,
	none,
}: {
	label: string;
	value: string | undefined;
	fallback: string;
	onChange: (value: string | undefined) => void;
	none?: boolean;
}) => (
	<div className="mb-3">
		<label className="form-label mb-1">{label}</label>
		<div className="d-flex align-items-center gap-2">
			<input
				type="color"
				className="form-control form-control-color"
				value={value || fallback}
				disabled={value === ""}
				onChange={(e) => onChange(e.target.value)}
			/>
			<button
				type="button"
				className="btn btn-sm btn-light-bordered"
				onClick={() => onChange(undefined)}
				disabled={value === undefined}
			>
				Auto
			</button>
			{none ? (
				<button
					type="button"
					className="btn btn-sm btn-light-bordered"
					onClick={() => onChange("")}
					disabled={value === ""}
				>
					None
				</button>
			) : null}
		</div>
	</div>
);

const EditTeamStadium = ({
	tid,
	abbrev,
	region,
	name,
	colors,
	imgURL,
	stadium: savedStadium,
	helmet: savedHelmet,
	pictures: picturesSaved,
}: View<"editTeamStadium">) => {
	useTitleBar({
		title: "Customize Stadium & Uniform",
		customMenu: undefined,
	});

	const [stadium, setStadium] = useState<StadiumLook>(savedStadium ?? {});
	const [helmet, setHelmet] = useState<HelmetLook>(savedHelmet ?? {});
	const [previewSky, setPreviewSky] = useState<"day" | "night">("day");
	const [saving, setSaving] = useState(false);
	// Pictures by id: the stadium's own, and any uploaded since.
	const [pictures, setPictures] = useState<Record<string, string>>(
		picturesSaved ?? {},
	);
	const [uploaded, setUploaded] = useState<string[]>([]);
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
	const [json, setJSON] = useState<string | undefined>();
	const [jsonError, setJSONError] = useState<string | undefined>();
	const apply = (text: string) => {
		const parsed = parseLook(text);
		if (typeof parsed === "string") {
			setJSON(text);
			setJSONError(parsed);
			return;
		}
		setJSON(undefined);
		setJSONError(undefined);
		setStadium(parsed.stadium);
		setHelmet(parsed.helmet);
	};

	const setField = <K extends keyof StadiumLook>(
		key: K,
		value: StadiumLook[K],
	) => {
		setStadium((s) => {
			const next = { ...s };
			if (value === undefined) {
				delete next[key];
			} else {
				next[key] = value;
			}
			return next;
		});
	};
	const setPart = <K extends keyof HelmetLook>(
		key: K,
		value: HelmetLook[K] | undefined,
	) => {
		setHelmet((h) => {
			const next = { ...h };
			if (value === undefined) {
				delete next[key];
			} else {
				next[key] = value;
			}
			return next;
		});
	};

	const sky = stadium.sky ?? previewSky;
	// (What each part of the helmet is, left to the team's colors - and of
	// the uniform, to go with what is set.)
	const auto = fieldKit(colors, "home", { shell: helmet.shell });
	const worn = {
		home: fieldKit(colors, "home", helmet),
		away: fieldKit(colors, "away", helmet),
	};

	// (The team with one setting left to its own - and its field and stadium
	// so.)
	const teamWithout = (key: keyof StadiumLook) => {
		const look = { ...stadium };
		delete look[key];
		return { abbrev, name, region, colors, stadium: look };
	};
	const fieldAuto = (key: keyof StadiumLook) =>
		fieldStyleFor(teamWithout(key), undefined);

	const save = async () => {
		setSaving(true);
		try {
			await toWorker("main", "updateTeamStadium", {
				tid,
				stadium,
				helmet,
				uploaded,
			});
			setUploaded([]);
			showNotification({ type: "success", text: "Stadium saved." });
			realtimeUpdate([], helpers.leagueUrl(["manage_teams"]));
		} catch (error) {
			showNotification({
				type: "error",
				text: `Could not save stadium: ${(error as Error).message}`,
				persistent: true,
			});
		} finally {
			setSaving(false);
		}
	};

	return (
		<div className="row">
			<div className="col-lg-7 mb-3">
				<Suspense
					fallback={
						<div
							className="rounded"
							style={{ aspectRatio: "16 / 9", background: "#07060a" }}
						/>
					}
				>
					<StadiumPreview
						team={{ abbrev, name, region, colors, imgURL }}
						stadium={stadium}
						helmet={helmet}
						sky={sky}
					/>
				</Suspense>
				{stadium.sky === undefined && stadium.roof !== "dome" ? (
					<div className="btn-group btn-group-sm mt-2" role="group">
						{(["day", "night"] as const).map((s) => (
							<button
								key={s}
								type="button"
								className={`btn ${previewSky === s ? "btn-secondary" : "btn-light-bordered"}`}
								onClick={() => setPreviewSky(s)}
							>
								{s === "day" ? "Day" : "Night"}
							</button>
						))}
					</div>
				) : null}
			</div>

			<div className="col-lg-5">
				<h3 className="h6 text-body-secondary">{`${region} ${name}`}</h3>
				<Choice<"roof">
					label="Roof"
					value={stadium.roof}
					options={[
						["open", "Open air"],
						["dome", "Dome"],
					]}
					onChange={(v) => setField("roof", v)}
				/>
				<HelmetColor
					label="Seats"
					value={stadium.seatColor}
					fallback={toHex(stadiumStyleFor(teamWithout("seatColor")).seats)}
					onChange={(v) => setField("seatColor", v)}
				/>
				<div className="mb-3">
					<label className="form-label mb-1" htmlFor="board-text">
						Board text
					</label>
					<input
						id="board-text"
						type="text"
						className="form-control"
						maxLength={BOARD_TEXT_MAX}
						value={stadium.boardText ?? ""}
						onChange={(e) =>
							setField(
								"boardText",
								e.target.value === "" ? undefined : e.target.value,
							)
						}
					/>
				</div>
				<Choice<"turf">
					label="Field"
					value={stadium.turf}
					options={[
						["grass", "Grass"],
						["artificial", "Artificial turf"],
					]}
					onChange={(v) => setField("turf", v)}
				/>
				<HelmetColor
					label="Field color"
					value={stadium.turfColor}
					fallback={fieldAuto("turfColor").turf}
					onChange={(v) => setField("turfColor", v)}
				/>
				<HelmetColor
					label="Line color"
					value={stadium.lineColor}
					fallback={fieldAuto("lineColor").lines}
					onChange={(v) => setField("lineColor", v)}
				/>
				<HelmetColor
					label="Sideline color"
					value={stadium.sidelineColor}
					fallback={toHex(fieldAuto("sidelineColor").surround)}
					onChange={(v) => setField("sidelineColor", v)}
				/>
				<Choice<"endZones">
					label="End zones"
					value={stadium.endZones}
					options={[
						["solid", "Team color"],
						["green", "Green with name"],
						["striped", "Striped"],
					]}
					onChange={(v) => setField("endZones", v)}
				/>
				<HelmetColor
					label="End zone color"
					value={stadium.endZoneColor}
					fallback={colors[0]}
					onChange={(v) => setField("endZoneColor", v)}
				/>
				<div className="mb-3">
					<label className="form-label mb-1" htmlFor="end-zone-text">
						End zone lettering
					</label>
					<div className="d-flex align-items-center gap-2">
						<input
							id="end-zone-text"
							type="text"
							className="form-control"
							maxLength={END_ZONE_TEXT_MAX}
							value={stadium.endZoneText ?? ""}
							placeholder={fieldAuto("endZoneText").endZones[0].text}
							disabled={stadium.endZoneText === ""}
							onChange={(e) =>
								setField(
									"endZoneText",
									e.target.value === "" ? undefined : e.target.value,
								)
							}
						/>
						<button
							type="button"
							className="btn btn-sm btn-light-bordered"
							onClick={() => setField("endZoneText", undefined)}
							disabled={stadium.endZoneText === undefined}
						>
							Auto
						</button>
						<button
							type="button"
							className="btn btn-sm btn-light-bordered"
							onClick={() => setField("endZoneText", "")}
							disabled={stadium.endZoneText === ""}
						>
							None
						</button>
					</div>
				</div>
				<HelmetColor
					label="Lettering color"
					value={stadium.endZoneTextColor}
					fallback={fieldAuto("endZoneTextColor").endZones[0].textColor}
					onChange={(v) => setField("endZoneTextColor", v)}
				/>
				<div className="form-check mb-3">
					<input
						type="checkbox"
						className="form-check-input"
						id="mid-logo"
						checked={stadium.midLogo !== false}
						onChange={(e) =>
							setField("midLogo", e.target.checked ? undefined : false)
						}
					/>
					<label className="form-check-label" htmlFor="mid-logo">
						Logo at midfield
					</label>
				</div>
				{stadium.midLogo !== false ? (
					<PictureField
						label="Midfield logo"
						url={stadium.midLogoURL}
						pictures={pictures}
						onChange={(v) => setField("midLogoURL", v)}
						onUpload={upload}
					/>
				) : null}
				<PictureField
					label="Championship trophy"
					url={stadium.trophyURL}
					pictures={pictures}
					onChange={(v) => setField("trophyURL", v)}
					onUpload={upload}
				/>
				<Choice<"sky">
					label="Kickoff"
					value={stadium.sky}
					options={[
						["day", "Always day"],
						["night", "Always night"],
					]}
					onChange={(v) => setField("sky", v)}
				/>

				<hr />
				<h3 className="h6 text-body-secondary">Helmet</h3>
				<HelmetColor
					label="Shell"
					value={helmet.shell}
					fallback={auto.helmet}
					onChange={(v) => setPart("shell", v)}
				/>
				<HelmetColor
					label="Stripe"
					value={helmet.stripe}
					fallback={auto.helmetStripe ?? colors[1]}
					onChange={(v) => setPart("stripe", v)}
					none
				/>
				<HelmetColor
					label="Stripe edge"
					value={helmet.stripeEdge}
					fallback={auto.helmetStripeEdge ?? colors[2]}
					onChange={(v) => setPart("stripeEdge", v)}
					none
				/>
				<HelmetColor
					label="Facemask"
					value={helmet.facemask}
					fallback={auto.facemask}
					onChange={(v) => setPart("facemask", v)}
				/>
				<div className="form-check mb-3">
					<input
						type="checkbox"
						className="form-check-input"
						id="helmet-logo"
						checked={helmet.logo !== false}
						onChange={(e) =>
							setPart("logo", e.target.checked ? undefined : false)
						}
					/>
					<label className="form-check-label" htmlFor="helmet-logo">
						Logo on helmet
					</label>
				</div>

				<hr />
				<h3 className="h6 text-body-secondary">Uniform</h3>
				<div className="row">
					{(["home", "away"] as const).map((edition) => (
						<div key={edition} className="col-6">
							<HelmetColor
								label={edition === "home" ? "Home jersey" : "Road jersey"}
								value={helmet[`${edition}Jersey`]}
								fallback={worn[edition].jersey}
								onChange={(v) => setPart(`${edition}Jersey`, v)}
							/>
							<HelmetColor
								label="Numbers"
								value={helmet[`${edition}Number`]}
								fallback={worn[edition].number}
								onChange={(v) => setPart(`${edition}Number`, v)}
							/>
							<HelmetColor
								label="Pants"
								value={helmet[`${edition}Pants`]}
								fallback={worn[edition].pants}
								onChange={(v) => setPart(`${edition}Pants`, v)}
							/>
						</div>
					))}
				</div>

				<hr />
				<button
					type="button"
					className="btn btn-sm btn-light-bordered"
					onClick={() => {
						setJSONError(undefined);
						setJSON(
							json === undefined
								? JSON.stringify({ stadium, helmet }, null, 2)
								: undefined,
						);
					}}
				>
					{json === undefined ? "Edit as JSON" : "Close JSON"}
				</button>
				<label className="btn btn-sm btn-light-bordered ms-2 mb-0">
					Load file
					<input
						type="file"
						accept=".json,application/json"
						hidden
						onChange={(e) => {
							const file = e.target.files?.[0];
							e.target.value = "";
							if (!file) {
								return;
							}
							file.text().then(apply, () => {
								setJSON("");
								setJSONError("Could not read that file.");
							});
						}}
					/>
				</label>
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
							onClick={() => apply(json)}
						>
							Apply
						</button>
					</div>
				) : null}

				<div className="d-flex gap-2 mt-3">
					<button
						type="button"
						className="btn btn-primary"
						onClick={save}
						disabled={saving}
					>
						Save stadium
					</button>
					<button
						type="button"
						className="btn btn-light-bordered"
						onClick={() => {
							setStadium({});
							setHelmet({});
						}}
						disabled={saving}
					>
						Reset to default
					</button>
				</div>
			</div>
		</div>
	);
};

export default EditTeamStadium;
