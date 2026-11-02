import { lazy, Suspense, useMemo, useState, type ReactNode } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { helpers } from "../util/helpers.ts";
import { showNotification } from "../util/showNotification.ts";
import type {
	CourtImageAdjust,
	CourtPair,
	CourtVersionName,
	CourtVersionPicture,
	HelmetLook,
	StadiumLook,
	View,
} from "../../common/types.ts";
import { versionPictures } from "../../common/courtVersions.ts";
import {
	AdjustFields,
	PIC,
	pictureFromFile,
} from "../components/CourtImageField.tsx";
import {
	BOARD_TEXT_MAX,
	cleanHelmet,
	cleanStadium,
	END_ZONE_TEXT_MAX,
	STADIUM_PICTURE_KEYS,
} from "../../common/stadiumLook.ts";
import { toHex } from "./LiveGame/field3d/color.ts";
import {
	FIELD_TEMPLATE_H,
	FIELD_TEMPLATE_W,
	fieldTemplate,
} from "./fieldTemplate.ts";
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
	clear = "Auto",
	adjust,
}: {
	label: string;
	url: string | undefined;
	pictures: Record<string, string>;
	onChange: (value: string | undefined) => void;
	onUpload: (file: File) => Promise<string | undefined>;
	// (The button that goes back to none of its own.)
	clear?: string;
	// Its knobs, moving and sizing it where it sits (see StadiumLook.adjust).
	adjust?: {
		slot: string;
		value: CourtImageAdjust | undefined;
		onChange: (next: CourtImageAdjust | undefined) => void;
	};
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
					{clear}
				</button>
			</div>
			{adjust ? (
				<AdjustFields
					slot={adjust.slot}
					adjust={adjust.value}
					onAdjust={adjust.onChange}
					room={FIELD_ROOM}
				/>
			) : null}
		</div>
	);
};
// (How far a picture moves, feet: to the back of either end zone, and to
// either sideline.)
const FIELD_ROOM = { dx: 180, dy: 80 };
// A whole-field picture: as big as the template, a JPEG to fit.
const FIELD_UPLOAD = {
	sides: [FIELD_TEMPLATE_W, 2400, 1920, 1536],
	type: "image/jpeg",
} as const;

// The field's versions, as the page shows them (see StadiumLook.versions).
type Version = "regular" | CourtVersionName;
const VERSIONS: [Version, string][] = [
	["regular", "Regular season"],
	["homeOpener", "Home opener"],
	["playoffs", "Playoffs"],
	["finals", "Finals"],
];
// A picture added to a version: one at each 30-yard line, between the
// hashes, clear of a secondary logo at the 25s.
const NEW_PICTURE: CourtVersionPicture = {
	image: "",
	adjust: { scale: 0.5, dx: -58 },
	pair: true,
};
const PAIRS: [string, CourtPair | undefined][] = [
	["One", undefined],
	["Two, mirrored", true],
	["Two, diagonal", "diagonal"],
];

// A version's own pictures: each with where it sits, one or a pair, and
// another added. (The finals, with none of their own, show the playoffs' -
// till given their own.)
const VersionPictures = ({
	name,
	stadium,
	pictures,
	onUpload,
	onChange,
	onSet,
	trophy,
}: {
	name: CourtVersionName;
	stadium: StadiumLook;
	pictures: Record<string, string>;
	onUpload: (file: File) => Promise<string | undefined>;
	onChange: (i: number, next: Partial<CourtVersionPicture>) => void;
	onSet: (next: CourtVersionPicture[] | undefined) => void;
	trophy: ReactNode;
}) => {
	const own = stadium.versions?.[name];
	const borrowed = name === "finals" && own === undefined;
	const list = borrowed
		? (stadium.versions?.playoffs?.pictures ?? [])
		: (own?.pictures ?? []);
	return (
		<div className="border rounded p-2 mb-3">
			{trophy}
			{borrowed && list.length > 0 ? (
				<div className="d-flex align-items-center gap-2 mb-3">
					<span className="text-body-secondary small flex-grow-1">
						Same as playoffs
					</span>
					<button
						type="button"
						className="btn btn-sm btn-light-bordered"
						onClick={() => onSet(list.map((p) => ({ ...p })))}
					>
						Own pictures
					</button>
				</div>
			) : (
				list.map((p, i) => (
					<div key={i} className="mb-3">
						<PictureField
							label={`Picture ${i + 1}`}
							url={p.image === "" ? undefined : p.image}
							pictures={pictures}
							onChange={(v) => onChange(i, { image: v ?? "" })}
							onUpload={onUpload}
							clear="None"
							adjust={{
								slot: `${name}${i}`,
								value: p.adjust,
								onChange: (next) => onChange(i, { adjust: next }),
							}}
						/>
						<div className="d-flex align-items-center gap-2">
							<select
								className="form-select form-select-sm w-auto"
								value={PAIRS.findIndex(([, pair]) => pair === p.pair)}
								onChange={(e) =>
									onChange(i, {
										pair: PAIRS[Number.parseInt(e.target.value)]![1],
									})
								}
							>
								{PAIRS.map(([label], k) => (
									<option key={label} value={k}>
										{label}
									</option>
								))}
							</select>
							<button
								type="button"
								className="btn btn-sm btn-light-bordered ms-auto"
								onClick={() => onSet(list.filter((_, j) => j !== i))}
							>
								Delete
							</button>
						</div>
					</div>
				))
			)}
			{borrowed && list.length > 0 ? null : (
				<button
					type="button"
					className="btn btn-sm btn-light-bordered"
					onClick={() => onSet([...list, { ...NEW_PICTURE }])}
				>
					Add picture
				</button>
			)}
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
	const [version, setVersion] = useState<Version>("regular");
	const [saving, setSaving] = useState(false);
	// Pictures by id: the stadium's own, and any uploaded since.
	const [pictures, setPictures] = useState<Record<string, string>>(
		picturesSaved ?? {},
	);
	const [uploaded, setUploaded] = useState<string[]>([]);
	const upload = async (
		file: File,
		options?: Parameters<typeof pictureFromFile>[1],
	) => {
		try {
			const url = await pictureFromFile(file, options);
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
	// (The stadium as the preview draws it: its uploaded pictures filled in,
	// and the version's laid on the field.)
	const drawn = useMemo(() => {
		const out: StadiumLook = { ...stadium };
		for (const key of STADIUM_PICTURE_KEYS) {
			const v = out[key];
			if (v?.startsWith(PIC)) {
				const url = pictures[v.slice(PIC.length)];
				if (url === undefined) {
					delete out[key];
				} else {
					out[key] = url;
				}
			}
		}
		const decals = versionPictures(
			stadium,
			version === "regular" ? undefined : version,
		).flatMap((p) => {
			const href = p.image.startsWith(PIC)
				? pictures[p.image.slice(PIC.length)]
				: p.image;
			return href ? [{ href, adjust: p.adjust, pair: p.pair }] : [];
		});
		if (decals.length > 0) {
			out.decals = decals;
		}
		return out;
	}, [stadium, pictures, version]);
	// The JSON open beside it: the stadium as it is, as it changes - or, once
	// typed in, the text typed, till it is applied.
	const [jsonOpen, setJSONOpen] = useState(false);
	const [typed, setTyped] = useState<string | undefined>();
	const [jsonError, setJSONError] = useState<string | undefined>();
	const json = typed ?? JSON.stringify({ stadium, helmet }, null, 2);
	const apply = (text: string) => {
		const parsed = parseLook(text);
		if (typeof parsed === "string") {
			setJSONOpen(true);
			setTyped(text);
			setJSONError(parsed);
			return;
		}
		setTyped(undefined);
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
	const setAdjust = (
		slot: keyof NonNullable<StadiumLook["adjust"]>,
		value: CourtImageAdjust | undefined,
	) => {
		const next = { ...stadium.adjust };
		if (value === undefined) {
			delete next[slot];
		} else {
			next[slot] = value;
		}
		setField("adjust", Object.keys(next).length > 0 ? next : undefined);
	};
	// A version's own pictures (the finals with none of their own have the
	// playoffs' - see versionPictures), changed.
	const setVersionPictures = (
		name: CourtVersionName,
		next: CourtVersionPicture[] | undefined,
	) => {
		setStadium((s) => {
			const versions = { ...s.versions };
			if (next === undefined) {
				delete versions[name];
			} else {
				versions[name] = { pictures: next };
			}
			const out = { ...s };
			if (Object.keys(versions).length > 0) {
				out.versions = versions;
			} else {
				delete out.versions;
			}
			return out;
		});
	};
	const changePicture = (
		name: CourtVersionName,
		i: number,
		next: Partial<CourtVersionPicture>,
	) => {
		const list = stadium.versions?.[name]?.pictures ?? [];
		setVersionPictures(
			name,
			list.map((p, j) => {
				if (j !== i) {
					return p;
				}
				const out: CourtVersionPicture = { ...p, ...next };
				for (const key of Object.keys(next) as (keyof CourtVersionPicture)[]) {
					if (next[key] === undefined) {
						delete out[key];
					}
				}
				return out;
			}),
		);
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
						stadium={drawn}
						helmet={helmet}
						sky={sky}
						field={version !== "regular"}
						playoffs={version === "playoffs" || version === "finals"}
						finals={version === "finals"}
					/>
				</Suspense>
				<div className="d-flex flex-wrap gap-2 mt-2">
					<div className="btn-group btn-group-sm flex-wrap" role="group">
						{VERSIONS.map(([v, label]) => (
							<button
								key={v}
								type="button"
								className={`btn ${version === v ? "btn-secondary" : "btn-light-bordered"}`}
								onClick={() => setVersion(v)}
							>
								{label}
							</button>
						))}
					</div>
					{stadium.sky === undefined && stadium.roof !== "dome" ? (
						<div className="btn-group btn-group-sm" role="group">
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
			</div>

			<div className="col-lg-5">
				<h3 className="h6 text-body-secondary">{`${region} ${name}`}</h3>
				{version === "regular" ? null : (
					<VersionPictures
						name={version}
						stadium={stadium}
						pictures={pictures}
						onUpload={(file) => upload(file)}
						onChange={(i, next) => changePicture(version, i, next)}
						onSet={(next) => setVersionPictures(version, next)}
						trophy={
							version === "finals" ? (
								<>
									<PictureField
										label="Championship trophy"
										url={stadium.trophyURL}
										pictures={pictures}
										onChange={(v) => setField("trophyURL", v)}
										onUpload={upload}
									/>
									{stadium.fieldURL ? (
										<div className="mb-3">
											<div className="form-label mb-1">
												Field logo painted out
											</div>
											<AdjustFields
												slot="cover"
												adjust={stadium.adjust?.cover}
												onAdjust={(next) => setAdjust("cover", next)}
												room={FIELD_ROOM}
												placeOnly
											/>
										</div>
									) : null}
								</>
							) : null
						}
					/>
				)}
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
				<PictureField
					label="Board picture"
					url={stadium.boardURL}
					pictures={pictures}
					onChange={(v) => setField("boardURL", v)}
					onUpload={upload}
					clear="None"
				/>
				<Choice<"turf">
					label="Field"
					value={stadium.turf}
					options={[
						["grass", "Grass"],
						["artificial", "Artificial turf"],
					]}
					onChange={(v) => setField("turf", v)}
				/>
				<PictureField
					label="Field picture"
					url={stadium.fieldURL}
					pictures={pictures}
					onChange={(v) => {
						setField("fieldURL", v);
						if (v === undefined) {
							setField("fieldLines", undefined);
						}
					}}
					onUpload={(file) => upload(file, FIELD_UPLOAD)}
					clear="None"
				/>
				<div className="d-flex align-items-center gap-3 mb-3">
					{stadium.fieldURL ? (
						<div className="form-check mb-0">
							<input
								type="checkbox"
								className="form-check-input"
								id="field-lines"
								checked={stadium.fieldLines === "picture"}
								onChange={(e) =>
									setField(
										"fieldLines",
										e.target.checked ? "picture" : undefined,
									)
								}
							/>
							<label className="form-check-label" htmlFor="field-lines">
								Lines from the picture
							</label>
						</div>
					) : null}
					<button
						type="button"
						className="btn btn-sm btn-light-bordered"
						title={`${FIELD_TEMPLATE_W} \u00d7 ${FIELD_TEMPLATE_H}, 8 px per foot`}
						onClick={() => {
							const a = document.createElement("a");
							a.download = "field-template.png";
							a.href = fieldTemplate().toDataURL("image/png");
							a.click();
						}}
					>
						Download template
					</button>
				</div>
				{stadium.fieldURL ? null : (
					<HelmetColor
						label="Field color"
						value={stadium.turfColor}
						fallback={fieldAuto("turfColor").turf}
						onChange={(v) => setField("turfColor", v)}
					/>
				)}
				{stadium.fieldLines === "picture" ? null : (
					<HelmetColor
						label="Line color"
						value={stadium.lineColor}
						fallback={fieldAuto("lineColor").lines}
						onChange={(v) => setField("lineColor", v)}
					/>
				)}
				<HelmetColor
					label="Sideline color"
					value={stadium.sidelineColor}
					fallback={toHex(fieldAuto("sidelineColor").surround)}
					onChange={(v) => setField("sidelineColor", v)}
				/>
				{stadium.fieldURL ? null : (
					<>
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
						<PictureField
							label="End zone picture"
							url={stadium.endZoneURL}
							pictures={pictures}
							onChange={(v) => setField("endZoneURL", v)}
							onUpload={upload}
						/>
					</>
				)}
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
						adjust={{
							slot: "logo",
							value: stadium.adjust?.logo,
							onChange: (v) => setAdjust("logo", v),
						}}
					/>
				) : null}
				<PictureField
					label="Secondary logo"
					url={stadium.secondaryLogoURL}
					pictures={pictures}
					onChange={(v) => setField("secondaryLogoURL", v)}
					onUpload={upload}
					clear="None"
					{...(stadium.secondaryLogoURL
						? {
								adjust: {
									slot: "secondary",
									value: stadium.adjust?.secondary,
									onChange: (v: CourtImageAdjust | undefined) =>
										setAdjust("secondary", v),
								},
							}
						: {})}
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
						setTyped(undefined);
						setJSONOpen(!jsonOpen);
					}}
				>
					{jsonOpen ? "Close JSON" : "Edit as JSON"}
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
								setJSONOpen(true);
								setTyped("");
								setJSONError("Could not read that file.");
							});
						}}
					/>
				</label>
				{jsonOpen ? (
					<div className="mt-2">
						<textarea
							className="form-control font-monospace small"
							rows={12}
							spellCheck={false}
							value={json}
							onChange={(e) => setTyped(e.target.value)}
						/>
						{jsonError ? (
							<div className="text-danger small mt-1">{jsonError}</div>
						) : null}
						{typed !== undefined ? (
							<div className="d-flex gap-2 mt-2">
								<button
									type="button"
									className="btn btn-sm btn-secondary"
									onClick={() => apply(typed)}
								>
									Apply
								</button>
								<button
									type="button"
									className="btn btn-sm btn-light-bordered"
									onClick={() => {
										setJSONError(undefined);
										setTyped(undefined);
									}}
								>
									Discard
								</button>
							</div>
						) : null}
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
