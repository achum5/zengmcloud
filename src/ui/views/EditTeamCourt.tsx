import { lazy, Suspense, useMemo, useState, type ReactNode } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { helpers } from "../util/helpers.ts";
import { showNotification } from "../util/showNotification.ts";
import type {
	View,
	CourtStyle,
	CourtImageAdjust,
	CourtImageSlot,
	CourtPair,
	CourtVersionName,
	CourtVersionPicture,
} from "../../common/types.ts";
import {
	versionPictures,
	withVersionPictures,
} from "../../common/courtVersions.ts";
import {
	AdjustFields,
	ImageField,
	PIC,
	pictureFromFile,
} from "../components/CourtImageField.tsx";
import { keepCourtPictures, parseCourt, PICTURE_KEYS } from "./courtFile.ts";
import { withArenaAdPictures } from "../../common/arenaAdPictures.ts";
import {
	FLOOR_TEMPLATE_H,
	FLOOR_TEMPLATE_W,
	floorTemplate,
} from "./courtTemplate.ts";

// (The 3D court is big: fetched when the page is opened.)
const CourtPreview = lazy(() => import("./LiveGame/court3d/CourtPreview.tsx"));

const DEFAULT_FLOOR = "#c9a165";
const DEFAULT_LINES = "#f8f5f0";

// A whole-floor picture: as big as the template, a JPEG to fit.
const FLOOR_UPLOAD = {
	sides: [2080, 1664, 1300, 1040],
	type: "image/jpeg",
} as const;

// The court as drawn: its uploaded pictures filled in.
const withPictures = (
	style: CourtStyle,
	pictures: Record<string, string>,
): CourtStyle => {
	const out = { ...style };
	for (const key of PICTURE_KEYS) {
		const v = out[key];
		if (typeof v === "string" && v.startsWith(PIC)) {
			const url = pictures[v.slice(PIC.length)];
			if (url === undefined) {
				delete out[key];
			} else {
				out[key] = url;
			}
		}
	}
	if (out.versions) {
		out.versions = withVersionPictures(out.versions, pictures);
	}
	if (out.arenaAds) {
		out.arenaAds = withArenaAdPictures(out.arenaAds, pictures);
	}
	return out;
};

// The court's versions, as the editor shows them (see CourtVersion).
type Version = "regular" | CourtVersionName;
const VERSIONS: [Version, string][] = [
	["regular", "Regular season"],
	["homeOpener", "Home opener"],
	["playoffs", "Playoffs"],
	["finals", "Finals"],
];
// A picture added to a version: where the league's own logos sit on an NBA
// floor - inside each three-point line toward the corner, one near each
// sideline.
const NEW_PICTURE: CourtVersionPicture = {
	image: "",
	adjust: { scale: 0.55, dx: -38, dy: 15 },
	pair: "diagonal",
};
const PAIRS: [string, CourtPair | undefined][] = [
	["One", undefined],
	["Two, mirrored", true],
	["Two, diagonal", "diagonal"],
];

// A row of a color picker + optional "use default" reset, bound to one
// CourtStyle field. Passing an empty value clears the field (fall back to
// default).
const ColorField = ({
	label,
	value,
	fallback,
	onChange,
	onClear,
	cleared,
}: {
	label: string;
	value: string;
	fallback: string;
	onChange: (value: string) => void;
	onClear?: () => void;
	cleared?: boolean;
}) => (
	<div className="mb-3">
		<label className="form-label mb-1">{label}</label>
		<div className="d-flex align-items-center gap-2">
			<input
				type="color"
				className="form-control form-control-color"
				value={cleared ? fallback : value}
				onChange={(e) => onChange(e.target.value)}
			/>
			{onClear ? (
				<button
					type="button"
					className="btn btn-sm btn-light-bordered"
					onClick={onClear}
					disabled={cleared}
				>
					Default
				</button>
			) : null}
		</div>
	</div>
);

// A color a court can have or go without: a checkbox, and the color once
// it has it.
const OptionalColor = ({
	id,
	label,
	value,
	fallback,
	onChange,
}: {
	id: string;
	label: string;
	value: string | undefined;
	fallback: string;
	onChange: (value: string | undefined) => void;
}) => (
	<div className="mb-3">
		<div className="form-check mb-1">
			<input
				type="checkbox"
				className="form-check-input"
				id={id}
				checked={value !== undefined}
				onChange={(e) => onChange(e.target.checked ? fallback : undefined)}
			/>
			<label className="form-check-label" htmlFor={id}>
				{label}
			</label>
		</div>
		{value !== undefined ? (
			<input
				type="color"
				className="form-control form-control-color"
				value={value}
				onChange={(e) => onChange(e.target.value)}
			/>
		) : null}
	</div>
);

// The same court, written either way round.
const sameCourt = (a: CourtStyle, b: CourtStyle) => {
	const key = (v: unknown): string =>
		v && typeof v === "object" && !Array.isArray(v)
			? `{${Object.keys(v)
					.sort()
					.map((k) => `${k}:${key((v as Record<string, unknown>)[k])}`)
					.join(",")}}`
			: JSON.stringify(v);
	return key(a) === key(b);
};

// A text input bound to one CourtStyle field, with an optional companion color
// picker (for text-color fields shown only when the text is non-empty).
const TextField = ({
	label,
	hint,
	value,
	placeholder,
	onChange,
	colorValue,
	colorFallback,
	onColorChange,
}: {
	label: string;
	hint?: string;
	value: string;
	placeholder?: string;
	onChange: (value: string) => void;
	colorValue?: string;
	colorFallback?: string;
	onColorChange?: (value: string) => void;
}) => (
	<div className="mb-3">
		<label className="form-label mb-1">
			{label}{" "}
			{hint ? <span className="text-body-secondary">{hint}</span> : null}
		</label>
		<div className="d-flex align-items-center gap-2">
			<input
				type="text"
				className="form-control"
				value={value}
				placeholder={placeholder}
				onChange={(e) => onChange(e.target.value)}
			/>
			{onColorChange && value ? (
				<input
					type="color"
					className="form-control form-control-color flex-shrink-0"
					title="Text color"
					value={colorValue || colorFallback || "#ffffff"}
					onChange={(e) => onColorChange(e.target.value)}
				/>
			) : null}
		</div>
	</div>
);

// A version's own pictures: each with where it sits, one or a pair, and
// another added. (The finals, with none of their own, show the playoffs' -
// till given their own.)
const VersionPictures = ({
	name,
	style,
	pictures,
	onUpload,
	onChange,
	onSet,
	trophy,
}: {
	name: CourtVersionName;
	style: CourtStyle;
	pictures: Record<string, string>;
	onUpload: (file: File) => Promise<string | undefined>;
	onChange: (i: number, next: Partial<CourtVersionPicture>) => void;
	onSet: (next: CourtVersionPicture[] | undefined) => void;
	trophy: ReactNode;
}) => {
	const own = style.versions?.[name];
	const borrowed = name === "finals" && own === undefined;
	const list = borrowed
		? (style.versions?.playoffs?.pictures ?? [])
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
						<ImageField
							label={`Picture ${i + 1}`}
							slot={`${name}${i}`}
							url={p.image}
							onURL={(v) => onChange(i, { image: v })}
							adjust={p.adjust}
							onAdjust={(_, next: CourtImageAdjust | undefined) =>
								onChange(i, { adjust: next })
							}
							pictures={pictures}
							onUpload={onUpload}
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

const EditTeamCourt = ({
	tid,
	region,
	name,
	colors,
	court,
	stock,
	pictures: picturesSaved,
	preview,
}: View<"editTeamCourt">) => {
	useTitleBar({
		title: `Customize Court`,
		customMenu: undefined,
	});

	// (A team with no court of its own opens on its stock floor.)
	const [style, setStyle] = useState<CourtStyle>(court ?? stock ?? {});
	const [version, setVersion] = useState<Version>("regular");
	const [saving, setSaving] = useState(false);
	// Pictures by id: the court's own, and any uploaded since.
	const [pictures, setPictures] =
		useState<Record<string, string>>(picturesSaved);
	const [uploaded, setUploaded] = useState<string[]>([]);
	// The JSON open beside it: the court as it is, as it changes - or, once
	// typed in, the text typed, till it is applied.
	const [jsonOpen, setJSONOpen] = useState(false);
	const [typed, setTyped] = useState<string | undefined>();
	const [jsonError, setJSONError] = useState<string | undefined>();
	const json = typed ?? JSON.stringify(style, null, 2);

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
	const picture = { pictures, onUpload: (file: File) => upload(file) };

	// A court from a file keeps its pictures in the league, as uploads do (one
	// too big to keep stays in the court).
	const keepPictures = async (court: CourtStyle) => {
		const kept = await keepCourtPictures(court);
		setPictures((p) => ({ ...p, ...kept.pictures }));
		setUploaded((u) => [...u, ...kept.ids]);
		setStyle(kept.court);
		setTyped(undefined);
	};

	// A version's own pictures (the finals with none of their own have the
	// playoffs' - see versionPictures), changed.
	const setVersionPictures = (
		name: CourtVersionName,
		next: CourtVersionPicture[] | undefined,
	) => {
		setStyle((s) => {
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
		const list = style.versions?.[name]?.pictures ?? [];
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

	// One image slot's size/position knobs.
	const setAdjust = (
		slot: CourtImageSlot,
		next: CourtImageAdjust | undefined,
	) => {
		setStyle((s) => {
			const adjust = { ...s.adjust };
			if (next === undefined) {
				delete adjust[slot];
			} else {
				adjust[slot] = next;
			}
			const out = { ...s };
			if (Object.keys(adjust).length > 0) {
				out.adjust = adjust;
			} else {
				delete out.adjust;
			}
			return out;
		});
	};

	// One of the arena ads changed: blank fields dropped, and blank ads off
	// the end of the list.
	const setAd = (
		i: number,
		patch: Partial<NonNullable<CourtStyle["arenaAds"]>[number]>,
	) => {
		setStyle((s) => {
			const ads = [...(s.arenaAds ?? [])];
			while (ads.length <= i) {
				ads.push({});
			}
			const next: Record<string, string | undefined> = {
				...ads[i],
				...patch,
			};
			for (const k of Object.keys(next)) {
				if (next[k] === undefined || next[k] === "") {
					delete next[k];
				}
			}
			ads[i] = next;
			while (ads.length > 0 && Object.keys(ads.at(-1)!).length === 0) {
				ads.pop();
			}
			const out = { ...s };
			if (ads.length > 0) {
				out.arenaAds = ads;
			} else {
				delete out.arenaAds;
			}
			return out;
		});
	};
	const set = <K extends keyof CourtStyle>(key: K, value: CourtStyle[K]) => {
		setStyle((s) => {
			const next = { ...s };
			if (value === undefined || value === "") {
				delete next[key];
			} else {
				next[key] = value;
			}
			return next;
		});
	};

	const shownStyle = useMemo((): CourtStyle => {
		const court = withPictures(style, pictures);
		const decals = versionPictures(
			court,
			version === "regular" ? undefined : version,
		)
			.filter((p) => p.image !== "" && !p.image.startsWith(PIC))
			.map((p) => ({ href: p.image, adjust: p.adjust, pair: p.pair }));
		return decals.length > 0 ? { ...court, decals } : court;
	}, [style, pictures, version]);

	const save = async () => {
		setSaving(true);
		try {
			await toWorker("main", "updateTeamCourt", {
				tid,
				// (Its stock floor, as it was, is no court of its own: it stays
				// the stock floor, following the team's colors.)
				court:
					Object.keys(style).length > 0 && !(stock && sameCourt(style, stock))
						? style
						: undefined,
				uploaded,
			});
			setUploaded([]);
			showNotification({
				type: "success",
				text: "Court saved.",
			});
			realtimeUpdate([], helpers.leagueUrl(["manage_teams"]));
		} catch (error) {
			showNotification({
				type: "error",
				text: `Could not save court: ${(error as Error).message}`,
				persistent: true,
			});
		} finally {
			setSaving(false);
		}
	};

	return (
		<>
			<p className="text-body-secondary">
				Design the {region} {name} home court shown during live game
				simulations. It's saved on the team, so everyone in a multiplayer league
				sees it.
			</p>

			<div className="row">
				<div className="col-lg-7 mb-3">
					<Suspense fallback={null}>
						<CourtPreview
							home={preview.home}
							away={preview.away}
							court={shownStyle}
							season={preview.season}
							arena={preview.arena}
							finals={version === "finals"}
							playoffs={version === "playoffs" || version === "finals"}
						/>
					</Suspense>
					<div className="btn-group btn-group-sm flex-wrap">
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
				</div>

				<div className="col-lg-5">
					{version === "regular" ? null : (
						<VersionPictures
							name={version}
							style={style}
							pictures={pictures}
							onUpload={(file) => upload(file)}
							onChange={(i, next) => changePicture(version, i, next)}
							onSet={(next) => setVersionPictures(version, next)}
							trophy={
								version === "finals" ? (
									<>
										<ImageField
											label="Championship trophy"
											hint="(center court)"
											slot="trophy"
											url={style.trophyURL ?? ""}
											onURL={(v) => set("trophyURL", v)}
											adjust={style.adjust?.trophy}
											onAdjust={setAdjust}
											{...picture}
										/>
										{style.floorImageURL ? (
											<div className="mb-3">
												<label className="form-label mb-1">
													Floor logo painted out
												</label>
												<AdjustFields
													slot="cover"
													adjust={style.adjust?.cover}
													onAdjust={(next) => setAdjust("cover", next)}
													placeOnly
												/>
											</div>
										) : null}
									</>
								) : null
							}
						/>
					)}

					<ImageField
						label="Floor picture"
						hint="(the whole floor from above)"
						slot="floor"
						url={style.floorImageURL ?? ""}
						onURL={(v) => {
							set("floorImageURL", v);
							if (!v) {
								set("floorLines", undefined);
							}
						}}
						adjust={style.adjust?.floor}
						onAdjust={setAdjust}
						pictures={pictures}
						onUpload={(file) => upload(file, FLOOR_UPLOAD)}
						defaultFit="fill"
					/>
					<div className="d-flex align-items-center gap-3 mb-3">
						{style.floorImageURL ? (
							<div className="form-check mb-0">
								<input
									type="checkbox"
									className="form-check-input"
									id="floor-lines"
									checked={style.floorLines === "picture"}
									onChange={(e) =>
										set("floorLines", e.target.checked ? "picture" : undefined)
									}
								/>
								<label className="form-check-label" htmlFor="floor-lines">
									Lines from the picture
								</label>
							</div>
						) : null}
						<button
							type="button"
							className="btn btn-sm btn-light-bordered"
							title={`${FLOOR_TEMPLATE_W} \u00d7 ${FLOOR_TEMPLATE_H}, 20 px per foot`}
							onClick={() => {
								const a = document.createElement("a");
								a.download = "floor-template.png";
								a.href = floorTemplate().toDataURL("image/png");
								a.click();
							}}
						>
							Download template
						</button>
					</div>

					{style.floorImageURL ? null : (
						<>
							<ColorField
								label="Floor color"
								value={style.floor ?? DEFAULT_FLOOR}
								fallback={DEFAULT_FLOOR}
								cleared={style.floor === undefined}
								onChange={(v) => set("floor", v)}
								onClear={() => set("floor", undefined)}
							/>

							<div className="mb-3">
								<label className="form-label mb-1">Floor pattern</label>
								<select
									className="form-select"
									value={style.floorPattern ?? "hardwood"}
									onChange={(e) =>
										set(
											"floorPattern",
											e.target.value as CourtStyle["floorPattern"],
										)
									}
								>
									<option value="hardwood">Hardwood planks</option>
									<option value="parquet">Parquet (basketweave)</option>
									<option value="diagonal">Diagonal planks</option>
									<option value="chevron">Chevron</option>
									<option value="solid">Solid</option>
								</select>
							</div>

							<OptionalColor
								id="inside-floor"
								label="Inside the three-point lines"
								value={style.insideFloor}
								fallback="#b37a43"
								onChange={(v) => set("insideFloor", v)}
							/>
						</>
					)}

					{style.floorLines === "picture" ? null : (
						<ColorField
							label="Line color"
							value={style.lines ?? DEFAULT_LINES}
							fallback={DEFAULT_LINES}
							cleared={style.lines === undefined}
							onChange={(v) => set("lines", v)}
							onClear={() => set("lines", undefined)}
						/>
					)}

					<div className="mb-3">
						<div className="form-check mb-1">
							<input
								type="checkbox"
								className="form-check-input"
								id="paint-key"
								checked={style.paint !== undefined}
								onChange={(e) =>
									set("paint", e.target.checked ? colors[0] : undefined)
								}
							/>
							<label className="form-check-label" htmlFor="paint-key">
								Painted key (colored lane)
							</label>
						</div>
						{style.paint !== undefined ? (
							<input
								type="color"
								className="form-control form-control-color"
								value={style.paint}
								onChange={(e) => set("paint", e.target.value)}
							/>
						) : null}
					</div>

					<OptionalColor
						id="paint-trim"
						label="Lane trim"
						value={style.paintTrim}
						fallback={colors[1]}
						onChange={(v) => set("paintTrim", v)}
					/>

					<OptionalColor
						id="center-circle"
						label="Center circle"
						value={style.centerCircle}
						fallback={colors[0]}
						onChange={(v) => set("centerCircle", v)}
					/>

					<ColorField
						label="Rail / sideline color"
						value={style.apron ?? colors[0]}
						fallback={colors[0]}
						cleared={style.apron === undefined}
						onChange={(v) => set("apron", v)}
						onClear={() => set("apron", undefined)}
					/>

					<ColorField
						label="Rail text color"
						value={style.apronText ?? colors[1]}
						fallback={colors[1]}
						cleared={style.apronText === undefined}
						onChange={(v) => set("apronText", v)}
						onClear={() => set("apronText", undefined)}
					/>

					<ImageField
						label="Center logo"
						hint="(blank = team logo)"
						slot="logo"
						url={style.logoURL ?? ""}
						onURL={(v) => set("logoURL", v)}
						adjust={style.adjust?.logo}
						onAdjust={setAdjust}
						{...picture}
					/>

					<ImageField
						label="Secondary logo"
						hint="(one in each half)"
						slot="secondary"
						url={style.secondaryLogoURL ?? ""}
						onURL={(v) => set("secondaryLogoURL", v)}
						adjust={style.adjust?.secondary}
						onAdjust={setAdjust}
						{...picture}
					/>

					<ImageField
						label="Sideline banner"
						hint="(runs along both sidelines)"
						slot="sideline"
						url={style.sidelineImageURL ?? ""}
						onURL={(v) => set("sidelineImageURL", v)}
						adjust={style.adjust?.sideline}
						onAdjust={setAdjust}
						{...picture}
						defaultFit="fill"
					/>

					<hr />
					<h3 className="h6 text-body-secondary">Baselines</h3>

					<ImageField
						label="Baseline rail image"
						hint="(the strip where the team name is)"
						slot="rail"
						url={style.railImageURL ?? ""}
						onURL={(v) => set("railImageURL", v)}
						adjust={style.adjust?.rail}
						onAdjust={setAdjust}
						{...picture}
						defaultFit="fill"
					/>

					<TextField
						label="Rail text"
						value={style.railText ?? ""}
						placeholder={name.toUpperCase()}
						onChange={(v) => set("railText", v)}
					/>
					<TextField
						label="Other rail"
						value={style.railText2 ?? ""}
						placeholder={style.railText || name.toUpperCase()}
						onChange={(v) => set("railText2", v)}
					/>
					<div className="mb-3">
						<label className="form-label mb-1" htmlFor="lettering">
							Lettering
						</label>
						<select
							id="lettering"
							className="form-select"
							value={style.lettering ?? "block"}
							onChange={(e) =>
								set(
									"lettering",
									e.target.value === "block"
										? undefined
										: (e.target.value as CourtStyle["lettering"]),
								)
							}
						>
							<option value="block">Block</option>
							<option value="serif">Serif</option>
							<option value="italic">Italic</option>
						</select>
					</div>

					<div className="form-check mb-3">
						<input
							className="form-check-input"
							type="checkbox"
							id="hide-rail-text"
							checked={style.hideRailText ?? false}
							onChange={(e) =>
								set("hideRailText", e.target.checked ? true : undefined)
							}
							disabled={!!style.railImageURL}
						/>
						<label className="form-check-label" htmlFor="hide-rail-text">
							Hide team name on the rails
						</label>
					</div>

					<ImageField
						label="Baseline floor logo"
						hint="(on the floor in each backcourt)"
						slot="baseline"
						url={style.baselineImageURL ?? ""}
						onURL={(v) => set("baselineImageURL", v)}
						adjust={style.adjust?.baseline}
						onAdjust={setAdjust}
						{...picture}
					/>

					<hr />
					<h3 className="h6 text-body-secondary">Arena-floor details</h3>

					<TextField
						label="Center-court script text"
						hint="(above the logo, e.g. “The Finals”)"
						value={style.centerText ?? ""}
						placeholder="The Finals"
						onChange={(v) => set("centerText", v)}
						colorValue={style.centerTextColor}
						colorFallback={style.apron ?? colors[0]}
						onColorChange={(v) => set("centerTextColor", v)}
					/>

					<ImageField
						label="Quarter-court logo"
						hint="(repeated in the four corners)"
						slot="corner"
						url={style.cornerLogoURL ?? ""}
						onURL={(v) => set("cornerLogoURL", v)}
						adjust={style.adjust?.corner}
						onAdjust={setAdjust}
						{...picture}
					/>

					<ImageField
						label="Bench banner"
						hint="(along the bench sideline only)"
						slot="bench"
						url={style.benchImageURL ?? ""}
						onURL={(v) => set("benchImageURL", v)}
						adjust={style.adjust?.bench}
						onAdjust={setAdjust}
						{...picture}
						defaultFit="fill"
					/>

					<TextField
						label="Bench sponsor text"
						hint="(e.g. “celtics.com”)"
						value={style.benchText ?? ""}
						placeholder="celtics.com"
						onChange={(v) => set("benchText", v)}
						colorValue={style.benchTextColor}
						colorFallback={style.apronText ?? colors[1]}
						onColorChange={(v) => set("benchTextColor", v)}
					/>

					<hr />
					<h3 className="h6 text-body-secondary">Arena ads</h3>
					{Array.from(
						{ length: Math.min(4, (style.arenaAds?.length ?? 0) + 1) },
						(_, i) => {
							const ad = style.arenaAds?.[i] ?? {};
							return (
								<div key={i} className="mb-3">
									<TextField
										label={`Ad ${i + 1}`}
										value={ad.text ?? ""}
										placeholder="Made-up sponsor"
										onChange={(v) => setAd(i, { text: v })}
										colorValue={ad.fg}
										colorFallback={colors[1]}
										onColorChange={(v) => setAd(i, { fg: v })}
									/>
									<TextField
										label="Line under it"
										value={ad.sub ?? ""}
										onChange={(v) => setAd(i, { sub: v })}
									/>
									<ColorField
										label="Background"
										value={ad.bg ?? colors[0]}
										fallback={colors[0]}
										onChange={(v) => setAd(i, { bg: v })}
										onClear={() => setAd(i, { bg: undefined })}
										cleared={ad.bg === undefined}
									/>
									<ImageField
										label="Picture"
										slot={`ad${i}`}
										url={ad.imageURL ?? ""}
										onURL={(v) => setAd(i, { imageURL: v })}
										adjust={undefined}
										onAdjust={() => {}}
										{...picture}
										defaultFit="contain"
									/>
								</div>
							);
						},
					)}

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
								file
									.text()
									.then((text) => {
										const parsed = parseCourt(text);
										if (typeof parsed === "string") {
											setJSONOpen(true);
											setTyped(text);
											setJSONError(parsed);
										} else {
											setJSONError(undefined);
											return keepPictures(parsed);
										}
									})
									.catch(() => {
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
										onClick={() => {
											const parsed = parseCourt(typed);
											if (typeof parsed === "string") {
												setJSONError(parsed);
											} else {
												setJSONError(undefined);
												void keepPictures(parsed);
											}
										}}
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
							Save court
						</button>
						<button
							type="button"
							className="btn btn-light-bordered"
							onClick={() => setStyle(stock ?? {})}
							disabled={saving}
						>
							Reset to default
						</button>
					</div>
				</div>
			</div>
		</>
	);
};

export default EditTeamCourt;
