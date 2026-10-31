import { lazy, Suspense, useState } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { helpers } from "../util/helpers.ts";
import { showNotification } from "../util/showNotification.ts";
import type { HelmetLook, StadiumLook, View } from "../../common/types.ts";
import { END_ZONE_TEXT_MAX } from "../../common/stadiumLook.ts";
import { fieldStyleFor } from "./LiveGame/field3d/fieldPaint.ts";
import { fieldKit } from "./LiveGame/field3d/kit.ts";

const StadiumPreview = lazy(
	() => import("./LiveGame/field3d/StadiumPreview.tsx"),
);

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
}: View<"editTeamStadium">) => {
	useTitleBar({
		title: "Customize Stadium & Uniform",
		customMenu: undefined,
	});

	const [stadium, setStadium] = useState<StadiumLook>(savedStadium ?? {});
	const [helmet, setHelmet] = useState<HelmetLook>(savedHelmet ?? {});
	const [previewSky, setPreviewSky] = useState<"day" | "night">("day");
	const [saving, setSaving] = useState(false);

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

	// (What the end zones are with one setting left to the team's colors and
	// name.)
	const endZoneAuto = (key: keyof StadiumLook) => {
		const look = { ...stadium };
		delete look[key];
		return fieldStyleFor(
			{ abbrev, name, region, colors, stadium: look },
			undefined,
		).endZones[0];
	};

	const save = async () => {
		setSaving(true);
		try {
			await toWorker("main", "updateTeamStadium", { tid, stadium, helmet });
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
				<Choice<"turf">
					label="Field"
					value={stadium.turf}
					options={[
						["grass", "Grass"],
						["artificial", "Artificial turf"],
					]}
					onChange={(v) => setField("turf", v)}
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
							placeholder={endZoneAuto("endZoneText").text}
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
					fallback={endZoneAuto("endZoneTextColor").textColor}
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
