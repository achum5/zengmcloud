import { useLayoutEffect, useState, type ReactNode } from "react";
import type { FaceConfig } from "facesjs";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { toWorker } from "../util/toWorker.ts";
import { realtimeUpdate } from "../util/realtimeUpdate.ts";
import { helpers } from "../util/helpers.ts";
import { showNotification } from "../util/showNotification.ts";
import type { View } from "../../common/types.ts";
import { DEFAULT_JERSEY, JERSEYS } from "../../common/constants.ts";
import {
	parseUniform,
	presetToSpec,
	serializeUniform,
	type UniformSpec,
	type UniformTrim,
} from "../../common/uniform.ts";
import { MyFace } from "../components/MyFace.tsx";
import { FullBodyPlayer } from "../components/FullBodyPlayer.tsx";

// The uniform editor. Everything here edits a UniformSpec; the preview and the
// save both go through the same serialized jersey string the rest of the app
// reads, so what you see is exactly what every face in the league will wear.

const ColorField = ({
	label,
	value,
	fallback,
	offLabel = "Default",
	onChange,
	onClear,
}: {
	label: string;
	value: string | undefined;
	fallback: string;
	offLabel?: string;
	onChange: (value: string) => void;
	onClear: () => void;
}) => (
	<div className="mb-3">
		<label className="form-label mb-1">{label}</label>
		<div className="d-flex align-items-center gap-2">
			<input
				type="color"
				className="form-control form-control-color"
				value={value ?? fallback}
				onChange={(e) => onChange(e.target.value)}
			/>
			<button
				type="button"
				className="btn btn-sm btn-light-bordered"
				onClick={onClear}
				disabled={value === undefined}
			>
				{offLabel}
			</button>
		</div>
	</div>
);

// One list of trim bands - the collar's or the armholes'. Bands stack in
// order, first at the bottom, same as the spec stores them.
const TrimList = ({
	label,
	trims,
	onChange,
	extra,
}: {
	label: string;
	trims: UniformTrim[] | undefined;
	onChange: (trims: UniformTrim[] | undefined) => void;
	extra?: ReactNode;
}) => {
	const list = trims ?? [];
	const set = (i: number, trim: UniformTrim) => {
		const next = [...list];
		next[i] = trim;
		onChange(next);
	};
	return (
		<div className="mb-3">
			<label className="form-label mb-1">{label}</label>
			{list.map((trim, i) => (
				<div key={i} className="d-flex align-items-center gap-2 mb-1">
					<input
						type="color"
						className="form-control form-control-color flex-shrink-0"
						value={trim.color}
						onChange={(e) => set(i, { ...trim, color: e.target.value })}
					/>
					<input
						type="range"
						className="form-range flex-grow-1"
						min={1}
						max={24}
						step={1}
						title="Width"
						value={trim.width}
						onChange={(e) =>
							set(i, { ...trim, width: Number.parseInt(e.target.value) })
						}
					/>
					<button
						type="button"
						className="btn btn-sm btn-light-bordered"
						title="Remove band"
						onClick={() => {
							const next = list.filter((_, j) => j !== i);
							onChange(next.length > 0 ? next : undefined);
						}}
					>
						×
					</button>
				</div>
			))}
			<div className="d-flex gap-2">
				{list.length < 4 ? (
					<button
						type="button"
						className="btn btn-sm btn-light-bordered"
						onClick={() => onChange([...list, { color: "#000000", width: 8 }])}
					>
						Add band
					</button>
				) : null}
				{extra}
			</div>
		</div>
	);
};

const EditTeamUniform = ({
	tid,
	abbrev,
	region,
	name,
	colors,
	jersey,
}: View<"editTeamUniform">) => {
	useTitleBar({
		title: "Customize Jersey",
		customMenu: undefined,
	});

	const [spec, setSpec] = useState<UniformSpec>(() => {
		const parsed = parseUniform(jersey);
		if (parsed) {
			return parsed;
		}
		// Starting fresh from a preset: put the team name on the chest as a
		// starting point. Clearing the field takes it off.
		return {
			...presetToSpec(jersey ?? DEFAULT_JERSEY, colors),
			wordmark: { text: name.toUpperCase().slice(0, 16) },
		};
	});
	const [saving, setSaving] = useState(false);
	const [face, setFace] = useState<FaceConfig | undefined>();
	const [faceCount, setFaceCount] = useState(0);

	useLayoutEffect(() => {
		let stale = false;
		(async () => {
			const newFace = await toWorker("main", "generateFace", undefined);
			if (!stale) {
				setFace(newFace);
			}
		})();
		return () => {
			stale = true;
		};
	}, [faceCount]);

	const set = <K extends keyof UniformSpec>(key: K, value: UniformSpec[K]) => {
		setSpec((s) => {
			const next = { ...s };
			if (value === undefined) {
				delete next[key];
			} else {
				next[key] = value;
			}
			return next;
		});
	};

	const setImage = <K extends keyof NonNullable<UniformSpec["image"]>>(
		key: K,
		value: NonNullable<UniformSpec["image"]>[K],
	) => {
		setSpec((s) => {
			if (!s.image) {
				return s;
			}
			const image = { ...s.image };
			if (
				value === undefined ||
				(key !== "url" &&
					value === (key === "scale" || key === "opacity" ? 1 : 0))
			) {
				delete image[key];
			} else {
				image[key] = value;
			}
			return { ...s, image };
		});
	};

	const previewJersey = serializeUniform(spec);

	const save = async () => {
		setSaving(true);
		try {
			await toWorker("main", "updateTeamUniform", {
				tid,
				jersey: previewJersey,
			});
			showNotification({
				type: "success",
				text: "Jersey saved.",
			});
			realtimeUpdate([], helpers.leagueUrl(["manage_teams"]));
		} catch (error) {
			showNotification({
				type: "error",
				text: `Could not save jersey: ${(error as Error).message}`,
				persistent: true,
			});
		} finally {
			setSaving(false);
		}
	};

	const image = spec.image;

	return (
		<>
			<p className="text-body-secondary">
				Design the {region} {name} ({abbrev}) jersey. It's saved on the team, so
				everyone in a multiplayer league sees it.
			</p>

			<div className="row">
				<div className="col-lg-5 mb-3">
					<div className="position-sticky" style={{ top: 60 }}>
						{face ? (
							<div className="d-flex align-items-start gap-3">
								<div
									style={{ width: 190, cursor: "pointer" }}
									title="New face"
									onClick={() => {
										setFaceCount((c) => c + 1);
									}}
								>
									<FullBodyPlayer
										colors={colors}
										face={face}
										jersey={previewJersey}
										jerseyNumber="35"
										hgt={78}
									/>
								</div>
								{/* The jersey up close - it's a sliver of the full face. */}
								<div
									style={{ width: 230, height: 150, overflow: "hidden" }}
									className="rounded border"
								>
									<div
										style={{
											width: 400,
											marginLeft: -85,
											marginTop: -455,
										}}
									>
										<MyFace
											colors={colors}
											face={face}
											jersey={previewJersey}
										/>
									</div>
								</div>
							</div>
						) : null}
						<div className="mt-3 d-flex gap-2">
							<button
								className="btn btn-primary"
								disabled={saving}
								onClick={save}
							>
								Save
							</button>
							<a
								className="btn btn-light-bordered"
								href={helpers.leagueUrl(["manage_teams"])}
							>
								Cancel
							</a>
						</div>
					</div>
				</div>

				<div className="col-lg-7 col-xl-5">
					<div className="mb-3">
						<label className="form-label mb-1">Start from preset</label>
						<select
							className="form-select"
							value=""
							onChange={(e) => {
								if (e.target.value) {
									setSpec(presetToSpec(e.target.value, colors));
								}
							}}
						>
							<option value="">—</option>
							{helpers.keys(JERSEYS).map((id) => (
								<option key={id} value={id}>
									{JERSEYS[id]}
								</option>
							))}
						</select>
					</div>

					<ColorField
						label="Jersey color"
						value={spec.base}
						fallback={colors[0]}
						onChange={(v) => set("base", v)}
						onClear={() => set("base", undefined)}
					/>

					<TrimList
						label="Collar"
						trims={spec.collar}
						onChange={(trims) => set("collar", trims)}
					/>

					<TrimList
						label="Armholes"
						trims={spec.arm}
						onChange={(trims) => set("arm", trims)}
						extra={
							spec.collar ? (
								<button
									type="button"
									className="btn btn-sm btn-light-bordered"
									onClick={() =>
										set(
											"arm",
											spec.collar!.map((t) => ({ ...t })),
										)
									}
								>
									Copy collar
								</button>
							) : undefined
						}
					/>

					<ColorField
						label="Shoulder yoke"
						value={spec.yoke}
						fallback={colors[1]}
						offLabel="Off"
						onChange={(v) => set("yoke", v)}
						onClear={() => set("yoke", undefined)}
					/>

					<ColorField
						label="Chest band"
						value={spec.band}
						fallback={colors[1]}
						offLabel="Off"
						onChange={(v) => set("band", v)}
						onClear={() => set("band", undefined)}
					/>

					<div className="mb-3">
						<label className="form-label mb-1">Pinstripes</label>
						<div className="d-flex align-items-center gap-2">
							<input
								type="color"
								className="form-control form-control-color flex-shrink-0"
								value={spec.pinstripes?.color ?? colors[2]}
								onChange={(e) =>
									set("pinstripes", {
										...spec.pinstripes,
										color: e.target.value,
									})
								}
							/>
							<button
								type="button"
								className="btn btn-sm btn-light-bordered"
								onClick={() => set("pinstripes", undefined)}
								disabled={spec.pinstripes === undefined}
							>
								Off
							</button>
						</div>
						{spec.pinstripes ? (
							<div className="mt-2 ps-2 border-start">
								<label className="d-flex align-items-center gap-2 mb-1 small">
									<span
										style={{ width: "4.5rem" }}
										className="text-body-secondary"
									>
										Spacing
									</span>
									<input
										type="range"
										className="form-range flex-grow-1"
										min={6}
										max={40}
										step={1}
										value={spec.pinstripes.gap ?? 14}
										onChange={(e) =>
											set("pinstripes", {
												...spec.pinstripes!,
												gap: Number.parseInt(e.target.value),
											})
										}
									/>
								</label>
								<label className="d-flex align-items-center gap-2 mb-1 small">
									<span
										style={{ width: "4.5rem" }}
										className="text-body-secondary"
									>
										Width
									</span>
									<input
										type="range"
										className="form-range flex-grow-1"
										min={1}
										max={8}
										step={1}
										value={spec.pinstripes.width ?? 3}
										onChange={(e) =>
											set("pinstripes", {
												...spec.pinstripes!,
												width: Number.parseInt(e.target.value),
											})
										}
									/>
								</label>
							</div>
						) : null}
					</div>

					<div className="mb-3">
						<label className="form-label mb-1">Image</label>
						<input
							type="text"
							className="form-control"
							value={image?.url ?? ""}
							placeholder="https://..."
							onChange={(e) => {
								const url = e.target.value;
								if (url) {
									set("image", { ...image, url });
								} else {
									set("image", undefined);
								}
							}}
						/>
						{image ? (
							<div className="mt-2 ps-2 border-start">
								<label className="d-flex align-items-center gap-2 mb-1 small">
									<span
										style={{ width: "4.5rem" }}
										className="text-body-secondary"
									>
										Size
									</span>
									<input
										type="range"
										className="form-range flex-grow-1"
										min={0.2}
										max={4}
										step={0.05}
										value={image.scale ?? 1}
										onChange={(e) =>
											setImage("scale", Number.parseFloat(e.target.value))
										}
									/>
								</label>
								<label className="d-flex align-items-center gap-2 mb-1 small">
									<span
										style={{ width: "4.5rem" }}
										className="text-body-secondary"
									>
										Fade
									</span>
									<input
										type="range"
										className="form-range flex-grow-1"
										min={0}
										max={1}
										step={0.05}
										value={image.opacity ?? 1}
										onChange={(e) =>
											setImage("opacity", Number.parseFloat(e.target.value))
										}
									/>
								</label>
								<label className="d-flex align-items-center gap-2 mb-1 small">
									<span
										style={{ width: "4.5rem" }}
										className="text-body-secondary"
									>
										Left/right
									</span>
									<input
										type="range"
										className="form-range flex-grow-1"
										min={-100}
										max={100}
										step={1}
										value={image.dx ?? 0}
										onChange={(e) =>
											setImage("dx", Number.parseInt(e.target.value))
										}
									/>
								</label>
								<label className="d-flex align-items-center gap-2 mb-1 small">
									<span
										style={{ width: "4.5rem" }}
										className="text-body-secondary"
									>
										Up/down
									</span>
									<input
										type="range"
										className="form-range flex-grow-1"
										min={-100}
										max={100}
										step={1}
										value={image.dy ?? 0}
										onChange={(e) =>
											setImage("dy", Number.parseInt(e.target.value))
										}
									/>
								</label>
								<div className="form-check">
									<input
										type="checkbox"
										className="form-check-input"
										id="uniform-image-stretch"
										checked={image.stretch === true}
										onChange={(e) =>
											setImage("stretch", e.target.checked ? true : undefined)
										}
									/>
									<label
										className="form-check-label"
										htmlFor="uniform-image-stretch"
									>
										Stretch
									</label>
								</div>
							</div>
						) : null}
					</div>

					<div className="mb-3">
						<label className="form-label mb-1">Wordmark</label>
						<div className="d-flex align-items-center gap-2">
							<input
								type="text"
								className="form-control"
								maxLength={16}
								value={spec.wordmark?.text ?? ""}
								placeholder={name.toUpperCase()}
								onChange={(e) => {
									const text = e.target.value;
									if (text) {
										set("wordmark", { ...spec.wordmark, text });
									} else {
										set("wordmark", undefined);
									}
								}}
							/>
							{spec.wordmark?.text ? (
								<input
									type="color"
									className="form-control form-control-color flex-shrink-0"
									title="Wordmark color"
									value={spec.wordmark.color ?? colors[1]}
									onChange={(e) =>
										set("wordmark", {
											...spec.wordmark,
											color: e.target.value,
										})
									}
								/>
							) : null}
						</div>
					</div>

					<ColorField
						label="Number"
						value={spec.number?.color}
						fallback={colors[1]}
						onChange={(v) => set("number", { ...spec.number, color: v })}
						onClear={() => set("number", undefined)}
					/>

					<ColorField
						label="Shorts"
						value={spec.shorts?.base}
						fallback={spec.base ?? colors[0]}
						onChange={(v) => set("shorts", { ...spec.shorts, base: v })}
						onClear={() => {
							const next = { ...spec.shorts };
							delete next.base;
							set("shorts", Object.keys(next).length > 0 ? next : undefined);
						}}
					/>

					<ColorField
						label="Waistband"
						value={spec.shorts?.belt}
						fallback={spec.shorts?.base ?? spec.base ?? colors[0]}
						onChange={(v) => set("shorts", { ...spec.shorts, belt: v })}
						onClear={() => {
							const next = { ...spec.shorts };
							delete next.belt;
							set("shorts", Object.keys(next).length > 0 ? next : undefined);
						}}
					/>

					<ColorField
						label="Shorts side stripe"
						value={spec.shorts?.side}
						fallback={colors[1]}
						offLabel="Off"
						onChange={(v) => set("shorts", { ...spec.shorts, side: v })}
						onClear={() => {
							const next = { ...spec.shorts };
							delete next.side;
							set("shorts", Object.keys(next).length > 0 ? next : undefined);
						}}
					/>

					<TrimList
						label="Shorts hem"
						trims={spec.shorts?.trim}
						onChange={(trims) => {
							const next = { ...spec.shorts };
							if (trims) {
								next.trim = trims;
							} else {
								delete next.trim;
							}
							set("shorts", Object.keys(next).length > 0 ? next : undefined);
						}}
					/>
				</div>
			</div>
		</>
	);
};

export default EditTeamUniform;
