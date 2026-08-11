import { useCallback, useEffect, useMemo, useState } from "react";
import useTitleBar from "../../hooks/useTitleBar.tsx";
import type { View } from "../../../common/types.ts";
import {
	CARD_ERAS,
	CARD_SETS,
	cardSetsById,
	cardTitle,
	type CardSet,
} from "../../../common/tradingCards.ts";
import { toWorker } from "../../util/toWorker.ts";
import { realtimeUpdate } from "../../util/realtimeUpdate.ts";
import SelectMultiple from "../../components/SelectMultiple/index.tsx";
import ImageUploader from "../../components/ImageUploader.tsx";
import { TradingCardGallery } from "../../components/TradingCardGallery.tsx";
import { CopyPromptButton } from "./CopyPromptButton.tsx";
import { PasteButton } from "./PasteButton.tsx";
import { SetDesign } from "./SetDesign.tsx";
import { PlayerPicture } from "../../components/PlayerPicture.tsx";
import { usePlayerFace } from "../../util/playerFaces.ts";
import { useLocal } from "../../util/local.ts";
import { safeLocalStorage } from "../../util/safeLocalStorage.ts";

type PlayerOption = View<"createCards">["players"][number];

const uuid = (): string =>
	typeof crypto !== "undefined" && crypto.randomUUID
		? crypto.randomUUID()
		: `${Date.now()}-${Math.floor(Math.random() * 1e9)}`;

const sample = <T,>(arr: T[]): T | undefined =>
	arr.length > 0 ? arr[Math.floor(Math.random() * arr.length)] : undefined;

// Every variant label in the catalogue, so "parallel" can be filtered on
// before a set is chosen - the whole point of the filter is to find the sets
// that HAVE a refractor, not to pick one after the fact.
const ALL_VARIANT_LABELS = [
	...new Set(CARD_SETS.flatMap((set) => set.variants.map((v) => v.label))),
].sort();

// The set picker is a type-ahead over the whole catalogue, so the brand has to
// be searchable even when the set's name doesn't contain it ("Flair" is Fleer).
const setOptionLabel = (set: CardSet) =>
	set.label.includes(set.brand) ? set.label : `${set.label} · ${set.brand}`;

const CreateCards = ({
	cards,
	players,
	season: currentSeason,
}: View<"createCards">) => {
	useTitleBar({ title: "Create Cards" });

	const [era, setEra] = useState("");
	const [brand, setBrand] = useState("");
	const [variantLabel, setVariantLabel] = useState("");
	const [setId, setSetId] = useState(CARD_SETS[0]!.id);
	const [variantId, setVariantId] = useState("base");

	const [pid, setPid] = useState<number | undefined>();
	const [season, setSeason] = useState<number | undefined>();
	const [seasons, setSeasons] = useState<number[]>([]);

	const [prompts, setPrompts] = useState<
		{ front: string; back: string; title: string } | undefined
	>();
	const [frontURL, setFrontURL] = useState("");
	const [backURL, setBackURL] = useState("");
	const [saving, setSaving] = useState(false);

	// Shared with the achievement card modal, and remembered, because a name that
	// gets a prompt refused once gets every prompt refused.
	const [includeName, setIncludeNameRaw] = useState(
		() => safeLocalStorage.getItem("cardPromptIncludeName") !== "false",
	);
	const setIncludeName = (next: boolean) => {
		setIncludeNameRaw(next);
		safeLocalStorage.setItem("cardPromptIncludeName", next ? "true" : "false");
	};

	// Each filter offers only values that survive the OTHER filters, so no
	// combination can ever come back empty and no dropdown offers a dead option.
	const matchesFilters = useCallback(
		(set: CardSet, ignore?: "era" | "brand" | "variant") =>
			(ignore === "era" || era === "" || set.era === era) &&
			(ignore === "brand" || brand === "" || set.brand === brand) &&
			(ignore === "variant" ||
				variantLabel === "" ||
				set.variants.some((v) => v.label === variantLabel)),
		[era, brand, variantLabel],
	);

	const eras = useMemo(
		() =>
			CARD_ERAS.filter((e) =>
				CARD_SETS.some((set) => set.era === e.id && matchesFilters(set, "era")),
			),
		[matchesFilters],
	);

	const brands = useMemo(
		() =>
			[
				...new Set(
					CARD_SETS.filter((set) => matchesFilters(set, "brand")).map(
						(set) => set.brand,
					),
				),
			].sort(),
		[matchesFilters],
	);

	const variantLabels = useMemo(
		() =>
			ALL_VARIANT_LABELS.filter((label) =>
				CARD_SETS.some(
					(set) =>
						matchesFilters(set, "variant") &&
						set.variants.some((v) => v.label === label),
				),
			),
		[matchesFilters],
	);

	const filteredSets = useMemo(
		() =>
			CARD_SETS.filter((set) => matchesFilters(set)).sort(
				(a, b) => a.since - b.since,
			),
		[matchesFilters],
	);

	const set = cardSetsById.get(setId);
	const variant = set?.variants.find((v) => v.id === variantId);
	const filtered = era !== "" || brand !== "" || variantLabel !== "";

	// A set that just fell out of the filters shouldn't stay selected.
	useEffect(() => {
		if (filteredSets.length > 0 && !filteredSets.some((s) => s.id === setId)) {
			const next = filteredSets[0]!;
			setSetId(next.id);
			setVariantId(
				(variantLabel !== ""
					? next.variants.find((v) => v.label === variantLabel)
					: undefined
				)?.id ?? next.variants[0]!.id,
			);
		}
	}, [filteredSets, setId, variantLabel]);

	// Seasons come from the worker once a player is picked, rather than riding
	// along with every player in the index.
	useEffect(() => {
		let stale = false;
		if (pid === undefined) {
			setSeasons([]);
			setSeason(undefined);
			return;
		}
		void (async () => {
			const options = await toWorker("main", "getTradingCardOptions", pid);
			if (stale) {
				return;
			}
			setSeasons(options.seasons);
			setSeason(options.seasons[0] ?? currentSeason);
		})();
		return () => {
			stale = true;
		};
	}, [pid, currentSeason]);

	// Any change to what the card IS invalidates the prompts and the images
	// generated from them.
	useEffect(() => {
		setPrompts(undefined);
		setFrontURL("");
		setBackURL("");
	}, [pid, season, setId, variantId, includeName]);

	const player = players.find((p) => p.pid === pid);
	const ready = pid !== undefined && season !== undefined && set !== undefined;

	// The selected player's face (or photo), in the uniform of the selected
	// season, right here for screenshotting - no trip to the player page.
	const { lid } = useLocal(["lid"]);
	const faceData = usePlayerFace(pid, season, lid);

	const generatePrompts = async () => {
		if (!ready) {
			return;
		}
		const result = await toWorker("main", "getTradingCardPrompts", {
			pid,
			season,
			setId,
			variantId,
			includeName,
		});
		setPrompts(result);
	};

	const pickSet = (next: CardSet) => {
		setSetId(next.id);
		setVariantId(
			(variantLabel !== ""
				? next.variants.find((v) => v.label === variantLabel)
				: undefined
			)?.id ?? next.variants[0]!.id,
		);
	};

	const randomizeCard = () => {
		const nextSet = sample(filteredSets);
		if (!nextSet) {
			return;
		}
		const pool =
			variantLabel === ""
				? nextSet.variants
				: nextSet.variants.filter((v) => v.label === variantLabel);
		setSetId(nextSet.id);
		setVariantId(sample(pool)?.id ?? nextSet.variants[0]!.id);
	};

	const randomizePlayer = () => {
		const next = sample(players);
		if (next) {
			setPid(next.pid);
		}
	};

	const save = async () => {
		if (!ready || frontURL === "") {
			return;
		}
		setSaving(true);
		try {
			await toWorker("main", "upsertTradingCard", {
				id: uuid(),
				pid,
				season,
				setId,
				variantId,
				title: cardTitle(setId, variantId, season),
				frontURL,
				backURL: backURL === "" ? undefined : backURL,
				at: Date.now(),
			});
			setFrontURL("");
			setBackURL("");
			setPrompts(undefined);
		} finally {
			setSaving(false);
		}
	};

	const refresh = () => {
		realtimeUpdate(["tradingCards"]);
	};

	const imageField = (
		which: "front" | "back",
		url: string,
		setURL: (url: string) => void,
	) => (
		<div className="col-md-6">
			<label className="form-label mb-1 small text-body-secondary">
				{which === "front" ? "Front image" : "Back image"}
			</label>
			<div className="input-group input-group-sm mb-2">
				<input
					type="text"
					className="form-control"
					placeholder="Image URL"
					value={url}
					onChange={(event) => {
						setURL(event.target.value);
					}}
				/>
				<PasteButton onPaste={setURL} />
				{url !== "" ? (
					<button
						type="button"
						className="btn btn-secondary"
						title="Clear"
						onClick={() => {
							setURL("");
						}}
					>
						×
					</button>
				) : null}
			</div>
			{url !== "" ? (
				<img
					src={url}
					alt=""
					className="img-thumbnail mb-2 d-block"
					style={{ maxHeight: 220 }}
				/>
			) : (
				<ImageUploader onUploaded={setURL} />
			)}
		</div>
	);

	return (
		<>
			<div className="row g-3">
				<div className="col-lg-7">
					<h3 className="h5">The card</h3>
					<div className="row g-2">
						<div className="col-sm-4">
							<label className="form-label mb-1 small text-body-secondary">
								Era
							</label>
							<select
								className="form-select form-select-sm"
								value={era}
								onChange={(event) => {
									setEra(event.target.value);
								}}
							>
								<option value="">All eras</option>
								{eras.map((e) => (
									<option key={e.id} value={e.id}>
										{e.label}
									</option>
								))}
							</select>
						</div>
						<div className="col-sm-4">
							<label className="form-label mb-1 small text-body-secondary">
								Brand
							</label>
							<select
								className="form-select form-select-sm"
								value={brand}
								onChange={(event) => {
									setBrand(event.target.value);
								}}
							>
								<option value="">All brands</option>
								{brands.map((b) => (
									<option key={b} value={b}>
										{b}
									</option>
								))}
							</select>
						</div>
						<div className="col-sm-4">
							<label className="form-label mb-1 small text-body-secondary">
								Parallel
							</label>
							<select
								className="form-select form-select-sm"
								value={variantLabel}
								onChange={(event) => {
									setVariantLabel(event.target.value);
								}}
							>
								<option value="">Any</option>
								{variantLabels.map((label) => (
									<option key={label} value={label}>
										{label}
									</option>
								))}
							</select>
						</div>

						<div className="col-12">
							<label className="form-label mb-1 small text-body-secondary">
								Set ({filteredSets.length} of {CARD_SETS.length})
								{filtered ? (
									<button
										type="button"
										className="btn btn-link btn-sm p-0 ms-2 align-baseline"
										onClick={() => {
											setEra("");
											setBrand("");
											setVariantLabel("");
										}}
									>
										Clear filters
									</button>
								) : null}
							</label>
							<SelectMultiple<CardSet>
								value={set ?? null}
								options={filteredSets}
								onChange={(next) => {
									if (next) {
										pickSet(next);
									}
								}}
								getOptionLabel={setOptionLabel}
								getOptionValue={(s) => s.id}
								isClearable={false}
								placeholder="Search every set…"
							/>
						</div>

						<div className="col-sm-8">
							<label className="form-label mb-1 small text-body-secondary">
								Version
							</label>
							<select
								className="form-select form-select-sm"
								value={variantId}
								onChange={(event) => {
									setVariantId(event.target.value);
								}}
							>
								{set?.variants.map((v) => (
									<option key={v.id} value={v.id}>
										{v.label}
									</option>
								))}
							</select>
						</div>
						<div className="col-sm-4 d-flex align-items-end">
							<button
								type="button"
								className="btn btn-secondary btn-sm"
								onClick={randomizeCard}
								disabled={filteredSets.length === 0}
							>
								Random card
							</button>
						</div>

						{set ? (
							<div className="col-12">
								<SetDesign set={set} variant={variant} />
							</div>
						) : null}
					</div>
				</div>

				<div className="col-lg-5">
					<h3 className="h5">The player</h3>
					<div className="row g-2">
						<div className="col-12">
							<label className="form-label mb-1 small text-body-secondary">
								Player
							</label>
							<SelectMultiple<PlayerOption>
								value={player ?? null}
								options={players}
								onChange={(p) => {
									setPid(p?.pid);
								}}
								getOptionLabel={(p) =>
									`${p.name}${p.abbrev ? ` (${p.abbrev})` : ""}`
								}
								getOptionValue={(p) => String(p.pid)}
								placeholder="Choose a player…"
							/>
						</div>
						<div className="col-sm-6">
							<label className="form-label mb-1 small text-body-secondary">
								Season
							</label>
							<select
								className="form-select form-select-sm"
								value={season ?? ""}
								disabled={seasons.length === 0}
								onChange={(event) => {
									setSeason(Number(event.target.value));
								}}
							>
								{seasons.map((s) => (
									<option key={s} value={s}>
										{s}
									</option>
								))}
							</select>
						</div>
						<div className="col-sm-6 d-flex align-items-end">
							<button
								type="button"
								className="btn btn-secondary btn-sm"
								onClick={randomizePlayer}
							>
								Random player
							</button>
						</div>
						{faceData && (faceData.face || faceData.imgURL) ? (
							<div className="col-12">
								<div style={{ width: 120, height: 180 }}>
									<PlayerPicture
										face={faceData.face}
										imgURL={faceData.imgURL}
										colors={faceData.colors}
										jersey={faceData.jersey}
									/>
								</div>
							</div>
						) : null}
					</div>
				</div>
			</div>

			<hr />

			<div className="d-flex flex-wrap align-items-center gap-2">
				<button
					type="button"
					className="btn btn-primary"
					disabled={!ready}
					onClick={generatePrompts}
				>
					Build prompts
				</button>
				<div className="form-check">
					<input
						className="form-check-input"
						type="checkbox"
						id="create-cards-include-name"
						checked={includeName}
						onChange={(event) => {
							setIncludeName(event.target.checked);
						}}
					/>
					<label
						className="form-check-label"
						htmlFor="create-cards-include-name"
						title="Image models refuse prompts naming a real player. Off leaves the nameplate blank."
					>
						Include name
					</label>
				</div>
				{prompts ? (
					<>
						<CopyPromptButton label="Copy front prompt" text={prompts.front} />
						<CopyPromptButton label="Copy back prompt" text={prompts.back} />
						<a
							className="btn btn-secondary"
							href="https://chatgpt.com/"
							target="_blank"
							rel="noreferrer"
						>
							ChatGPT
						</a>
						<span className="text-body-secondary">{prompts.title}</span>
					</>
				) : null}
			</div>

			<div className="row g-3 mt-1">
				{imageField("front", frontURL, setFrontURL)}
				{imageField("back", backURL, setBackURL)}
				<div className="col-12">
					<button
						type="button"
						className="btn btn-primary"
						disabled={!ready || frontURL === "" || saving}
						onClick={save}
					>
						{saving ? "Saving…" : "Save card"}
					</button>
				</div>
			</div>

			<h2 className="h5 mt-4">Cards ({cards.length})</h2>
			<TradingCardGallery cards={cards} showPlayerName onDeleted={refresh} />
		</>
	);
};

export default CreateCards;
