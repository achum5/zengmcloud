import { useEffect, useState } from "react";
import { Modal } from "./Modal.tsx";
import { toWorker } from "../util/toWorker.ts";
import {
	CARD_SETS,
	cardSetsById,
	cardTitle,
	type CardSet,
} from "../../common/tradingCards.ts";
import type {
	AchievementCardSpec,
	DraftCardScene,
} from "../../common/achievementCards.ts";
import SelectMultiple from "./SelectMultiple/index.tsx";
import ImageUploader from "./ImageUploader.tsx";
import { CopyPromptButton } from "../views/CreateCards/CopyPromptButton.tsx";
import { PasteButton } from "../views/CreateCards/PasteButton.tsx";
import { RecapAIButton } from "./RecapAIButton.tsx";
import { PlayerPicture } from "./PlayerPicture.tsx";
import { usePlayerFace } from "../util/playerFaces.ts";
import { useLocal } from "../util/local.ts";

type AchievementCardData = {
	pending: AchievementCardSpec[];
	total: number;
	done: number;
};

// Every card needs a set to be printed in, and defaulting it deterministically
// from the card's id spreads a season's cards across the catalogue instead of
// opening every card on the same design. Same FNV walk as the prompt builder.
const defaultSetId = (id: string): string => {
	let h = 2166136261;
	for (let i = 0; i < id.length; i++) {
		h ^= id.charCodeAt(i);
		h = Math.imul(h, 16777619);
	}
	return CARD_SETS[(h >>> 0) % CARD_SETS.length]!.id;
};

const setOptionLabel = (set: CardSet) =>
	set.label.includes(set.brand) ? set.label : `${set.label} · ${set.brand}`;

const sample = <T,>(arr: T[]): T | undefined =>
	arr.length > 0 ? arr[Math.floor(Math.random() * arr.length)] : undefined;

// One achievement at a time: the modal opens on whoever is next, and saving
// moves straight to the one after. State is reset by the parent keying this
// component on spec.id.
const CardMaker = ({
	spec,
	remaining,
	onSaved,
	onSkip,
	onHide,
}: {
	spec: AchievementCardSpec;
	remaining: number;
	onSaved: () => void;
	onSkip: () => void;
	onHide: () => void;
}) => {
	const [setId, setSetId] = useState(() => defaultSetId(spec.id));
	const [variantId, setVariantId] = useState(
		() => cardSetsById.get(defaultSetId(spec.id))!.variants[0]!.id,
	);
	const [scene, setScene] = useState<DraftCardScene>("draftNight");
	const [prompts, setPrompts] = useState<
		{ front: string; back: string; title: string } | undefined
	>();
	const [frontURL, setFrontURL] = useState("");
	const [backURL, setBackURL] = useState("");
	const [saving, setSaving] = useState(false);

	const set = cardSetsById.get(setId);

	// A whole different design, in one press, when the seeded default isn't the
	// one you want for this player. Rolls the version too, so a parallel can turn
	// up on its own rather than only when you go looking for it.
	const randomizeCard = () => {
		const nextSet = sample(CARD_SETS);
		if (!nextSet) {
			return;
		}
		setSetId(nextSet.id);
		setVariantId(sample(nextSet.variants)?.id ?? nextSet.variants[0]!.id);
	};

	// Any change to what the card IS invalidates the prompts and the images
	// generated from them.
	useEffect(() => {
		setPrompts(undefined);
		setFrontURL("");
		setBackURL("");
	}, [setId, variantId, scene]);

	const { lid } = useLocal(["lid"]);
	const faceData = usePlayerFace(spec.pid, spec.season, lid);

	const generatePrompts = async () => {
		const result = await toWorker("main", "getAchievementCardPrompts", {
			pid: spec.pid,
			season: spec.season,
			setId,
			variantId,
			kind: spec.kind,
			label: spec.label,
			scene: spec.kind === "draft" ? scene : undefined,
		});
		setPrompts(result);
	};

	const save = async () => {
		if (frontURL === "") {
			return;
		}
		setSaving(true);
		try {
			await toWorker("main", "upsertTradingCard", {
				id: spec.id,
				pid: spec.pid,
				season: spec.season,
				setId,
				variantId,
				title: `${cardTitle(setId, variantId, spec.season)} · ${spec.label}`,
				frontURL,
				backURL: backURL === "" ? undefined : backURL,
				at: Date.now(),
			});
			onSaved();
		} finally {
			setSaving(false);
		}
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
					style={{ maxHeight: 180 }}
				/>
			) : (
				<ImageUploader onUploaded={setURL} />
			)}
		</div>
	);

	return (
		<Modal centered show onHide={onHide} scrollable size="lg">
			<Modal.Header closeButton>
				<Modal.Title>
					{spec.name} · {spec.label} · {spec.season}
				</Modal.Title>
			</Modal.Header>
			<Modal.Body>
				<div className="row g-2">
					<div className="col-md-6">
						<label className="form-label mb-1 small text-body-secondary">
							Set
						</label>
						<SelectMultiple<CardSet>
							value={set ?? null}
							options={CARD_SETS}
							onChange={(next) => {
								if (next) {
									setSetId(next.id);
									setVariantId(next.variants[0]!.id);
								}
							}}
							getOptionLabel={setOptionLabel}
							getOptionValue={(s) => s.id}
							isClearable={false}
						/>
					</div>
					<div className="col-md-4">
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
					<div className="col-md-2 d-flex align-items-end">
						<button
							type="button"
							className="btn btn-secondary btn-sm"
							onClick={randomizeCard}
							title="Random set and version"
						>
							Random
						</button>
					</div>

					{spec.kind === "draft" ? (
						<div className="col-12">
							<div className="btn-group btn-group-sm" role="group">
								<button
									type="button"
									className={`btn ${scene === "draftNight" ? "btn-primary" : "btn-outline-primary"}`}
									onClick={() => {
										setScene("draftNight");
									}}
								>
									Draft night
								</button>
								<button
									type="button"
									className={`btn ${scene === "college" ? "btn-primary" : "btn-outline-primary"}`}
									onClick={() => {
										setScene("college");
									}}
								>
									College action
								</button>
							</div>
						</div>
					) : null}

					{faceData && (faceData.face || faceData.imgURL) ? (
						<div className="col-12">
							<div style={{ width: 90, height: 135 }}>
								<PlayerPicture
									face={faceData.face}
									imgURL={faceData.imgURL}
									colors={faceData.colors}
									jersey={faceData.jersey}
								/>
							</div>
						</div>
					) : null}

					<div className="col-12 d-flex flex-wrap align-items-center gap-2">
						<button
							type="button"
							className="btn btn-primary btn-sm"
							onClick={generatePrompts}
						>
							Build prompts
						</button>
						{prompts ? (
							<>
								<CopyPromptButton
									label="Copy front prompt"
									text={prompts.front}
								/>
								<CopyPromptButton
									label="Copy back prompt"
									text={prompts.back}
								/>
								<RecapAIButton />
							</>
						) : null}
					</div>

					{imageField("front", frontURL, setFrontURL)}
					{imageField("back", backURL, setBackURL)}
				</div>
			</Modal.Body>
			<Modal.Footer>
				<span className="me-auto small text-body-secondary">
					{remaining} remaining
				</span>
				<button
					type="button"
					className="btn btn-secondary"
					disabled={remaining < 2}
					onClick={onSkip}
				>
					Skip
				</button>
				<button
					type="button"
					className="btn btn-primary"
					disabled={frontURL === "" || saving}
					onClick={save}
				>
					{saving ? "Saving…" : "Save & next"}
				</button>
			</Modal.Footer>
		</Modal>
	);
};

// The achievement-card reminder, in the recap widgets' mold: it sits on the
// page while the season still has cards unmade, counts what's left, and takes
// itself off the page the moment the set is complete. Finished cards land in
// the synced tradingCards store, so the count falls on every device in the
// room no matter who makes a card.
export const AchievementCards = ({
	season,
	context,
	heading,
}: {
	season: number;
	// "draft" = the class's top picks (Draft History page); "season" = awards,
	// All-Stars and champions (season History page).
	context: "draft" | "season";
	heading: string;
}) => {
	const [data, setData] = useState<AchievementCardData | undefined>();
	const [reload, setReload] = useState(0);
	const [show, setShow] = useState(false);
	const [offset, setOffset] = useState(0);

	useEffect(() => {
		let cancelled = false;
		setData(undefined);
		(async () => {
			try {
				const result = await toWorker("main", "getAchievementCardData", {
					season,
					context,
				});
				if (!cancelled) {
					setData(result);
				}
			} catch (error) {
				console.error("Failed to load achievement card data", error);
			}
		})();
		return () => {
			cancelled = true;
		};
	}, [season, context, reload]);

	if (!data || data.pending.length === 0) {
		return null;
	}

	const spec = data.pending[offset % data.pending.length]!;

	return (
		<div className="d-inline-flex flex-column">
			<h2 className="h5">{heading}</h2>
			<div className="d-flex align-items-center gap-2">
				<button
					className="btn btn-sm btn-primary"
					onClick={() => {
						setShow(true);
					}}
				>
					Make cards
				</button>
				<span className="small text-body-secondary">
					{data.done}/{data.total} made
				</span>
			</div>
			{show ? (
				<CardMaker
					key={spec.id}
					spec={spec}
					remaining={data.pending.length}
					onSaved={() => {
						setOffset(0);
						setReload((prev) => prev + 1);
					}}
					onSkip={() => {
						setOffset((prev) => prev + 1);
					}}
					onHide={() => {
						setShow(false);
					}}
				/>
			) : null}
		</div>
	);
};

export default AchievementCards;
