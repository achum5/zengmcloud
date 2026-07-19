import { useCallback, useEffect, useRef, useState } from "react";
import { Modal } from "./Modal.tsx";
import ImageUploader from "./ImageUploader.tsx";
import SelectMultiple from "./SelectMultiple/index.tsx";
import { toWorker } from "../util/toWorker.ts";
import { helpers } from "../util/helpers.ts";
import { showNotification } from "../util/showNotification.ts";
import type { Image } from "../../common/types.ts";

// What a gallery is anchored to. A player gallery is every image tagging that
// pid; a team gallery is every image with that tid.
export type ImagesSubject =
	| { type: "player"; pid: number; name: string }
	| { type: "team"; tid: number; name: string };

type TaggablePlayer = { pid: number; name: string; abbrev?: string };

const CATEGORIES: { key: string; label: string }[] = [
	{ key: "general", label: "General" },
	{ key: "draft", label: "Draft night" },
	{ key: "postgame", label: "Post-game" },
	{ key: "profile", label: "Profile" },
	{ key: "other", label: "Other" },
];

const uuid = (): string =>
	typeof crypto !== "undefined" && crypto.randomUUID
		? crypto.randomUUID()
		: `${Date.now()}-${Math.floor(Math.random() * 1e9)}`;

// One image card: preview, caption, category, tagged players, and actions.
const ImageCard = ({
	image,
	subject,
	players,
	onChange,
	onDelete,
	onSetPrimary,
}: {
	image: Image;
	subject: ImagesSubject;
	players: TaggablePlayer[];
	onChange: (image: Image) => Promise<void>;
	onDelete: (id: string) => Promise<void>;
	onSetPrimary: (image: Image, small?: boolean) => Promise<void>;
}) => {
	const [caption, setCaption] = useState(image.caption ?? "");
	const nameByPid = new Map(players.map((p) => [p.pid, p.name]));

	const untaggedPlayers = players.filter(
		(p) => !image.playerIds.includes(p.pid),
	);

	return (
		<div className="border rounded p-2 d-flex gap-3">
			<a href={image.url} target="_blank" rel="noreferrer">
				<img
					src={image.url}
					alt=""
					style={{
						width: 120,
						height: 120,
						objectFit: "cover",
						borderRadius: 4,
					}}
				/>
			</a>
			<div className="flex-grow-1">
				<div className="mb-2 d-flex gap-2">
					<input
						type="text"
						className="form-control form-control-sm"
						placeholder="Caption"
						value={caption}
						onChange={(event) => setCaption(event.target.value)}
						onBlur={() => {
							if ((image.caption ?? "") !== caption) {
								void onChange({ ...image, caption: caption || undefined });
							}
						}}
					/>
					<select
						className="form-select form-select-sm"
						style={{ width: 130 }}
						value={image.category}
						onChange={(event) =>
							void onChange({ ...image, category: event.target.value })
						}
					>
						{CATEGORIES.map((c) => (
							<option key={c.key} value={c.key}>
								{c.label}
							</option>
						))}
					</select>
				</div>

				<div className="mb-2">
					<div className="text-body-secondary small mb-1">Tagged players</div>
					<div className="d-flex flex-wrap gap-1 mb-1">
						{image.playerIds.length === 0 ? (
							<span className="text-body-secondary small">None</span>
						) : (
							image.playerIds.map((pid) => (
								<span
									key={pid}
									className="badge text-bg-secondary d-inline-flex align-items-center gap-1"
								>
									<a
										href={helpers.leagueUrl(["player", pid])}
										className="text-white text-decoration-none"
									>
										{nameByPid.get(pid) ?? `Player ${pid}`}
									</a>
									<button
										type="button"
										className="btn-close btn-close-white"
										style={{ fontSize: 8 }}
										aria-label="Untag"
										onClick={() =>
											void onChange({
												...image,
												playerIds: image.playerIds.filter((x) => x !== pid),
											})
										}
									/>
								</span>
							))
						)}
					</div>
					<SelectMultiple<TaggablePlayer>
						value={null}
						options={untaggedPlayers}
						onChange={(p) => {
							if (p) {
								void onChange({
									...image,
									playerIds: [...image.playerIds, p.pid],
								});
							}
						}}
						getOptionLabel={(p) => p.name}
						getOptionValue={(p) => String(p.pid)}
						placeholder="Tag another player…"
						isClearable={false}
					/>
				</div>

				<div className="d-flex gap-2">
					{subject.type === "player" ? (
						<button
							type="button"
							className="btn btn-outline-primary btn-sm"
							onClick={() => void onSetPrimary(image)}
						>
							Set as profile
						</button>
					) : (
						<>
							<button
								type="button"
								className="btn btn-outline-primary btn-sm"
								onClick={() => void onSetPrimary(image)}
							>
								Set as logo
							</button>
							<button
								type="button"
								className="btn btn-outline-primary btn-sm"
								onClick={() => void onSetPrimary(image, true)}
							>
								Set as small logo
							</button>
						</>
					)}
					<button
						type="button"
						className="btn btn-outline-danger btn-sm ms-auto"
						onClick={() => void onDelete(image.id)}
					>
						Delete
					</button>
				</div>
			</div>
		</div>
	);
};

export const ImagesModal = ({
	show,
	subject,
	season,
	onHide,
}: {
	show: boolean;
	subject: ImagesSubject | undefined;
	season: number;
	onHide: () => void;
}) => {
	const [images, setImages] = useState<Image[] | undefined>();
	const [players, setPlayers] = useState<TaggablePlayer[]>([]);

	const load = useCallback(async () => {
		if (!subject) {
			return;
		}
		const filter =
			subject.type === "player" ? { pid: subject.pid } : { tid: subject.tid };
		const rows = await toWorker("main", "getImages", filter);
		setImages(rows);
	}, [subject]);

	useEffect(() => {
		if (!show || !subject) {
			return;
		}
		setImages(undefined);
		void load();

		// Player list for tagging + resolving tagged names (active players).
		void (async () => {
			try {
				const raw = await toWorker(
					"main",
					"getPlayersCommandPalette",
					undefined,
				);
				setPlayers(
					raw.map((p: any) => ({
						pid: p.pid,
						name: `${p.firstName} ${p.lastName}`,
						abbrev: p.abbrev,
					})),
				);
			} catch {
				setPlayers([]);
			}
		})();
	}, [show, subject, load]);

	const handleUploaded = async (url: string) => {
		if (!subject) {
			return;
		}
		const image: Image = {
			id: uuid(),
			url,
			playerIds: subject.type === "player" ? [subject.pid] : [],
			tid: subject.type === "team" ? subject.tid : undefined,
			category: "general",
			season,
			at: Date.now(),
		};
		await toWorker("main", "upsertImage", image);
		await load();
	};

	const handleChange = async (image: Image) => {
		await toWorker("main", "upsertImage", image);
		await load();
	};

	const handleDelete = async (id: string) => {
		await toWorker("main", "deleteImage", id);
		await load();
	};

	const handleSetPrimary = async (image: Image, small?: boolean) => {
		try {
			if (subject?.type === "player") {
				await toWorker("main", "setPlayerImage", {
					pid: subject.pid,
					imgURL: image.url,
				});
			} else if (subject?.type === "team") {
				await toWorker("main", "setTeamImage", {
					tid: subject.tid,
					imgURL: image.url,
					small,
				});
			}
			showNotification({
				type: "success",
				text: small ? "Small logo updated." : "Image updated.",
			});
		} catch (error) {
			showNotification({
				type: "error",
				text: error instanceof Error ? error.message : "Could not set image.",
			});
		}
	};

	return (
		<Modal show={show} onHide={onHide} size="lg">
			<Modal.Header closeButton>
				<Modal.Title>
					{subject ? `${subject.name} — images` : "Images"}
				</Modal.Title>
			</Modal.Header>
			<Modal.Body>
				<div className="mb-3">
					<ImageUploader onUploaded={handleUploaded} />
				</div>
				{images === undefined ? (
					<p className="text-body-secondary">Loading…</p>
				) : images.length === 0 ? (
					<p className="text-body-secondary">
						No images yet. Upload or paste one above.
					</p>
				) : (
					<div className="d-flex flex-column gap-2">
						{subject
							? images.map((image) => (
									<ImageCard
										key={image.id}
										image={image}
										subject={subject}
										players={players}
										onChange={handleChange}
										onDelete={handleDelete}
										onSetPrimary={handleSetPrimary}
									/>
								))
							: null}
					</div>
				)}
			</Modal.Body>
		</Modal>
	);
};

// Controller hook, mirroring useNegotiaionModal: owns show/subject and exposes
// an `open(subject)` you call from a button.
export const useImagesModal = (season: number) => {
	const [show, setShow] = useState(false);
	const [subject, setSubject] = useState<ImagesSubject | undefined>();
	const seasonRef = useRef(season);
	seasonRef.current = season;

	return {
		open: (nextSubject: ImagesSubject) => {
			setSubject(nextSubject);
			setShow(true);
		},
		props: {
			show,
			subject,
			season: seasonRef.current,
			onHide: () => setShow(false),
		},
	};
};
