import { lazy, Suspense, useEffect, useRef, useState } from "react";
import type { HelmetLook, JerseySkinIds } from "../../../common/types.ts";
import { Modal } from "../../components/Modal.tsx";
import { downloadFile } from "../../util/downloadFile.ts";
import { toWorker } from "../../util/toWorker.ts";
import { fileToDataURL } from "../../util/uploadToImgbb.ts";
import {
	ART_H,
	ART_H_LETTERED,
	ART_W,
	artScaleOf,
	clearGuides,
	DEFAULT_FIT,
	type KitFit,
	kitArtOf,
	loadKitPicture,
	writeFit,
} from "../LiveGame/court3d/kitArt.ts";

const KitPreview = lazy(() => import("../LiveGame/court3d/KitPreview.tsx"));
const FieldKitPreview = lazy(
	() => import("../LiveGame/field3d/FieldKitPreview.tsx"),
);

type Side = keyof JerseySkinIds;

export type JerseySkinsTeam = {
	tid: number;
	region: string;
	name: string;
	colors: [string, string, string];
	jersey?: string;
	// (Football: on its helmet.)
	imgURL?: string;
	helmet?: HelmetLook;
};

// A picture kept is a PNG at most this long (see setJerseySkin).
const SKIN_MAX = 690_000;

// An uploaded picture (a file, or one already read in) made ready to keep:
// the template's shape, its guides gone, a PNG as big as it can be and
// still be kept.
export const toSkinURL = async (file: File | string): Promise<string> => {
	const img = await loadKitPicture(
		typeof file === "string" ? file : await fileToDataURL(file),
	);
	const scale = artScaleOf(img.width, img.height);
	if (scale === undefined) {
		throw new Error(
			`The picture must be ${ART_W} × ${ART_H} (or ${ART_W} × ${ART_H_LETTERED}) pixels, or 2, 3 or 4 times that.`,
		);
	}
	const full = document.createElement("canvas");
	full.width = img.width;
	full.height = img.height;
	const g = full.getContext("2d", { willReadFrequently: true })!;
	g.drawImage(img, 0, 0);
	const pixels = g.getImageData(0, 0, img.width, img.height);
	clearGuides(pixels.data, img.width, img.height, scale);
	g.putImageData(pixels, 0, 0);
	const tall = img.height / scale;
	for (let keep = scale; keep >= 1; keep--) {
		let url: string;
		if (keep === scale) {
			url = full.toDataURL("image/png");
		} else {
			const out = document.createElement("canvas");
			out.width = ART_W * keep;
			out.height = tall * keep;
			const g2 = out.getContext("2d")!;
			g2.imageSmoothingQuality = "high";
			g2.drawImage(full, 0, 0, out.width, out.height);
			url = out.toDataURL("image/png");
		}
		if (url.length <= SKIN_MAX) {
			return url;
		}
	}
	throw new Error("That picture is too big.");
};

// A picture's lettering settings.
const fitOf = async (url: string): Promise<KitFit> =>
	kitArtOf(await loadKitPicture(url), "")?.fit ?? DEFAULT_FIT;

// A picture with these lettering settings written into it (made lettered,
// if it wasn't).
const withFit = async (url: string, fit: KitFit): Promise<string> => {
	const img = await loadKitPicture(url);
	const scale = artScaleOf(img.width, img.height) ?? 1;
	const cv = document.createElement("canvas");
	cv.width = img.width;
	cv.height = ART_H_LETTERED * scale;
	const g = cv.getContext("2d", { willReadFrequently: true })!;
	g.drawImage(img, 0, 0);
	const pixels = g.getImageData(0, 0, cv.width, cv.height);
	writeFit(pixels.data, cv.width, scale, fit);
	g.putImageData(pixels, 0, 0);
	return cv.toDataURL("image/png");
};

const FIT_SLIDERS: [keyof KitFit, string, number, number, number][] = [
	["numberSize", "Number size", 0.6, 1.6, 0.02],
	["numberUp", "Number height", -0.15, 0.15, 0.005],
	["backSize", "Back number size", 0.6, 1.6, 0.02],
	["backUp", "Back number height", -0.15, 0.15, 0.005],
	["nameSize", "Name size", 0.6, 1.8, 0.02],
	["nameUp", "Name height", -0.15, 0.1, 0.005],
	["nameArch", "Name arch", -1, 1, 0.05],
];

const FitControls = ({
	fit,
	onChange,
	side,
}: {
	fit: KitFit;
	onChange: (fit: KitFit) => void;
	side: Side;
}) => (
	<div className="text-start small mt-2" style={{ width: 190 }}>
		{FIT_SLIDERS.map(([key, label, min, max, step]) => (
			<label key={key} className="d-block mb-1">
				<span className="text-body-secondary">{label}</span>
				<input
					type="range"
					className="form-range"
					min={min}
					max={max}
					step={step}
					value={fit[key] as number}
					onChange={(event) => {
						onChange({ ...fit, [key]: Number(event.target.value) });
					}}
				/>
			</label>
		))}
		<div className="form-check">
			<input
				className="form-check-input"
				type="checkbox"
				id={`trim-${side}`}
				checked={fit.trim === "picture"}
				onChange={(event) => {
					onChange({ ...fit, trim: event.target.checked ? "picture" : "game" });
				}}
			/>
			<label className="form-check-label" htmlFor={`trim-${side}`}>
				Trim from picture
			</label>
		</div>
		<button
			className="btn btn-sm btn-light-bordered mt-1"
			onClick={() => {
				onChange(DEFAULT_FIT);
			}}
		>
			Reset
		</button>
	</div>
);

const JerseySkins = ({
	t,
	ids: initialIds,
	onChange,
	onHide,
}: {
	t: JerseySkinsTeam;
	ids: JerseySkinIds | undefined;
	onChange: (ids: JerseySkinIds) => void;
	onHide: () => void;
}) => {
	const [ids, setIds] = useState<JerseySkinIds>(initialIds ?? {});
	const [urls, setUrls] = useState<Record<string, string>>({});
	const [busy, setBusy] = useState<Side | "template">();
	const [problem, setProblem] = useState<string>();
	// The lettering: each picture's settings, by its id, and those being set
	// on a side till they're kept in a new picture.
	const [lettering, setLettering] = useState(false);
	const [fitsById, setFitsById] = useState<Record<string, KitFit>>({});
	const [drafts, setDrafts] = useState<Partial<Record<Side, KitFit>>>({});
	const keepTimer = useRef<Partial<Record<Side, number>>>({});

	useEffect(() => {
		const want = [ids.home, ids.away].filter(
			(id): id is string => id !== undefined && urls[id] === undefined,
		);
		if (want.length === 0) {
			return;
		}
		let alive = true;
		void (async () => {
			const got = await toWorker("main", "getJerseySkins", want);
			if (alive) {
				setUrls((old) => ({ ...old, ...got }));
			}
		})();
		return () => {
			alive = false;
		};
	}, [ids, urls]);

	// Each picture's settings, read from it.
	useEffect(() => {
		for (const id of [ids.home, ids.away]) {
			const url = id === undefined ? undefined : urls[id];
			if (id === undefined || url === undefined || fitsById[id]) {
				continue;
			}
			void fitOf(url).then((fit) => {
				setFitsById((old) => (old[id] ? old : { ...old, [id]: fit }));
			});
		}
	}, [ids, urls, fitsById]);
	const fitFor = (side: Side) => {
		const id = ids[side];
		return (
			drafts[side] ??
			(id === undefined ? undefined : fitsById[id]) ??
			DEFAULT_FIT
		);
	};

	const idsNow = useRef(ids);
	const keep = async (side: Side, url: string | undefined) => {
		const id = await toWorker("main", "setJerseySkin", {
			tid: t.tid,
			side,
			url,
		});
		if (id !== undefined && url !== undefined) {
			setUrls((old) => ({ ...old, [id]: url }));
		}
		const next = { ...idsNow.current };
		if (id === undefined) {
			delete next[side];
		} else {
			next[side] = id;
		}
		idsNow.current = next;
		setIds(next);
		onChange(next);
		return id;
	};

	// New lettering settings: shown at once, kept in a new picture once they
	// stop changing.
	const setFit = (side: Side, fit: KitFit) => {
		setDrafts((old) => ({ ...old, [side]: fit }));
		const id = ids[side];
		const url = id === undefined ? undefined : urls[id];
		if (url === undefined) {
			return;
		}
		window.clearTimeout(keepTimer.current[side]);
		keepTimer.current[side] = window.setTimeout(() => {
			void (async () => {
				try {
					const made = await keep(side, await withFit(url, fit));
					if (made !== undefined) {
						setFitsById((old) => ({ ...old, [made]: fit }));
					}
					setDrafts((old) =>
						old[side] === fit ? { ...old, [side]: undefined } : old,
					);
				} catch (error) {
					setProblem(error.message);
				}
			})();
		}, 250);
	};

	const save = async (side: Side, file: File | undefined) => {
		setBusy(side);
		setProblem(undefined);
		try {
			const url = file ? await toSkinURL(file) : undefined;
			window.clearTimeout(keepTimer.current[side]);
			setDrafts((old) => ({ ...old, [side]: undefined }));
			await keep(side, url);
		} catch (error) {
			setProblem(error.message);
		}
		setBusy(undefined);
	};

	// A picture hosted elsewhere, by its link - the template's shape, on a
	// host that lets it be read back.
	const saveLink = async (side: Side, link: string) => {
		setBusy(side);
		setProblem(undefined);
		try {
			let ok = false;
			try {
				ok = kitArtOf(await loadKitPicture(link), link) !== undefined;
			} catch {}
			if (!ok) {
				throw new Error("That link isn't a jersey picture the game can read.");
			}
			window.clearTimeout(keepTimer.current[side]);
			setDrafts((old) => ({ ...old, [side]: undefined }));
			await keep(side, link);
		} catch (error) {
			setProblem(error.message);
		}
		setBusy(undefined);
	};

	const downloadTemplate = async () => {
		setBusy("template");
		try {
			const cv =
				__SPORT === "football"
					? await (
							await import("../LiveGame/field3d/kitTemplate.ts")
						).fieldKitTemplate()
					: await (
							await import("../LiveGame/court3d/kitTemplate.ts")
						).kitTemplate();
			const blob = await new Promise<Blob | null>((resolve) => {
				cv.toBlob(resolve, "image/png");
			});
			if (blob) {
				downloadFile(
					"jersey-skin-template.png",
					[new Uint8Array(await blob.arrayBuffer())],
					"image/png",
				);
			}
		} catch (error) {
			setProblem(error.message);
		}
		setBusy(undefined);
	};

	return (
		<Modal show onHide={onHide}>
			<Modal.Header closeButton>
				{t.region} {t.name} 3D jerseys
			</Modal.Header>
			<Modal.Body>
				<div className="d-flex justify-content-center gap-4">
					{(["home", "away"] as const).map((side) => {
						const id = ids[side];
						return (
							<div key={side} className="text-center">
								<div className="fw-bold mb-1">
									{side === "home" ? "Home" : "Away"}
								</div>
								<Suspense
									fallback={<div style={{ width: 150, height: 240 }} />}
								>
									{__SPORT === "football" ? (
										<FieldKitPreview
											colors={t.colors}
											helmet={t.helmet}
											imgURL={t.imgURL}
											side={side}
											url={id === undefined ? undefined : urls[id]}
										/>
									) : (
										<KitPreview
											dress={{ colors: t.colors, jersey: t.jersey }}
											side={side}
											url={id === undefined ? undefined : urls[id]}
											wordmark={side === "home" ? t.name : t.region}
										/>
									)}
								</Suspense>
								<div className="mt-2">
									<label
										className={`btn btn-sm btn-light-bordered mb-0${busy !== undefined ? " disabled" : ""}`}
									>
										{busy === side ? "Saving..." : "Upload"}
										<input
											type="file"
											accept="image/*"
											hidden
											disabled={busy !== undefined}
											onChange={(event) => {
												const file = event.target.files?.[0];
												event.target.value = "";
												if (file) {
													void save(side, file);
												}
											}}
										/>
									</label>
									{id !== undefined ? (
										<button
											className="btn btn-sm btn-light-bordered ms-2"
											disabled={busy !== undefined}
											onClick={() => {
												void save(side, undefined);
											}}
										>
											Remove
										</button>
									) : null}
								</div>
								<input
									key={id}
									type="url"
									className="form-control form-control-sm mt-2 mx-auto"
									style={{ width: 190 }}
									placeholder="or a link: https://..."
									defaultValue={
										id !== undefined && /^https?:\/\//.test(id) ? id : ""
									}
									disabled={busy !== undefined}
									onKeyDown={(event) => {
										if (event.key === "Enter") {
											event.currentTarget.blur();
										}
									}}
									onBlur={(event) => {
										const link = event.target.value.trim();
										if (/^https?:\/\/\S+$/.test(link) && link !== id) {
											void saveLink(side, link);
										}
									}}
								/>
								{lettering && id !== undefined ? (
									<FitControls
										fit={fitFor(side)}
										side={side}
										onChange={(fit) => {
											setFit(side, fit);
										}}
									/>
								) : null}
							</div>
						);
					})}
				</div>
				{problem ? (
					<div className="alert alert-danger mt-3 mb-0">{problem}</div>
				) : null}
			</Modal.Body>
			<Modal.Footer>
				{ids.home !== undefined || ids.away !== undefined ? (
					<button
						className={`btn btn-light-bordered${lettering ? " active" : ""}`}
						onClick={() => {
							setLettering(!lettering);
						}}
					>
						Lettering
					</button>
				) : null}
				<button
					className="btn btn-light-bordered"
					disabled={busy !== undefined}
					onClick={() => {
						void downloadTemplate();
					}}
				>
					{busy === "template" ? "Making template..." : "Download template"}
				</button>
				<button className="btn btn-secondary" onClick={onHide}>
					Close
				</button>
			</Modal.Footer>
		</Modal>
	);
};

export default JerseySkins;
