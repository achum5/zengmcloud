import { lazy, Suspense, useEffect, useState } from "react";
import type { JerseySkinIds } from "../../../common/types.ts";
import { Modal } from "../../components/Modal.tsx";
import { downloadFile } from "../../util/downloadFile.ts";
import { toWorker } from "../../util/toWorker.ts";
import { fileToDataURL } from "../../util/uploadToImgbb.ts";
import {
	ART_H,
	ART_W,
	artScaleOf,
	clearGuides,
} from "../LiveGame/court3d/kitArt.ts";

const KitPreview = lazy(() => import("../LiveGame/court3d/KitPreview.tsx"));

type Side = keyof JerseySkinIds;

export type JerseySkinsTeam = {
	tid: number;
	region: string;
	name: string;
	colors: [string, string, string];
	jersey?: string;
};

// An uploaded picture made ready to keep: the template's shape, its guides
// gone, a PNG no bigger than twice the template.
const toSkinURL = async (file: File): Promise<string> => {
	const img = new Image();
	img.src = await fileToDataURL(file);
	await img.decode();
	const scale = artScaleOf(img.width, img.height);
	if (scale === undefined) {
		throw new Error(
			`The picture must be ${ART_W} × ${ART_H} pixels, or 2, 3 or 4 times that.`,
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
	const keep = Math.min(scale, 2);
	if (keep === scale) {
		return full.toDataURL("image/png");
	}
	const out = document.createElement("canvas");
	out.width = ART_W * keep;
	out.height = ART_H * keep;
	const g2 = out.getContext("2d")!;
	g2.imageSmoothingQuality = "high";
	g2.drawImage(full, 0, 0, out.width, out.height);
	return out.toDataURL("image/png");
};

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

	const save = async (side: Side, file: File | undefined) => {
		setBusy(side);
		setProblem(undefined);
		try {
			const url = file ? await toSkinURL(file) : undefined;
			const id = await toWorker("main", "setJerseySkin", {
				tid: t.tid,
				side,
				url,
			});
			if (id !== undefined && url !== undefined) {
				setUrls((old) => ({ ...old, [id]: url }));
			}
			const next = { ...ids };
			if (id === undefined) {
				delete next[side];
			} else {
				next[side] = id;
			}
			setIds(next);
			onChange(next);
		} catch (error) {
			setProblem(error.message);
		}
		setBusy(undefined);
	};

	const downloadTemplate = async () => {
		setBusy("template");
		try {
			const { kitTemplate } =
				await import("../LiveGame/court3d/kitTemplate.ts");
			const cv = await kitTemplate();
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
									<KitPreview
										dress={{ colors: t.colors, jersey: t.jersey }}
										side={side}
										url={id === undefined ? undefined : urls[id]}
										wordmark={side === "home" ? t.name : t.region}
									/>
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
							</div>
						);
					})}
				</div>
				{problem ? (
					<div className="alert alert-danger mt-3 mb-0">{problem}</div>
				) : null}
			</Modal.Body>
			<Modal.Footer>
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
