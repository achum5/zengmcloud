import { useEffect, useState } from "react";
import type { CourtImageAdjust } from "../../common/types.ts";

// A picture uploaded to the league, as a court names it (see the worker's
// courtPictures.ts).
export const PIC = "pic:";

// A picture file made into a PNG small enough to keep in the league - as
// big as it can be and still fit.
const PICTURE_MAX = 690_000;
export const pictureFromFile = async (file: File): Promise<string> => {
	const src = URL.createObjectURL(file);
	try {
		const img = new Image();
		img.src = src;
		await img.decode();
		const w0 = img.naturalWidth || 1024;
		const h0 = img.naturalHeight || 1024;
		for (const side of [1024, 768, 512, 384, 256]) {
			const k = Math.min(1, side / Math.max(w0, h0));
			const cv = document.createElement("canvas");
			cv.width = Math.max(1, Math.round(w0 * k));
			cv.height = Math.max(1, Math.round(h0 * k));
			cv.getContext("2d")!.drawImage(img, 0, 0, cv.width, cv.height);
			const url = cv.toDataURL("image/png");
			if (url.length <= PICTURE_MAX) {
				return url;
			}
		}
		throw new Error("That picture is too big.");
	} finally {
		URL.revokeObjectURL(src);
	}
};

// AN IMAGE SLOT, with the knobs that make it yours.
//
// A URL on its own is not customization - it drops the picture wherever the
// court happens to put it, at whatever size the court happens to want. The
// sliders appear once there is an image to move, so an unused slot is still
// one line.
export const ImageField = <Slot extends string>({
	label,
	hint,
	slot,
	url,
	onURL,
	adjust,
	onAdjust,
	defaultFit = "contain",
	pictures,
	onUpload,
}: {
	label: string;
	hint?: string;
	slot: Slot;
	url: string;
	onURL: (value: string) => void;
	adjust: CourtImageAdjust | undefined;
	onAdjust: (slot: Slot, next: CourtImageAdjust | undefined) => void;
	defaultFit?: "contain" | "fill";
	pictures: Record<string, string>;
	onUpload: (file: File) => Promise<string | undefined>;
}) => {
	const uploaded = url.startsWith(PIC);
	const shown = uploaded ? pictures[url.slice(PIC.length)] : url;
	const [uploading, setUploading] = useState(false);
	// A URL that does not resolve to an image draws nothing at all, which looks
	// exactly like the feature being broken. Load it here and say so.
	const [broken, setBroken] = useState(false);
	useEffect(() => {
		setBroken(false);
		if (!shown) {
			return;
		}
		let stale = false;
		const img = new Image();
		img.onerror = () => {
			if (!stale) {
				setBroken(true);
			}
		};
		img.src = shown;
		return () => {
			stale = true;
		};
	}, [shown]);

	const set = <K extends keyof CourtImageAdjust>(
		key: K,
		value: CourtImageAdjust[K],
	) => {
		const next: CourtImageAdjust = { ...adjust };
		if (value === undefined) {
			delete next[key];
		} else {
			next[key] = value;
		}
		onAdjust(slot, Object.keys(next).length > 0 ? next : undefined);
	};

	const slider = (
		key: "scale" | "opacity" | "dx" | "dy" | "rotate",
		text: string,
		min: number,
		max: number,
		step: number,
		fallback: number,
		format: (value: number) => string,
	) => (
		<label className="d-flex align-items-center gap-2 mb-1 small">
			<span style={{ width: "4.5rem" }} className="text-body-secondary">
				{text}
			</span>
			<input
				type="range"
				className="form-range flex-grow-1"
				min={min}
				max={max}
				step={step}
				value={adjust?.[key] ?? fallback}
				onChange={(e) => set(key, Number.parseFloat(e.target.value))}
			/>
			<span
				style={{ width: "3.25rem" }}
				className="text-end font-monospace text-body-secondary"
			>
				{format(adjust?.[key] ?? fallback)}
			</span>
		</label>
	);

	return (
		<div className="mb-3">
			<label className="form-label mb-1">
				{label}{" "}
				{hint ? <span className="text-body-secondary">{hint}</span> : null}
			</label>
			<div className="d-flex align-items-center gap-2">
				{uploaded ? (
					<>
						{shown ? (
							<img
								src={shown}
								alt=""
								style={{ height: 32, maxWidth: 96, objectFit: "contain" }}
							/>
						) : null}
						<span className="flex-grow-1 text-body-secondary">Uploaded</span>
						<button
							type="button"
							className="btn btn-sm btn-light-bordered"
							onClick={() => onURL("")}
						>
							Remove
						</button>
					</>
				) : (
					<>
						<input
							type="text"
							className="form-control"
							value={url}
							placeholder="https://..."
							onChange={(e) => onURL(e.target.value)}
						/>
						<label
							className={`btn btn-sm btn-light-bordered mb-0 flex-shrink-0${uploading ? " disabled" : ""}`}
						>
							Upload
							<input
								type="file"
								accept="image/*"
								className="d-none"
								disabled={uploading}
								onChange={async (e) => {
									const file = e.target.files?.[0];
									e.target.value = "";
									if (!file) {
										return;
									}
									setUploading(true);
									try {
										const next = await onUpload(file);
										if (next !== undefined) {
											onURL(next);
										}
									} finally {
										setUploading(false);
									}
								}}
							/>
						</label>
					</>
				)}
			</div>
			{broken ? (
				<div className="text-danger small mt-1">
					That URL didn&rsquo;t load as an image.
				</div>
			) : null}
			{url ? (
				<div className="mt-2 ps-2 border-start">
					{slider(
						"scale",
						"Size",
						0.1,
						4,
						0.05,
						1,
						(v) => `${Math.round(v * 100)}%`,
					)}
					{slider(
						"opacity",
						"Fade",
						0,
						1,
						0.05,
						1,
						(v) => `${Math.round(v * 100)}%`,
					)}
					{slider("dx", "Left/right", -47, 47, 0.5, 0, (v) => `${v} ft`)}
					{slider("dy", "Up/down", -25, 25, 0.5, 0, (v) => `${v} ft`)}
					{slider("rotate", "Rotate", -180, 180, 5, 0, (v) => `${v}\u00b0`)}
					<div className="d-flex align-items-center gap-3 mt-1">
						<div className="form-check form-check-inline mb-0">
							<input
								className="form-check-input"
								type="checkbox"
								id={`${slot}-fill`}
								checked={(adjust?.fit ?? defaultFit) === "fill"}
								onChange={(e) =>
									set("fit", e.target.checked ? "fill" : "contain")
								}
							/>
							<label
								className="form-check-label small"
								htmlFor={`${slot}-fill`}
							>
								Stretch to fit
							</label>
						</div>
						<button
							type="button"
							className="btn btn-sm btn-light-bordered"
							onClick={() => onAdjust(slot, undefined)}
							disabled={adjust === undefined}
						>
							Reset
						</button>
					</div>
				</div>
			) : null}
		</div>
	);
};
