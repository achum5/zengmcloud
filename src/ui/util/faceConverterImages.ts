import { downloadFile } from "./downloadFile.ts";

// Photos for the face converter, labelled so a chat AI can tie each face to a
// player id no matter what order the attachments arrive in.
//
// Everything here draws the photos onto a canvas, which only works when the
// image host allows cross-origin reads (CORS). When it doesn't, the canvas is
// "tainted" and nothing can be read back out of it - that surfaces as a
// PhotoReadError, and the page falls back to the on-screen sheet, which the
// user can screenshot instead.

export type SheetEntry = {
	label: string;
	photo: string | undefined;
};

export class PhotoReadError extends Error {}

// NBA headshots are 260x190, and Claude reads an image of up to about 1568px on
// its long side (1.15 megapixels) without shrinking it - so 19 headshots at
// full size, 5 across, fit in one image with nothing lost.
// A sheet holds at most this many photos: 20 headshots at full size is what
// fits under Claude's resize limit. A bigger batch becomes several sheets,
// each attached as its own image, so no photo is shrunk to fit.
export const SHEET_MAX = 20;

export const TILE_W = 260;
export const TILE_H = 190;
export const LABEL_H = 24;
const MAX_SIDE = 1568;
const MAX_PIXELS = 1_150_000;

export const sheetLayout = (count: number) => {
	const cols =
		count <= 20
			? Math.min(Math.max(count, 1), 5)
			: Math.ceil(Math.sqrt(count * 1.4));
	const rows = Math.ceil(count / cols);
	const width = cols * TILE_W;
	const height = rows * (TILE_H + LABEL_H);
	const scale = Math.min(
		1,
		MAX_SIDE / width,
		MAX_SIDE / height,
		Math.sqrt(MAX_PIXELS / (width * height)),
	);
	return { cols, rows, scale };
};

const loadDirect = (url: string) =>
	new Promise<HTMLImageElement>((resolve, reject) => {
		const img = new Image();
		// Asks the host for CORS headers. Without them the load fails outright,
		// rather than succeeding and tainting the canvas later.
		img.crossOrigin = "anonymous";
		img.onload = () => {
			resolve(img);
		};
		img.onerror = () => {
			reject(new PhotoReadError(url));
		};
		img.src = url;
	});

// Most photo hosts (NBA.com, basketball-reference, ESPN) send no CORS
// headers, so a canvas can't read their images and the sheet can't be made
// into one picture. wsrv.nl is a free public image relay that fetches the
// photo and serves it back with CORS allowed - the same pixels, now readable.
const RELAY = "https://wsrv.nl/?url=";

const loadImage = async (url: string) => {
	try {
		return await loadDirect(url);
	} catch (error) {
		if (!/^https?:/.test(url)) {
			throw error;
		}
		return loadDirect(`${RELAY}${encodeURIComponent(url)}`);
	}
};

const drawTile = (
	ctx: CanvasRenderingContext2D,
	img: HTMLImageElement | undefined,
	x: number,
	y: number,
	w: number,
	h: number,
	labelH: number,
	label: string,
) => {
	ctx.fillStyle = "#ffffff";
	ctx.fillRect(x, y, w, h + labelH);
	if (img) {
		const s = Math.min(w / img.naturalWidth, h / img.naturalHeight);
		const dw = img.naturalWidth * s;
		const dh = img.naturalHeight * s;
		ctx.drawImage(img, x + (w - dw) / 2, y + (h - dh) / 2, dw, dh);
	}
	ctx.fillStyle = "#ffffff";
	ctx.fillRect(x, y + h, w, labelH);
	ctx.fillStyle = "#000000";
	ctx.font = `bold ${Math.round(labelH * 0.58)}px sans-serif`;
	ctx.textBaseline = "middle";
	ctx.fillText(label, x + 6, y + h + labelH / 2, w - 12);
	ctx.strokeStyle = "#999999";
	ctx.lineWidth = 1;
	ctx.strokeRect(x + 0.5, y + 0.5, w - 1, h + labelH - 1);
};

// A photo that can't be read even through the relay leaves its tile blank
// rather than sinking the whole sheet; only when none can be read is it an
// error, and the page falls back to the on-screen sheet.
export let missedPhotos = 0;
const loadAll = async (entries: SheetEntry[]) => {
	const images = await Promise.all(
		entries.map((entry) =>
			entry.photo
				? loadImage(entry.photo).catch(() => undefined)
				: Promise.resolve(undefined),
		),
	);
	const wanted = entries.filter((entry) => entry.photo).length;
	const got = images.filter((img) => img !== undefined).length;
	missedPhotos = wanted - got;
	if (wanted > 0 && got === 0) {
		throw new PhotoReadError("no photo could be read");
	}
	return images;
};

const toBlob = (canvas: HTMLCanvasElement) =>
	new Promise<Blob>((resolve, reject) => {
		try {
			canvas.toBlob((blob) => {
				if (blob) {
					resolve(blob);
				} else {
					reject(new PhotoReadError("empty canvas"));
				}
			}, "image/png");
		} catch {
			// SecurityError: a tainted canvas.
			reject(new PhotoReadError("tainted canvas"));
		}
	});

export const buildSheet = async (entries: SheetEntry[]) => {
	const images = await loadAll(entries);
	const { cols, rows, scale } = sheetLayout(entries.length);
	const w = Math.round(TILE_W * scale);
	const h = Math.round(TILE_H * scale);
	const labelH = Math.max(18, Math.round(LABEL_H * scale));

	const canvas = document.createElement("canvas");
	canvas.width = cols * w;
	canvas.height = rows * (h + labelH);
	const ctx = canvas.getContext("2d")!;
	ctx.fillStyle = "#ffffff";
	ctx.fillRect(0, 0, canvas.width, canvas.height);
	for (const [i, entry] of entries.entries()) {
		drawTile(
			ctx,
			images[i],
			(i % cols) * w,
			Math.floor(i / cols) * (h + labelH),
			w,
			h,
			labelH,
			entry.label,
		);
	}
	return toBlob(canvas);
};

// One labelled image per player, at the photo's own size (capped, since a
// 1040px original tells the model nothing a 520px one doesn't).
export const buildFiles = async (entries: SheetEntry[]) => {
	const images = await loadAll(entries);
	const blobs: Blob[] = [];
	for (const [i, entry] of entries.entries()) {
		const img = images[i];
		const scale = img ? Math.min(1, 520 / img.naturalWidth) : 1;
		const w = img ? Math.round(img.naturalWidth * scale) : TILE_W;
		const h = img ? Math.round(img.naturalHeight * scale) : TILE_H;
		const labelH = Math.max(LABEL_H, Math.round(w * 0.08));
		const canvas = document.createElement("canvas");
		canvas.width = w;
		canvas.height = h + labelH;
		drawTile(canvas.getContext("2d")!, img, 0, 0, w, h, labelH, entry.label);
		blobs.push(await toBlob(canvas));
	}
	return blobs;
};

export const copyImageToClipboard = async (blob: Blob) => {
	await navigator.clipboard.write([new ClipboardItem({ "image/png": blob })]);
};

// The folder the last batch was saved to, for this page session. Saving into
// the same folder every time means the chat app's file picker opens right on
// it: select all, attach, done.
let folder: any;

// What a batch writes, so the next batch can clear it out and the folder only
// ever holds the current batch.
const BATCH_FILE = /^\d{2} .*\.png$|^prompt\.md$/;

export const saveBatchFiles = async (
	files: { name: string; blob: Blob }[],
): Promise<"folder" | "downloads"> => {
	const picker = (window as any).showDirectoryPicker;
	if (typeof picker === "function") {
		if (!folder) {
			folder = await picker.call(window, {
				id: "face-converter",
				mode: "readwrite",
			});
		}
		for await (const [name] of folder.entries()) {
			if (BATCH_FILE.test(name)) {
				await folder.removeEntry(name);
			}
		}
		for (const file of files) {
			const handle = await folder.getFileHandle(file.name, { create: true });
			const writable = await handle.createWritable();
			await writable.write(file.blob);
			await writable.close();
		}
		return "folder";
	}

	// No folder access (Firefox, Safari): plain downloads. The browser asks
	// once to allow several downloads from this page.
	for (const file of files) {
		const bytes = new Uint8Array(await file.blob.arrayBuffer());
		downloadFile(file.name, [bytes], file.blob.type || "text/markdown");
	}
	return "downloads";
};
