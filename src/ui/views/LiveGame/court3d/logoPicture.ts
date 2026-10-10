// A TEAM'S LOGO, AS A PICTURE OF ITS OWN.
//
// Drawn small - for a warm-up top, a banner - if it can be read back: a
// logo from a site that will not say it may be is left off (and whatever it
// was for goes without), so nothing drawn from it is ever spoiled for
// reading.
const LOGO_PX = 128;
export const logoPicture = async (
	url: string | undefined,
): Promise<HTMLCanvasElement | undefined> => {
	if (!url || typeof document === "undefined") {
		return undefined;
	}
	const img = new Image();
	img.crossOrigin = "anonymous";
	img.src = url;
	try {
		await img.decode();
	} catch {
		return undefined;
	}
	const w0 = img.naturalWidth || LOGO_PX;
	const h0 = img.naturalHeight || LOGO_PX;
	const k = LOGO_PX / Math.max(w0, h0);
	const cv = document.createElement("canvas");
	cv.width = Math.max(1, Math.round(w0 * k));
	cv.height = Math.max(1, Math.round(h0 * k));
	const g = cv.getContext("2d", { willReadFrequently: true });
	if (!g) {
		return undefined;
	}
	g.drawImage(img, 0, 0, cv.width, cv.height);
	try {
		g.getImageData(0, 0, 1, 1);
	} catch {
		return undefined;
	}
	return cv;
};
