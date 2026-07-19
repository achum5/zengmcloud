import { fetchWrapper } from "../../common/fetchWrapper.ts";
import { IMGBB_API_KEY } from "../../common/constants.ts";

// Strip the "data:image/png;base64," prefix, leaving raw base64 - the form imgbb
// (and Imgur, see takeScreenshotChunk.ts) expects for its `image` param.
const toBase64Payload = (dataURL: string): string => {
	const comma = dataURL.indexOf(",");
	return comma >= 0 ? dataURL.slice(comma + 1) : dataURL;
};

// Read a File/Blob (an uploaded or pasted image) into a data URL.
export const fileToDataURL = (file: Blob): Promise<string> =>
	new Promise((resolve, reject) => {
		const reader = new FileReader();
		reader.onload = () => resolve(reader.result as string);
		reader.onerror = () => reject(reader.error ?? new Error("Read failed"));
		reader.readAsDataURL(file);
	});

// Pull the first image out of a paste/drop, as a data URL, or undefined if the
// event carried no image.
export const imageDataURLFromDataTransfer = async (
	items: DataTransferItemList | null | undefined,
): Promise<string | undefined> => {
	if (!items) {
		return undefined;
	}
	for (let i = 0; i < items.length; i++) {
		const item = items[i]!;
		if (item.kind === "file" && item.type.startsWith("image/")) {
			const file = item.getAsFile();
			if (file) {
				return fileToDataURL(file);
			}
		}
	}
	return undefined;
};

// Upload an image (a File/Blob or a data URL) to imgbb and return the hosted
// URL. Runs on the UI thread via fetchWrapper, mirroring the Imgur uploader in
// takeScreenshotChunk.ts. Throws with a readable message on failure.
export const uploadToImgbb = async (image: Blob | string): Promise<string> => {
	if (!IMGBB_API_KEY || IMGBB_API_KEY === "YOUR_IMGBB_API_KEY") {
		throw new Error(
			"No imgbb API key configured. Set IMGBB_API_KEY in common/constants.ts.",
		);
	}

	const dataURL =
		typeof image === "string" ? image : await fileToDataURL(image);

	const data = await fetchWrapper({
		url: "https://api.imgbb.com/1/upload",
		method: "POST",
		data: {
			key: IMGBB_API_KEY,
			image: toBase64Payload(dataURL),
		},
	});

	const url = data?.data?.url ?? data?.data?.display_url;
	if (!data?.success || typeof url !== "string") {
		throw new Error(data?.error?.message ?? "imgbb upload failed");
	}
	return url;
};
