import streamSaver from "streamsaver";
import { downloadFile } from "./downloadFile.ts";

const HAS_FILE_SYSTEM_ACCESS_API = !!window.showSaveFilePicker;

// Why is this in UI? streamsaver does not work in worker. Otherwise it would be better there.
// If this is ever moved to the worker, be careful about file system access API crashing Chrome 93/94 https://dumbmatter.com/file-system-access-worker-bug/
const downloadFileStream = async (
	stream: boolean,
	filename: string,
	gzip: boolean,
) => {
	// Fall back to streamSaver's own download mechanism (a service-worker-backed
	// download to the default folder). Used when the File System Access API isn't
	// available, or when it fails on a restricted filesystem (see below).
	const streamSaverFallback = () => {
		// This is needed because we asynchronously load the stream polyfill
		streamSaver.WritableStream = window.WritableStream;
		return streamSaver.createWriteStream(filename);
	};

	if (stream) {
		if (HAS_FILE_SYSTEM_ACCESS_API) {
			try {
				const fileHandle = await window.showSaveFilePicker({
					suggestedName: filename,
					types: [
						gzip
							? {
									description: "Gzip Files",
									accept: {
										"application/gzip": [".gz"],
									},
								}
							: {
									description: "JSON Files",
									accept: {
										"application/json": [".json"],
									},
								},
					],
				});

				return await fileHandle.createWritable();
			} catch (error) {
				// The user cancelled the save dialog - respect that, don't download.
				if ((error as Error)?.name === "AbortError") {
					throw error;
				}
				// Some environments (sandboxed/flatpak Chrome, certain filesystems)
				// throw NoModificationAllowedError from createWritable. Rather than
				// fail the whole export, fall back to the default-folder download.
				console.warn(
					"File System Access API save failed, falling back to a normal download",
					error,
				);
				return streamSaverFallback();
			}
		}

		return streamSaverFallback();
	}

	const contents: Uint8Array<ArrayBuffer>[] = [];

	const fileStream = new WritableStream({
		write(chunk) {
			contents.push(chunk);
		},
		close() {
			downloadFile(
				filename,
				contents,
				gzip ? "application/gzip" : "application/json",
			);
		},
	});

	return fileStream;
};

export default downloadFileStream;
