import { useRef, useState } from "react";
import {
	fileToDataURL,
	imageDataURLFromDataTransfer,
	uploadToImgbb,
} from "../util/uploadToImgbb.ts";

// A small drop/paste/pick zone that uploads an image to imgbb and hands the
// hosted URL back via onUploaded. Accepts a file (click to pick), a pasted
// image (click the zone to focus, then Ctrl/Cmd+V), or a dragged-in image.
const ImageUploader = ({
	onUploaded,
	disabled,
}: {
	onUploaded: (url: string) => void | Promise<void>;
	disabled?: boolean;
}) => {
	const [uploading, setUploading] = useState(false);
	const [error, setError] = useState<string | undefined>();
	const inputRef = useRef<HTMLInputElement>(null);

	const handleImage = async (image: Blob | string) => {
		setUploading(true);
		setError(undefined);
		try {
			const url = await uploadToImgbb(image);
			await onUploaded(url);
		} catch (error) {
			setError(error instanceof Error ? error.message : "Upload failed");
		} finally {
			setUploading(false);
		}
	};

	const busy = disabled || uploading;

	return (
		<div
			className="border rounded p-3 text-center"
			tabIndex={0}
			style={{ outline: "none", cursor: busy ? "default" : "text" }}
			onPaste={async (event) => {
				if (busy) {
					return;
				}
				const url = await imageDataURLFromDataTransfer(
					event.clipboardData?.items,
				);
				if (url) {
					event.preventDefault();
					await handleImage(url);
				}
			}}
			onDragOver={(event) => event.preventDefault()}
			onDrop={async (event) => {
				event.preventDefault();
				if (busy) {
					return;
				}
				const url = await imageDataURLFromDataTransfer(
					event.dataTransfer?.items,
				);
				if (url) {
					await handleImage(url);
				} else if (event.dataTransfer?.files[0]) {
					await handleImage(await fileToDataURL(event.dataTransfer.files[0]));
				}
			}}
		>
			<input
				ref={inputRef}
				type="file"
				accept="image/*"
				className="d-none"
				onChange={async (event) => {
					const file = event.target.files?.[0];
					event.target.value = "";
					if (file) {
						await handleImage(file);
					}
				}}
			/>
			<button
				type="button"
				className="btn btn-secondary btn-sm"
				disabled={busy}
				onClick={() => inputRef.current?.click()}
			>
				{uploading ? "Uploading…" : "Choose image"}
			</button>
			<div className="text-body-secondary small mt-2">
				or click here and paste, or drop an image
			</div>
			{error ? <div className="text-danger small mt-2">{error}</div> : null}
		</div>
	);
};

export default ImageUploader;
