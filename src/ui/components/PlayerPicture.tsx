import type { FaceConfig } from "facesjs";
import { useState } from "react";
import { MyFace } from "./MyFace.tsx";

const imgStyle = {
	maxHeight: "100%",
	maxWidth: "100%",
};

export const PlayerPicture = ({
	colors,
	face,
	imgURL,
	jersey,
	lazy,
}: {
	colors?: [string, string, string];
	face?: FaceConfig;
	imgURL?: string;
	jersey?: string;
	lazy?: boolean;
}) => {
	// (A link that doesn't load - an old photo gone from wherever it was - and
	// he is drawn as his face, not as a broken picture.)
	const [failed, setFailed] = useState<string | undefined>();
	if (imgURL && (failed !== imgURL || !face)) {
		return (
			<img
				alt="Player"
				src={imgURL}
				style={imgStyle}
				onError={() => {
					setFailed(imgURL);
				}}
			/>
		);
	}

	if (face) {
		return <MyFace colors={colors} face={face} jersey={jersey} lazy={lazy} />;
	}

	return null;
};
