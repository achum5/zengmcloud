import { useEffect, useMemo, useRef, useState } from "react";
import type { HelmetLook } from "../../../../common/types.ts";
import { kitArtOf, loadKitPicture, type KitArt } from "../court3d/kitArt.ts";
import { logoPicture } from "../court3d/logoPicture.ts";
import { makeScratch, makeSpriteCache } from "../court3d/sprite.ts";
import { footballBody } from "./body.ts";
import { makeFieldCamera, MAIN_RIG } from "./camera.ts";
import {
	dressFieldKit,
	fieldKitsFor,
	type FieldGear,
	type FieldLook,
} from "./kit.ts";
import { drawFieldPlayer, type FieldPlayerState } from "./sprite.ts";

// A football player turning slowly in a team's home or away uniform - drawn
// from a picture (a data URL), if there is one (court3d's KitPreview, for
// football).
const GEAR: FieldGear = {
	mask: "bars",
	cleat: "#18181c",
	cleatSole: "#f4f2ec",
	cleatAccent: "#f4f2ec",
};

const FieldKitPreview = ({
	colors,
	helmet,
	imgURL,
	side,
	url,
	width = 150,
	height = 240,
}: {
	colors: [string, string, string];
	helmet?: HelmetLook;
	imgURL?: string;
	side: "home" | "away";
	url: string | undefined;
	width?: number;
	height?: number;
}) => {
	const canvas = useRef<HTMLCanvasElement>(null);
	const [art, setArt] = useState<KitArt>();
	useEffect(() => {
		if (!url) {
			setArt(undefined);
			return;
		}
		let alive = true;
		loadKitPicture(url)
			.then((img) => {
				if (alive) {
					setArt(kitArtOf(img, url.slice(-48)));
				}
			})
			.catch(() => {
				if (alive) {
					setArt(undefined);
				}
			});
		return () => {
			alive = false;
		};
	}, [url]);
	const [logo, setLogo] = useState<HTMLCanvasElement>();
	useEffect(() => {
		let alive = true;
		void logoPicture(imgURL).then((made) => {
			if (alive) {
				setLogo(made);
			}
		});
		return () => {
			alive = false;
		};
	}, [imgURL]);

	const look = useMemo((): FieldLook => {
		const kits = fieldKitsFor(colors, colors, [helmet, helmet]);
		return {
			kit: dressFieldKit(kits[side === "away" ? 0 : 1], art),
			gear: GEAR,
			...(art ? { kitArt: art } : {}),
			...(logo && helmet?.logo !== false ? { logo } : {}),
			skin: "#8d5524",
			jerseyNumber: "23",
			lastName: "PLAYER",
		};
	}, [colors, helmet, side, art, logo]);

	useEffect(() => {
		const cv = canvas.current;
		const ctx = cv?.getContext("2d");
		if (!cv || !ctx) {
			return;
		}
		const dpr = Math.min(2, window.devicePixelRatio || 1);
		cv.width = Math.round(width * dpr);
		cv.height = Math.round(height * dpr);
		const body = footballBody();
		const k = cv.height / (body.H * 1.3);
		const cam = makeFieldCamera(
			{ x: 180, width: cv.width / k, y: 80, z: body.H * 0.5 },
			cv.width,
			cv.height,
			MAIN_RIG,
		);
		const scratch = makeScratch();
		const cache = makeSpriteCache();
		let raf = 0;
		const t0 = performance.now();
		const frame = (now: number) => {
			// Facing out to begin with, a turn every five seconds.
			const yaw = Math.PI / 2 + ((now - t0) / 5000) * Math.PI * 2;
			ctx.clearRect(0, 0, cv.width, cv.height);
			const st: FieldPlayerState = {
				id: 1,
				team: side === "away" ? 0 : 1,
				x: 180,
				y: 80,
				z: 0,
				yaw,
				move: "stand",
				phase: 0,
			};
			drawFieldPlayer(ctx, scratch, cam, st, body, look, 1, cache);
			raf = requestAnimationFrame(frame);
		};
		raf = requestAnimationFrame(frame);
		return () => {
			cancelAnimationFrame(raf);
		};
	}, [look, side, width, height]);

	return (
		<canvas
			ref={canvas}
			style={{
				width,
				height,
				background: "#4d8a3c",
				borderRadius: 6,
			}}
		/>
	);
};

export default FieldKitPreview;
