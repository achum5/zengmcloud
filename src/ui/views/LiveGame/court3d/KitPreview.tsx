import { useEffect, useMemo, useRef, useState } from "react";
import { makeCamera, MAIN_RIG } from "./camera.ts";
import type { PlayerState } from "./evaluate.ts";
import { kitsFor, type Look, type TeamDress } from "./figure.ts";
import { dressKit, kitArtOf, type KitArt } from "./kitArt.ts";
import { bodyOf } from "./poses.ts";
import { drawSprite, makeScratch, makeSpriteCache } from "./sprite.ts";

// A player turning slowly in a team's home or away uniform - drawn from a
// picture (a data URL), if there is one - with `wordmark` across his chest
// where the uniform has none of its own.
const KitPreview = ({
	dress,
	side,
	url,
	wordmark,
	width = 150,
	height = 240,
}: {
	dress: TeamDress;
	side: "home" | "away";
	url: string | undefined;
	wordmark: string;
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
		const img = new Image();
		img.src = url;
		img
			.decode()
			.then(() => {
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

	const look = useMemo((): Look => {
		const kits = kitsFor(
			side === "away" ? dress : undefined,
			side === "home" ? dress : undefined,
		);
		const kit = dressKit(kits[side === "away" ? 0 : 1], art);
		return {
			kit,
			...(art ? { kitArt: art } : {}),
			skin: "#8d5524",
			hair: "#1f1612",
			jerseyNumber: "23",
			name: "",
			lastName: "PLAYER",
			wordmark: kit.chestText ?? wordmark,
		};
	}, [dress, side, art, wordmark]);

	useEffect(() => {
		const cv = canvas.current;
		const ctx = cv?.getContext("2d");
		if (!cv || !ctx) {
			return;
		}
		const dpr = Math.min(2, window.devicePixelRatio || 1);
		cv.width = Math.round(width * dpr);
		cv.height = Math.round(height * dpr);
		const body = bodyOf();
		const k = cv.height / (body.H * 1.3);
		const cam = makeCamera(
			{ x: 47, width: cv.width / k, y: 25, z: body.H * 0.5 },
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
			const st = {
				pid: 0,
				team: side === "away" ? 0 : 1,
				shown: true,
				x: 47,
				y: 25,
				z: 0,
				yaw,
				anim: "ready",
				phase: 0.3,
				moving: false,
				holding: false,
			} as PlayerState;
			drawSprite(ctx, scratch, cam, st, body, look, 1, cache);
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
				background: "#c99a5e",
				borderRadius: 6,
			}}
		/>
	);
};

export default KitPreview;
