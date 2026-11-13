import { lazy, Suspense, useEffect, useRef, useState } from "react";
import type { HeadlineSceneSpec } from "../../../../common/headlineScene.ts";

// A HEADLINE'S OWN LITTLE SCENE, PLAYING (see common/headlineScene.ts): a
// block as wide as what it is put in, the 3D building in it, no controls.
// It plays while `playing` and on the screen, holds where it is otherwise,
// stops on its last frame (`onEnd`), and plays again from the top if
// `playing` comes back after that. Where there is no scene to make of it
// (`onNone` - see the worker's headlineScene), it is not there at all.
//
// Light to have on a page: the 3D court is fetched only once the block has
// been on the screen, after the page has painted - never on the way in.

const Stage = lazy(() => import("./HeadlineStage.tsx"));

const BOX = {
	position: "relative",
	width: "100%",
	background: "#040406",
	borderRadius: 6,
	overflow: "hidden",
} as const;

export const HeadlineScene = ({
	scene,
	playing,
	onEnd,
	onNone,
}: {
	scene: HeadlineSceneSpec;
	playing: boolean;
	onEnd?: () => void;
	onNone?: () => void;
}) => {
	const ref = useRef<HTMLDivElement | null>(null);
	// On the screen now; and whether it ever has been, with the page
	// painted - the 3D court is asked for from then.
	const [seen, setSeen] = useState(false);
	const [wanted, setWanted] = useState(false);
	const [shown, setShown] = useState(
		typeof document === "undefined" || !document.hidden,
	);
	// Narrower than the 3D picture's own squarer shape starts (see
	// NARROW_W): the box it waits in is the shape it will be.
	const [narrow, setNarrow] = useState(false);
	// Which scene (by what it says, not which object says it: the page may
	// hand over a new one each time it draws), and whether there is nothing
	// to make of it (see the worker's headlineScene).
	const which = `${scene.type}:${scene.pid}`;
	const [noneOf, setNoneOf] = useState<string>();
	const none = noneOf === which;

	useEffect(() => {
		const el = ref.current;
		if (!el) {
			return;
		}
		const measure = () => {
			setNarrow(el.clientWidth < 560);
		};
		measure();
		const sized = new ResizeObserver(measure);
		sized.observe(el);
		const watched = new IntersectionObserver(
			(entries) => {
				setSeen(entries.some((e) => e.isIntersecting));
			},
			{ threshold: 0.35 },
		);
		watched.observe(el);
		const onShown = () => {
			setShown(!document.hidden);
		};
		document.addEventListener("visibilitychange", onShown);
		return () => {
			sized.disconnect();
			watched.disconnect();
			document.removeEventListener("visibilitychange", onShown);
		};
		// (The block is not there while there is no scene.)
	}, [none]);

	useEffect(() => {
		if (!seen || wanted) {
			return;
		}
		// After the page has painted.
		let timer: ReturnType<typeof setTimeout> | undefined;
		const frame = requestAnimationFrame(() => {
			timer = setTimeout(() => {
				setWanted(true);
			}, 0);
		});
		return () => {
			cancelAnimationFrame(frame);
			clearTimeout(timer);
		};
	}, [seen, wanted]);

	if (__SPORT !== "basketball" || none) {
		return null;
	}
	return (
		<div ref={ref} style={{ ...BOX, aspectRatio: narrow ? "4 / 3" : "16 / 9" }}>
			{wanted ? (
				<Suspense fallback={null}>
					<Stage
						// (Another scene: all of it from nothing.)
						key={which}
						scene={scene}
						playing={playing && seen && shown}
						onEnd={onEnd}
						onNone={() => {
							setNoneOf(which);
							onNone?.();
						}}
					/>
				</Suspense>
			) : null}
		</div>
	);
};

export default HeadlineScene;
