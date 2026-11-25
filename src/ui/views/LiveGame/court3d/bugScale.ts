import { useEffect, useState } from "react";

// HOW BIG A SCORE BUG IS DRAWN: a share of the width of the picture it is
// on, and the same share on every picture on the screen - the Nightly
// Recap's small one beside the scores, the game's own, the game full size -
// so a small picture is the big one shrunk, the bug and all. That share is
// the bug's own on TV, or on a phone more, enough to read (see
// CustomScoreBug's MIN_*), worked out on the widest picture the screen has:
// one across the page, less the page's margins.

// The page's margins either side of a picture across it, px, on a phone.
const PAGE_EDGES = 16;

const screenPicture = () =>
	typeof document === "undefined"
		? 1280
		: Math.max(1, document.documentElement.clientWidth - PAGE_EDGES);

// The width of the widest picture on this screen, px, kept up to date as the
// window changes.
export const useScreenPicture = () => {
	const [width, setWidth] = useState(screenPicture);
	useEffect(() => {
		const update = () => {
			setWidth(screenPicture());
		};
		update();
		window.addEventListener("resize", update);
		return () => {
			window.removeEventListener("resize", update);
		};
	}, []);
	return width;
};
