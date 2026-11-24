import {
	type CSSProperties,
	type PointerEvent,
	type RefObject,
	useEffect,
	useRef,
	useState,
} from "react";

// THE CONTROLS OVER A 3D PICTURE, the way a video player's work (the
// Nightly Recap's, a live game's or a replay's): out of sight until the
// picture is hovered or touched, and the picture full size at a tap.

// How long the controls stay up over the picture after a touch.
const CONTROLS_MS = 2500;
// Below this wide, the 3D court's picture is 4:3 (see court3d/broadcast.ts).
export const NARROW_W = 560;

// The controls hidden: still there, and still in the same place, so
// nothing moves when they come back.
const HIDDEN: CSSProperties = { opacity: 0, pointerEvents: "none" };

// Up for a moment after the mouse moves over the picture or it is touched
// (`wake`, on the picture), and all the while `hold` is (paused, not yet
// started) - and never put away under the mouse, with one of their menus
// open, or the keyboard in them.
export const useControlsUp = (hold: boolean) => {
	const [awake, setAwake] = useState(false);
	const ref = useRef<HTMLDivElement | null>(null);
	// (The mouse's, not a finger's: a touch screen's hover sticks.)
	const over = useRef(false);
	const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
	const busy = () => {
		const el = ref.current;
		if (!el) {
			return false;
		}
		if (over.current || el.querySelector('[aria-expanded="true"]')) {
			return true;
		}
		try {
			return el.querySelector(":focus-visible") !== null;
		} catch {
			return false;
		}
	};
	const arm = () => {
		clearTimeout(timer.current);
		timer.current = setTimeout(() => {
			if (busy()) {
				arm();
			} else {
				setAwake(false);
			}
		}, CONTROLS_MS);
	};
	const wake = () => {
		setAwake(true);
		arm();
	};
	useEffect(() => () => clearTimeout(timer.current), []);

	const up = awake || hold;
	return {
		up,
		wake,
		// For the controls' own box.
		controls: {
			ref,
			onPointerEnter: (event: PointerEvent) => {
				if (event.pointerType === "mouse") {
					over.current = true;
				}
			},
			onPointerLeave: () => {
				over.current = false;
			},
			onFocus: wake,
		},
		hidden: up ? undefined : HIDDEN,
	};
};

// FULL SIZE: the page's own full screen where there is one (not on an
// iPhone), and over the whole page either way - `rootRef` the box laid
// over it (see FULL_SIZE).
export const useFullSize = (rootRef: RefObject<HTMLElement | null>) => {
	const [big, setBig] = useState(false);
	// Where the page was scrolled to: back there after the full screen, which
	// leaves it at the top.
	const scrolledRef = useRef<number | undefined>(undefined);
	const open = () => {
		setBig(true);
		const el = rootRef.current;
		if (el && document.fullscreenEnabled && !document.fullscreenElement) {
			scrolledRef.current = window.scrollY;
			el.requestFullscreen?.().catch(() => {
				scrolledRef.current = undefined;
			});
		}
	};
	useEffect(() => {
		const onChange = () => {
			const y = scrolledRef.current;
			if (document.fullscreenElement || y === undefined) {
				return;
			}
			scrolledRef.current = undefined;
			window.scrollTo(0, y);
			requestAnimationFrame(() => {
				window.scrollTo(0, y);
			});
		};
		document.addEventListener("fullscreenchange", onChange);
		return () => {
			document.removeEventListener("fullscreenchange", onChange);
		};
	}, []);
	const close = () => {
		setBig(false);
		if (document.fullscreenElement) {
			document.exitFullscreen?.().catch(() => {});
		}
	};
	useEffect(() => {
		if (!big) {
			return;
		}
		const onChange = () => {
			if (!document.fullscreenElement) {
				setBig(false);
			}
		};
		const onKey = (event: KeyboardEvent) => {
			if (event.key === "Escape") {
				setBig(false);
				if (document.fullscreenElement) {
					document.exitFullscreen?.().catch(() => {});
				}
			}
		};
		const had = document.body.style.overflow;
		document.body.style.overflow = "hidden";
		document.addEventListener("fullscreenchange", onChange);
		window.addEventListener("keydown", onKey);
		return () => {
			document.body.style.overflow = had;
			document.removeEventListener("fullscreenchange", onChange);
			window.removeEventListener("keydown", onKey);
		};
	}, [big]);
	return { big, open, close };
};

// The box over the whole page, full size: the picture in the middle of it.
export const FULL_SIZE: CSSProperties = {
	position: "fixed",
	inset: 0,
	zIndex: 1070,
	background: "#000",
	display: "flex",
	alignItems: "center",
	justifyContent: "center",
};

// The picture's width full size, as big as the screen takes it: 16:9, or
// 4:3 where that would come out under NARROW_W wide (a phone held upright),
// as the picture itself goes - worked out from the screen, so turning a
// phone round switches it back.
export const useFullSizeWidth = (big: boolean): string | undefined => {
	const [, setTurns] = useState(0);
	useEffect(() => {
		if (!big) {
			return;
		}
		const onResize = () => {
			setTurns((n) => n + 1);
		};
		window.addEventListener("resize", onResize);
		return () => {
			window.removeEventListener("resize", onResize);
		};
	}, [big]);
	if (!big) {
		return undefined;
	}
	const narrow =
		Math.min(window.innerWidth, (window.innerHeight * 16) / 9) < NARROW_W;
	return `min(100vw, calc(100dvh * ${narrow ? "4 / 3" : "16 / 9"}))`;
};

const FullSizeIcon = ({ big }: { big: boolean }) => (
	<svg
		aria-hidden
		viewBox="0 0 16 16"
		width="12"
		height="12"
		fill="none"
		stroke="currentColor"
		strokeWidth="2"
		style={{ display: "block" }}
	>
		{big ? (
			<path d="M6 1v5H1M10 1v5h5M6 15v-5H1M10 15v-5h5" />
		) : (
			<path d="M1 6V1h5M15 6V1h-5M1 10v5h5M15 10v5h-5" />
		)}
	</svg>
);

export const FullSizeButton = ({
	big,
	open,
	close,
}: {
	big: boolean;
	open: () => void;
	close: () => void;
}) => (
	<button
		type="button"
		className="btn btn-sm btn-light-bordered"
		title={big ? "Exit full size" : "Full size"}
		aria-label={big ? "Exit full size" : "Full size"}
		onClick={big ? close : open}
		style={{
			alignSelf: "stretch",
			display: "flex",
			alignItems: "center",
		}}
	>
		<FullSizeIcon big={big} />
	</button>
);
