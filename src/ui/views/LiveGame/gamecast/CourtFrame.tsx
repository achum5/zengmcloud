import {
	type ReactNode,
	useCallback,
	useEffect,
	useRef,
	useState,
} from "react";
import { ModalContainer } from "../../../components/Modal.tsx";
import {
	FULL_SIZE,
	FullSizeButton,
	useControlsUp,
	useFullSize,
	useFullSizeWidth,
} from "../../../components/pictureControls.tsx";

// THE COURT AND ITS CONTROLS (play / pause / speed, the key plays' gear,
// full size) laid over its top right the way the Nightly Recap's are: out of
// sight until the picture is hovered or touched, and up while it is paused.
// The picture full size at a tap, the controls on it still (the gear's
// settings too, opened over it).

// Everything `el` sits in that is stacked on its own (the court pinned to
// the top of a phone's screen, under the page's own header) lifted over the
// page, till the returned undo.
const liftOver = (el: HTMLElement) => {
	const lifted: [HTMLElement, string][] = [];
	for (
		let up = el.parentElement;
		up && up !== document.body;
		up = up.parentElement
	) {
		const style = getComputedStyle(up);
		if (style.position !== "static" && style.zIndex !== "auto") {
			lifted.push([up, up.style.zIndex]);
			up.style.zIndex = "1070";
		}
	}
	return () => {
		for (const [up, had] of lifted) {
			up.style.zIndex = had;
		}
	};
};

export const CourtFrame = ({
	court,
	controls,
	paused,
	fullSize,
}: {
	court: ReactNode;
	controls: ReactNode;
	paused?: boolean;
	// Whether it can go full size (the 3D court: 16:9, or 4:3 on a phone).
	fullSize?: boolean;
}) => {
	const outerRef = useRef<HTMLDivElement | null>(null);
	const rootRef = useRef<HTMLDivElement | null>(null);
	const [root, setRoot] = useState<HTMLDivElement | null>(null);
	const onRoot = useCallback((el: HTMLDivElement | null) => {
		rootRef.current = el;
		setRoot(el);
	}, []);
	const { big, open, close } = useFullSize(rootRef);
	const width = useFullSizeWidth(big);
	const shown = useControlsUp(!!paused);

	// Full size, the page under it keeps its place (the court's room kept),
	// and the court is over all of it (see liftOver).
	const [room, setRoom] = useState<number | undefined>();
	const openBig = () => {
		setRoom(outerRef.current?.offsetHeight);
		open();
	};
	useEffect(() => {
		if (big && root) {
			return liftOver(root);
		}
	}, [big, root]);

	return (
		<div
			ref={outerRef}
			className="gamecast-court"
			style={big && room !== undefined ? { height: room } : undefined}
		>
			<div ref={onRoot} style={big ? FULL_SIZE : undefined}>
				<div
					className="gamecast-court-picture"
					style={big ? { width } : undefined}
					onPointerMove={shown.wake}
					onPointerDown={shown.wake}
				>
					<ModalContainer.Provider value={big ? root : null}>
						{court}
						<div
							{...shown.controls}
							className="gamecast-controls d-flex align-items-center gap-1"
							style={shown.hidden}
						>
							{controls}
							{fullSize ? (
								<FullSizeButton big={big} open={openBig} close={close} />
							) : null}
						</div>
					</ModalContainer.Provider>
				</div>
			</div>
		</div>
	);
};
