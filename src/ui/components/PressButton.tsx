import { useState } from "react";
import { useLocal } from "../util/local.ts";
import { HeadlineScene } from "../views/LiveGame/court3d/HeadlineScene.tsx";
import { Modal } from "./Modal.tsx";
import { PlayButton } from "./PlayButton.tsx";

// The press conference a team held for who it traded for or signed (see the
// worker's pressConference.ts), from wherever the move is listed: the 3D room
// is fetched only once this is pressed. A trade both teams held one for: the
// one, then the other, picked between at the top.
export const PressButton = ({
	eid,
	tids,
	className = "ms-2",
}: {
	eid: number;
	tids: number[];
	className?: string;
}) => {
	const { teamInfoCache } = useLocal(["teamInfoCache"]);
	const [show, setShow] = useState(false);
	const [at, setAt] = useState(0);
	const tid = tids[Math.min(at, tids.length - 1)]!;
	const nameOf = (t: number) => {
		const info = teamInfoCache[t];
		return info ? `${info.region} ${info.name}` : "";
	};
	const title =
		tids.length > 1
			? "Watch the press conferences"
			: nameOf(tid)
				? `Watch the ${nameOf(tid)} press conference`
				: "Watch the press conference";
	return (
		<>
			<PlayButton
				className={className}
				onClick={() => {
					setAt(0);
					setShow(true);
				}}
				title={title}
			/>
			{show ? (
				<Modal
					centered
					show
					size="lg"
					onHide={() => {
						setShow(false);
					}}
				>
					<Modal.Header closeButton>
						<Modal.Title>{nameOf(tid)}</Modal.Title>
						{tids.length > 1 ? (
							<div className="btn-group btn-group-sm ms-3" role="group">
								{tids.map((t, i) => (
									<button
										key={t}
										type="button"
										className={
											i === at
												? "btn btn-secondary"
												: "btn btn-outline-secondary"
										}
										onClick={() => {
											setAt(i);
										}}
									>
										{teamInfoCache[t]?.abbrev ?? t}
									</button>
								))}
							</div>
						) : null}
					</Modal.Header>
					<Modal.Body className="p-2">
						<HeadlineScene
							key={tid}
							scene={{ type: "pressConference", eid, tid }}
							playing
							onEnd={() => {
								// (Both teams': on to the other one's.)
								if (at + 1 < tids.length) {
									setTimeout(() => {
										setAt((i) => (i === at ? i + 1 : i));
									}, 1500);
								}
							}}
						/>
					</Modal.Body>
				</Modal>
			) : null}
		</>
	);
};
