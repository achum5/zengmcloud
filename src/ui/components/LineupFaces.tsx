import type { LineupPic } from "../../worker/util/lineupPictures.ts";
import { PlayerPicture } from "./PlayerPicture.tsx";
import { helpers } from "../util/helpers.ts";

// A FIVE, PICTURED: each man's head and shoulders in his uniform, side by
// side, his name under him, each a link to him.

const W = 58;
// The face drawn this much wider than its frame, so he fills it.
const ZOOM = 1.06;

// One of them. `label` in place of his name, and `dim` to grey him out.
export const LineupFace = ({
	pid,
	pic,
	colors,
	jersey,
	scale = 1,
	label,
	dim,
}: {
	pid: number;
	pic: LineupPic | undefined;
	colors?: [string, string, string];
	jersey?: string;
	scale?: number;
	label?: string;
	dim?: boolean;
}) => {
	const w = Math.round(W * scale);
	const h = Math.round(w * 1.42);
	const inner = w * ZOOM;
	return (
		<a
			href={helpers.leagueUrl(["player", pid])}
			className="d-block flex-shrink-0 text-decoration-none text-body"
			style={{ width: w }}
			title={pic?.name}
		>
			<div
				style={{
					height: h,
					overflow: "hidden",
					...(dim ? { filter: "grayscale(1)", opacity: 0.45 } : {}),
				}}
			>
				<div
					style={{
						width: inner,
						height: inner * 1.5,
						marginLeft: (w - inner) / 2,
						marginTop: -inner * 0.02,
					}}
				>
					{pic ? (
						<PlayerPicture
							face={pic.face}
							imgURL={pic.imgURL}
							colors={colors}
							jersey={jersey}
							lazy
						/>
					) : null}
				</div>
			</div>
			<div
				className="text-truncate text-center"
				style={{
					fontSize: Math.max(10, Math.round(11 * scale)),
					fontWeight: 600,
					lineHeight: 1.2,
					marginTop: 3,
				}}
			>
				{label ?? pic?.name ?? "?"}
			</div>
		</a>
	);
};

export const LineupFaces = ({
	pids,
	pics,
	colors,
	jersey,
	scale = 1,
}: {
	pids: number[];
	pics: Record<number, LineupPic> | undefined;
	colors?: [string, string, string];
	jersey?: string;
	scale?: number;
}) => (
	<div className="d-flex" style={{ gap: Math.round(4 * scale) }}>
		{pids.map((pid) => (
			<LineupFace
				key={pid}
				pid={pid}
				pic={pics?.[pid]}
				colors={colors}
				jersey={jersey}
				scale={scale}
			/>
		))}
	</div>
);
