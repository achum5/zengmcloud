import { tierColor } from "../util/triviaTiers.ts";

// A finished grid at a glance: nine squares, colored by how rare each answer
// was, blank where nothing was found. It says how someone did without naming a
// single player, which is what makes it safe to show next to a board you are
// about to play yourself.
export const TriviaSquares = ({
	cells,
	size = 10,
}: {
	cells: (number | null)[];
	size?: number;
}) => (
	<div
		className="trivia-squares flex-shrink-0"
		style={{ gridTemplateColumns: `repeat(3, ${size}px)` }}
		aria-hidden="true"
	>
		{Array.from({ length: 9 }, (_, i) => (
			<span
				key={i}
				style={{
					width: size,
					height: size,
					background: tierColor(cells[i]),
				}}
			/>
		))}
	</div>
);
