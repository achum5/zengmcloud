import type { CardSet, CardVariant } from "../../../common/tradingCards.ts";
import { cardErasById } from "../../../common/tradingCards.ts";

// What the chosen card actually looks like. This is the same text that goes
// into the image prompt, so the panel doubles as a preview of what is about to
// be asked for - the difference between picking blind from a list of a hundred
// names and knowing you chose the one with the comic-book backgrounds.
export const SetDesign = ({
	set,
	variant,
}: {
	set: CardSet;
	variant: CardVariant | undefined;
}) => {
	const era = cardErasById.get(set.era);

	const rows: [string, string | undefined][] = [
		["Stock", set.stock],
		["Shape", set.proportions],
		["Border", set.border],
		["Photo", set.photography],
		["Background", set.background],
		["Type", set.typography],
		["Layout", set.layout],
		["Era", set.markers],
		["Age", era?.wear],
	];

	return (
		<div className="border rounded p-2 small">
			{variant && variant.treatment !== "" ? (
				<p className="mb-2">
					<span className="text-body-secondary">{variant.label}:</span>{" "}
					{variant.treatment}
				</p>
			) : null}
			<div style={{ maxHeight: 220, overflowY: "auto" }}>
				{rows
					.filter((row): row is [string, string] => row[1] !== undefined)
					.map(([label, value]) => (
						<div key={label} className="mb-1">
							<span className="text-body-secondary">{label}:</span> {value}
						</div>
					))}
			</div>
		</div>
	);
};
