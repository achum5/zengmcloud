import type { ConfIdentity } from "../../common/confs.ts";

// A CONFERENCE, THE WAY A TEAM IS SHOWN: logo then name. One component, so
// the roster header, the dashboard, the standings and the bracket all draw a
// conference the same way and a logo set once shows up in all of them.
//
// A conference with no logo is just its name. There is no placeholder - a
// grey circle next to "Eastern Conference" would say "something is missing"
// on every league that never set one, which is most of them.
export const ConfLogoAndName = ({
	conf,
	size = 20,
	// Show the abbreviation instead of the full name, for tight spots.
	short = false,
	className,
}: {
	conf: ConfIdentity | undefined;
	size?: number;
	short?: boolean;
	className?: string;
}) => {
	if (!conf) {
		return null;
	}
	const label = short && conf.abbrev ? conf.abbrev : conf.name;
	return (
		<span
			className={`d-inline-flex align-items-center ${className ?? ""}`}
			style={{ gap: Math.max(4, Math.round(size * 0.3)) }}
		>
			{conf.imgURL ? (
				<img
					alt=""
					src={conf.imgURL}
					style={{
						width: size,
						height: size,
						objectFit: "contain",
						flexShrink: 0,
						verticalAlign: "middle",
					}}
					// A URL that fails to load shows nothing, not a broken frame
					// next to the conference name on every page.
					onError={(event) => {
						event.currentTarget.style.display = "none";
					}}
				/>
			) : null}
			<span>{label}</span>
		</span>
	);
};
