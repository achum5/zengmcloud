import clsx from "clsx";
import type { NationalTag as Tag } from "../../common/types.ts";

// A NATIONAL GAME'S NETWORK on its preview (see NationalBroadcast): its logo
// in its own colors - its dark page one on a dark page, where it has one - or
// its name.
export const NationalTag = ({
	className,
	tag,
}: {
	className?: string;
	tag: Tag;
}) => (
	<span className={clsx("national-tag", className)} title={tag.network}>
		{tag.logo ? (
			<>
				<img
					className={tag.logoDark ? "national-logo-light" : undefined}
					src={tag.logo}
					alt={tag.network}
				/>
				{tag.logoDark ? (
					<img
						className="national-logo-dark"
						src={tag.logoDark}
						alt={tag.network}
					/>
				) : null}
			</>
		) : (
			tag.network
		)}
	</span>
);
