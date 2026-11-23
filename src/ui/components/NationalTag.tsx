import clsx from "clsx";
import type { NationalTag as Tag } from "../../common/types.ts";

// A NATIONAL GAME'S NETWORK on its preview (see NationalBroadcast): its logo,
// or its name, on a dark chip so a white logo shows on either theme.
export const NationalTag = ({
	className,
	tag,
}: {
	className?: string;
	tag: Tag;
}) => (
	<span className={clsx("national-tag", className)} title={tag.network}>
		{tag.logo ? <img src={tag.logo} alt={tag.network} /> : tag.network}
	</span>
);
