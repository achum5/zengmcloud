import clsx from "clsx";
import { formatGameDate } from "../../common/calendarDates.ts";

// The night a game is (or was) played, on a calendar schedule. Nothing on
// classic numbered days.
export const GameDate = ({
	className,
	date,
}: {
	className?: string;
	date: string | undefined;
}) =>
	date === undefined ? null : (
		<div className={clsx("text-body-secondary small mb-1", className)}>
			{formatGameDate(date, "long")}
		</div>
	);
