import { bySport } from "../../common/sportFunctions.ts";
import BoxScoreBaseball from "./BoxScore.baseball.tsx";
import BoxScoreBasketball from "./BoxScore.basketball.tsx";
import BoxScoreFootball from "./BoxScore.football.tsx";
import BoxScoreHockey from "./BoxScore.hockey.tsx";
import type { BoxPeriods } from "./PeriodPicker.tsx";

export const BoxScore = (props: {
	boxScore: unknown;
	Row: unknown;
	forceRowUpdate: boolean;
	sportState?: unknown;
	// Each team's box score by the quarter too (basketball).
	periods?: BoxPeriods;
}) => {
	return bySport({
		baseball: BoxScoreBaseball(props as any),
		basketball: BoxScoreBasketball(props),
		football: BoxScoreFootball(props as any),
		hockey: BoxScoreHockey(props as any),
	});
};
