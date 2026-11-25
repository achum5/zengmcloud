import { gamesOutCell } from "../../common/gamesOut.ts";
import type { GamesOut } from "../../common/types.ts";

// An injury's games on the NBA calendar as a table cell: "5", "12+", "~19".
// "12+" sorts just past 12.
export const gamesOutCellData = (gamesOut: GamesOut | undefined) =>
	gamesOut
		? {
				value: gamesOutCell(gamesOut),
				sortValue: gamesOut.n + (gamesOut.more ? 0.5 : 0),
			}
		: null;
