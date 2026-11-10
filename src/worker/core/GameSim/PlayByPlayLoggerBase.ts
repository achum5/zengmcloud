// Input rather than Output for baseball because that's the only sport where input and output objects are the same, so output doesn't exist
import type { PlayByPlayEventInput as BaseballEvent } from "../GameSim.baseball/PlayByPlayLogger.ts";
import type { PlayByPlayEventOutput as FootballEvent } from "../GameSim.football/PlayByPlayLogger.ts";
import type { PlayByPlayEventOutput as BasketballEvent } from "../GameSim.basketball/PlayByPlayLogger.ts";
import type { PlayByPlayEventOutput as HockeyEvent } from "../GameSim.hockey/PlayByPlayLogger.ts";
import type { TeamNum } from "../../../common/types.ts";

type PlayByPlayEventStat = {
	type: "stat";
	t: TeamNum;
	pid: number | undefined | null;
	s: string;
	amt: number;
};

type PlayByPlayEventInit = {
	type: "init";

	// The sim's two teams, for sports whose live game reads them (the football 3D
	// field takes its depth charts from here)
	boxScore?: unknown;
};

type SportEvent = FootballEvent | BaseballEvent | BasketballEvent | HockeyEvent;

export type PlayByPlayEvent<T extends SportEvent> =
	| T
	| PlayByPlayEventStat
	| PlayByPlayEventInit;

export abstract class PlayByPlayLoggerBase<T extends SportEvent> {
	active: boolean = false;
	playByPlay: PlayByPlayEvent<T>[] = [];
	constructor(active: boolean) {
		this.active = active;
	}
	abstract logEvent(event: unknown): void;

	logStat(t: TeamNum, pid: number | undefined | null, s: string, amt: number) {
		if (!this.active) {
			return;
		}

		this.playByPlay.push({
			type: "stat",
			t,
			pid,
			s,
			amt,
		});
	}

	getPlayByPlay(boxScore?: unknown): PlayByPlayEvent<T>[] | undefined {
		if (!this.active) {
			return;
		}

		const init: PlayByPlayEventInit = {
			type: "init",
		};
		if (boxScore !== undefined) {
			init.boxScore = boxScore;
		}

		return [init, ...this.playByPlay];
	}
}
