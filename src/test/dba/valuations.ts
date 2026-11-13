// THE DBA REPLAY, part 3: what gets asked. A valuation answers, for one team
// in one deal at one moment of the owner's league, how much it gains - more
// than 0 is a yes. The replay sets the league to the moment (state.ts) and
// calls prepare once, then evaluate for each side.
//
// To put a new CPU to the test, write one of these and pass it to
// runDbaReplay (replay.ts), or add it to `valuations` below and name it in
// DBA_VALUATIONS.

import { g } from "../../worker/util/index.ts";
import type { Moment } from "./timeline.ts";

export type DealSide = {
	// The team asked, and the other team.
	tid: number;
	partnerTid: number;
	// What it gets and gives.
	pidsAdd: number[];
	pidsRemove: number[];
	dpidsAdd: number[];
	dpidsRemove: number[];
	// The other team is a human's.
	vsHuman: boolean;
	// A CPU trade's starter (vanilla's betweenAiTeams teams[0], which only
	// asks that it not be a disaster for it, |dv| <= 15).
	initiator: boolean;
};

export type Valuation = {
	name: string;
	// Once a moment, the league set to it.
	prepare?: (moment: Moment) => Promise<void>;
	// How much the team gains: > 0, it says yes.
	evaluate: (side: DealSide) => Promise<number>;
};

const ask = (calc: any, s: DealSide) =>
	calc.evaluate({
		tid: s.tid,
		pidsAdd: s.pidsAdd,
		pidsRemove: s.pidsRemove,
		dpidsAdd: s.dpidsAdd,
		dpidsRemove: s.dpidsRemove,
		tradingPartnerTid: s.partnerTid,
	}) as Promise<number>;

// What the owner's league plays on: stock trade values at its difficulty
// (difficultyTrade 3.5), or at another ("vanilla@2").
export const vanilla = (difficultyTrade?: number): Valuation => {
	let calc: any;
	return {
		name:
			difficultyTrade === undefined ? "vanilla" : `vanilla@${difficultyTrade}`,
		async prepare() {
			g.setWithoutSavingToDB("realisticFrontOffices", false);
			const { ValueChangeCalculator } =
				await import("../../worker/core/team/ValueChangeCalculator.ts");
			calc = new ValueChangeCalculator();
		},
		async evaluate(s) {
			if (difficultyTrade === undefined) {
				return ask(calc, s);
			}
			const saved = g.get("difficultyTrade");
			g.setWithoutSavingToDB("difficultyTrade", difficultyTrade);
			try {
				return await ask(calc, s);
			} finally {
				g.setWithoutSavingToDB("difficultyTrade", saved);
			}
		},
	};
};

// The realistic mode on master: realistic front offices on, trades on
// vanilla+ (core/vanillaPlus tradeValueCalculator) - lanes, worth season by
// season, the calendar, a human paying at least the best offer elsewhere.
export const realistic = (): Valuation => {
	let calc: any;
	return {
		name: "realistic",
		async prepare() {
			g.setWithoutSavingToDB("realisticFrontOffices", true);
			const { vanillaPlusSettings } =
				await import("../../worker/core/vanillaPlus/settings.ts");
			vanillaPlusSettings.on = true;
			const { tradeValueCalculator } =
				await import("../../worker/core/vanillaPlus/index.ts");
			calc = await tradeValueCalculator();
		},
		evaluate: (s) => ask(calc, s),
	};
};

// The smart CPU's GM (worker/core/smartCpu/worth.ts): its worth, and the
// market - not below the best real offer elsewhere ("smart-nomarket": its
// worth alone).
export const smart =
	(market = true): (() => Valuation) =>
	() => {
		let view: any;
		return {
			name: market ? "smart" : "smart-nomarket",
			async prepare() {
				g.setWithoutSavingToDB("realisticFrontOffices", true);
				const { buildOutlook } =
					await import("../../worker/core/realisticFrontOffice/outlook.ts");
				const { SmartView } =
					await import("../../worker/core/smartCpu/worth.ts");
				const env: Record<string, string | undefined> =
					(globalThis as any).process?.env ?? {};
				const outlook = await buildOutlook({
					iterations: Number(env.SMART_ITER ?? 500),
				});
				view = new SmartView(outlook, g.get("userTids"));
			},
			async evaluate(s) {
				const v = await view.verdict(
					s.tid,
					s.partnerTid,
					{ pids: s.pidsRemove, dpids: s.dpidsRemove },
					{ pids: s.pidsAdd, dpids: s.dpidsAdd },
					{ market },
				);
				return v.gain * 1000;
			},
		};
	};

export const valuations: Record<string, () => Valuation> = {
	vanilla: () => vanilla(),
	realistic,
	smart: smart(true),
	"smart-nomarket": smart(false),
};

// A valuation by name: one of `valuations`, or "vanilla@<difficultyTrade>".
export const valuationNamed = (name: string): Valuation => {
	const [base, arg] = name.split("@");
	if (base === "vanilla" && arg !== undefined) {
		return vanilla(Number(arg));
	}
	const make = valuations[name];
	if (!make) {
		throw new Error(`No valuation "${name}"`);
	}
	return make();
};
