// WHAT A PLAYER IS WORTH IN A REDRAFT (basketball): the win shares he has
// earned, plus the win shares he can still be expected to earn, with his best
// five seasons counted a second time - so a star's prime outranks a long run
// of bench minutes, and a 21-year-old who hasn't played much yet isn't judged
// on his first two seasons alone.
//
// The future is BBGM's own:
// - his rating as the Draft History table shows it, rising and then falling
//   the way players' ratings do on average at each age (from running
//   developSeason for thousands of players);
// - each season priced at what a player of that rating averages in win shares
//   over a simulated BBGM season (fit to whole leagues, bench players and
//   partial seasons included);
// - and only as long as shouldRetire would be expected to keep him playing
//   (its formulas, for the chance he hangs it up after each season).

// The average change in a player's rating from one age to the next, for
// players good enough to be in a rotation. (Not his potential: that's the
// best he's likely to reach, which is more than he can be expected to be at
// any one age.)
const change = (age: number) => {
	if (age <= 21) {
		return 3.3;
	}
	if (age <= 23) {
		return 2.1;
	}
	if (age <= 25) {
		return 0.6;
	}
	if (age <= 26) {
		return -0.3;
	}
	if (age <= 27) {
		return -0.5;
	}
	if (age <= 29) {
		return -1.3;
	}
	if (age <= 30) {
		return -1.7;
	}
	if (age <= 31) {
		return -2;
	}
	if (age <= 34) {
		return -2.6;
	}
	if (age <= 35) {
		return -3.3;
	}
	return -3.8;
};

export type RedraftContext = {
	season: number;
	// How much of this regular season is still to be played, 0 to 1.
	leftThisSeason: number;
	numGames: number;
	// The league's ratings spread, as player.value reads it.
	ovrMean: number;
	ovrStd: number;
};

export type RedraftPlayer = {
	bornYear: number;
	// Regular season win shares by season.
	ws: Map<number, number>;
	// Still playing, with ratings the user can see: his rating now and the
	// best of his career. Left out for anyone retired.
	current?: {
		ovr: number;
		peakOvr: number;
	};
};

const BEST_SEASONS = 5;
const MAX_AGE = 45;

export const redraftValue = (p: RedraftPlayer, ctx: RedraftContext) => {
	// On the default 47/10 scale the fit below was made on.
	const norm = (ovr: number) =>
		ctx.ovrStd > 0 ? ((ovr - ctx.ovrMean) / ctx.ovrStd) * 10 + 47 : ovr;
	const wsAt = (ovr: number) =>
		(0.088 * Math.max(0, norm(ovr) - 44) ** 1.5 * ctx.numGames) / 82;

	const seasons = new Map(p.ws);
	if (p.current) {
		const now =
			(seasons.get(ctx.season) ?? 0) + wsAt(p.current.ovr) * ctx.leftThisSeason;
		seasons.set(ctx.season, now);
	}
	const values = [...seasons.values()];

	if (p.current) {
		const age0 = ctx.season - p.bornYear;
		let ovr = p.current.ovr;
		let prevWS = seasons.get(ctx.season) ?? 0;
		let peakOvr = Math.max(p.current.peakOvr, ovr);
		let playing = 1;
		for (let age = age0 + 1; age <= MAX_AGE; age++) {
			const prevAge = age - 1;
			// (shouldRetire, after the season he just finished.)
			if (prevAge > 33) {
				const estWS = (Math.max(1.1 * norm(ovr), 37) - 37) ** 2 / 85.017;
				const logit =
					-1.4134 +
					(prevAge - 18) ** 2 / 175.19 -
					0.971 * Math.max(prevWS, estWS);
				let retire = 1 / (1 + Math.exp(-logit));
				const fraction = ovr / peakOvr;
				if (fraction < 0.7) {
					retire += (1 - retire) * Math.min(1, 0.2 + (0.7 - fraction));
				}
				playing *= 1 - retire;
			}

			ovr += change(prevAge);
			peakOvr = Math.max(peakOvr, ovr);

			prevWS = wsAt(ovr);
			const expected = playing * prevWS;
			values.push(expected);
			if (age > 26 && expected < 0.05) {
				break;
			}
		}
	}

	const total = values.reduce((sum, value) => sum + value, 0);
	const best = [...values]
		.sort((a, b) => b - a)
		.slice(0, BEST_SEASONS)
		.reduce((sum, value) => sum + value, 0);
	return total + best;
};
