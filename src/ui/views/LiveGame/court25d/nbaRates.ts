import type { PlayZone } from "./plays.ts";

// THE LEAGUE, BY THE NUMBERS.
//
// How NBA possessions really go, so the court stages each one the way it
// most likely happened. The sim decides who shot, from where, and whether
// somebody passed it to him; this decides the rest - was it a pick-and-roll,
// a cut, a post-up, a catch on the perimeter - at the rate the league plays
// it, and how much happens on the way.
//
// Sources (league-wide, 2024-25 unless noted):
//   - Synergy play types, as published on NBA.com/stats, summed over all 30
//     teams: how possessions finish.
//   - NBA.com player tracking (Second Spectrum): passes, touches, drives.
//   - Second Spectrum ball screens per 100 possessions (2023-24, 68.8; 71.7
//     in 2021-22), as reported by theScore and FiveThirtyEight.
//   - PBP Stats shot zones; PBP Stats seconds per possession.
//   - Synergy zone defense rate ("consistently 2-4%"), Second Spectrum
//     switch and blitz/show/ICE rates on ball screens.
// No public source counts named sets (Horns, Spain, Chicago, Floppy...) league
// wide; their frequencies follow from these, through the playbook's weights.

export type PlayType =
	| "Transition"
	| "Isolation"
	| "PRBallHandler"
	| "PRRollMan"
	| "Postup"
	| "Spotup"
	| "Handoff"
	| "Cut"
	| "OffScreen"
	| "Putback"
	| "Misc";

// Share of possessions (%) each play type finishes, league-wide.
export const LEAGUE_PLAY_TYPES: Record<PlayType, number> = {
	Spotup: 23,
	Transition: 18.6,
	PRBallHandler: 16,
	Isolation: 7,
	Cut: 6.9,
	PRRollMan: 5.8,
	Putback: 5.3,
	Misc: 5.3,
	Handoff: 4.9,
	OffScreen: 3.7,
	Postup: 3.5,
};

// The same, for a guard (PG, SG), a wing (SF, GF) and a big (PF, C). Not
// published by position; drawn so that guards finishing ~42% of possessions,
// wings ~33% and bigs ~25% add back up to the league's shares above (within
// half a point each).
export type Role = "guard" | "wing" | "big";
export const PLAY_TYPE_SHARE: Record<Role, Record<PlayType, number>> = {
	guard: {
		PRBallHandler: 33,
		Spotup: 18,
		Transition: 18,
		Isolation: 9,
		Handoff: 6.5,
		OffScreen: 5,
		Misc: 5,
		Cut: 2.5,
		Putback: 1.5,
		Postup: 1,
		PRRollMan: 0.5,
	},
	wing: {
		Spotup: 32,
		Transition: 21,
		PRBallHandler: 8,
		Isolation: 8,
		Cut: 7,
		Misc: 6,
		Handoff: 5,
		OffScreen: 4,
		Putback: 4,
		Postup: 3,
		PRRollMan: 2,
	},
	big: {
		PRRollMan: 19,
		Spotup: 18.5,
		Transition: 16,
		Cut: 14.5,
		Putback: 14,
		Postup: 8,
		Misc: 4,
		Isolation: 2.5,
		Handoff: 2,
		OffScreen: 1,
		PRBallHandler: 0.5,
	},
};

// A position rank (0 a point guard to 8 a center) as a role.
export const roleOf = (rank: number): Role =>
	rank <= 2 ? "guard" : rank <= 5 ? "wing" : "big";

// For each play type: the share of its shots from each zone - at the rim, in
// the paint short of it (the sim's low post), mid-range, three - and in each
// zone the share that come straight off a pass. Drawn so that, weighted by
// the shares above, they add up to the league's shot chart (rim 27.7%, short
// 22.9%, mid-range 7.3%, three 42.1% of shots; ~70% of threes catch-and-
// shoot).
type Profile = {
	zone: Record<PlayZone, number>;
	assisted: Record<PlayZone, number>;
};

export const ZONES: Record<PlayType, Profile> = {
	Transition: {
		zone: { rim: 0.42, post: 0.12, mid: 0.05, three: 0.41 },
		assisted: { rim: 0.5, post: 0.35, mid: 0.4, three: 0.85 },
	},
	Isolation: {
		zone: { rim: 0.22, post: 0.3, mid: 0.16, three: 0.32 },
		assisted: { rim: 0.02, post: 0.02, mid: 0.02, three: 0.02 },
	},
	PRBallHandler: {
		zone: { rim: 0.2, post: 0.33, mid: 0.12, three: 0.35 },
		assisted: { rim: 0.03, post: 0.03, mid: 0.03, three: 0.03 },
	},
	PRRollMan: {
		zone: { rim: 0.55, post: 0.28, mid: 0.03, three: 0.14 },
		assisted: { rim: 0.9, post: 0.8, mid: 0.85, three: 0.95 },
	},
	Postup: {
		zone: { rim: 0.15, post: 0.65, mid: 0.15, three: 0.05 },
		assisted: { rim: 0.1, post: 0.08, mid: 0.08, three: 0.3 },
	},
	Spotup: {
		zone: { rim: 0.06, post: 0.11, mid: 0.06, three: 0.77 },
		assisted: { rim: 0.25, post: 0.25, mid: 0.7, three: 0.95 },
	},
	Handoff: {
		zone: { rim: 0.2, post: 0.2, mid: 0.1, three: 0.5 },
		assisted: { rim: 0.35, post: 0.35, mid: 0.6, three: 0.8 },
	},
	Cut: {
		zone: { rim: 0.8, post: 0.19, mid: 0.01, three: 0 },
		assisted: { rim: 0.95, post: 0.9, mid: 0.9, three: 0.9 },
	},
	OffScreen: {
		zone: { rim: 0.03, post: 0.12, mid: 0.2, three: 0.65 },
		assisted: { rim: 0.6, post: 0.6, mid: 0.85, three: 0.95 },
	},
	Putback: {
		zone: { rim: 0.62, post: 0.38, mid: 0, three: 0 },
		assisted: { rim: 0.02, post: 0.02, mid: 0, three: 0 },
	},
	Misc: {
		zone: { rim: 0.3, post: 0.2, mid: 0.2, three: 0.3 },
		assisted: { rim: 0.3, post: 0.3, mid: 0.3, three: 0.5 },
	},
};

// The sim's shot chart is not the league's - it takes two and a half times
// the mid-range shots and three-quarters of the threes - so the odds below,
// taken over its shots, would come out with too many of the play types that
// live where it shoots more (cuts, isolations) and too few spot-ups. These
// bring the mix over its shots back to the league's (measured over a season
// of its games), each shot still a way that shot is really taken.
export const FIT: Record<PlayType, number> = {
	Transition: 1.0,
	Isolation: 1.13,
	PRBallHandler: 1.81,
	PRRollMan: 0.61,
	Postup: 0.8,
	Spotup: 1.33,
	Handoff: 1.43,
	Cut: 0.49,
	OffScreen: 1.09,
	Putback: 1.0,
	Misc: 1.0,
};

// How likely each play type is for a shot by a man of this position rank,
// given where the shot came from and whether it came off a pass (`assisted`
// undefined: a miss, whose passer the sim does not say): share x P(zone, pass
// | type), unnormalized.
export const playTypeOdds = (
	type: string,
	zone: PlayZone,
	assisted: boolean | undefined,
	rank: number,
): number => {
	const t = type as PlayType;
	const share = PLAY_TYPE_SHARE[roleOf(rank)][t];
	const prof = ZONES[t];
	if (share === undefined || !prof) {
		return 0;
	}
	const pz = prof.zone[zone];
	const pa = prof.assisted[zone];
	return (
		FIT[t] * share * pz * (assisted === undefined ? 1 : assisted ? pa : 1 - pa)
	);
};

// The share of trips that get their shot up (or turn it over) inside six
// seconds - run out on the break - by what started them (2024-25 play-by-
// play): a defensive rebound, a steal, a made field goal at the other end.
export const BREAK_SHARE = {
	board: 0.33,
	steal: 0.63,
	make: 0.08,
};

// What happens on the way, per possession (NBA.com tracking; ball screens
// from Second Spectrum, 2023-24).
export const PER_POSSESSION = {
	passes: 2.84,
	touches: 4.1,
	drives: 0.47,
	ballScreens: 0.69,
};

// How ball screens are defended (Second Spectrum): switched ~26%, met
// aggressively - a blitz, a hard show, ICE - ~22%, and the rest mostly a big
// dropping back. And how much zone: ~3% of possessions.
export const COVERAGE = {
	switch: 0.26,
	aggressive: 0.22,
	drop: 0.52,
	zone: 0.03,
};
