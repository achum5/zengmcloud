// A PLAY, SAID THE WAY A BROADCAST'S PLAY LIST SAYS IT: one plain line, the
// man first - "Devin Booker misses three point jumper", "Wendell Carter Jr.
// makes layup (Franz Wagner assists)", "Franz Wagner personal foul". Only
// how the gamecast shows a play: the game's own lines (the classic page,
// the 3D court acting them out) are as they were. A line it has no way of
// saying is shown as the game wrote it (undefined here).

type Event = { type: string; [key: string]: any };

// What kind of shot it was, from the attempt and how the line says it
// finished (see liveGameWording's finishOf).
const shotKind = (attempt: Event | undefined, finish: string | undefined) => {
	const dunk = finish === "dunk" || finish === "poster";
	switch (attempt?.type) {
		case "fgaTp":
			return attempt.desperation
				? "deep three at the buzzer"
				: "three point jumper";
		case "fgaTpFake":
			return attempt.desperation ? "deep shot at the buzzer" : "deep jumper";
		case "fgaMidRange":
			return "jump shot";
		case "fgaLowPost":
			return "post shot";
		case "fgaAtRim":
			return dunk ? "dunk" : "layup";
		case "fgaPutBack":
			return dunk ? "putback dunk" : "putback layup";
		case "fgaTipIn":
			return dunk ? "alley-oop dunk" : "tip-in";
		default:
			return "shot";
	}
};

export const playSentence = ({
	events,
	finish,
	name,
	teamName,
}: {
	// The lines the play was put together from, oldest first (a shot: its
	// attempt and its result).
	events: (Event | undefined)[];
	finish?: string;
	name: (pid: number | undefined) => string | undefined;
	// The team's name, for a timeout (box score order: 0 home, 1 away).
	teamName: (t: number) => string | undefined;
}): string | undefined => {
	const result = events.at(-1);
	if (!result) {
		return undefined;
	}
	const attempt = events.length > 1 ? events[0] : undefined;
	const who = name(result.pid);
	const type = result.type;
	const kind = shotKind(attempt, finish);

	// A shot, made or missed, blocked or fouled.
	if (attempt && /^fga/.test(attempt.type)) {
		const shooter = name(attempt.pid);
		if (!shooter) {
			return undefined;
		}
		if (/^(fg|tp)/.test(type)) {
			const ast = name(result.pidAst);
			const onWho = finish === "poster" ? name(result.pidDefense) : undefined;
			const made = onWho
				? `${shooter} dunks on ${onWho}`
				: `${shooter} makes ${kind}`;
			const and1 = type.endsWith("AndOne") ? ", and the foul" : "";
			return `${made}${and1}${ast ? ` (${ast} assists)` : ""}`;
		}
		if (type.startsWith("miss")) {
			return `${shooter} misses ${kind}`;
		}
		if (type.startsWith("blk")) {
			return who ? `${who} blocks ${shooter}'s ${kind}` : undefined;
		}
		if (type === "pfFG" || type === "pfTP") {
			return who
				? `${who} shooting foul (${shooter} to the line for ${type === "pfTP" ? 3 : 2})`
				: undefined;
		}
	}
	if (/^fga/.test(type)) {
		// (The attempt alone, still in the air.)
		return who ? `${who} goes up for a ${kind}` : undefined;
	}
	switch (type) {
		case "ft":
			return who ? `${who} makes free throw` : undefined;
		case "missFt":
			return who ? `${who} misses free throw` : undefined;
		case "orb":
			return who ? `${who} offensive rebound` : undefined;
		case "drb":
			return who ? `${who} defensive rebound` : undefined;
		case "tov":
			return who
				? `${who} ${result.outOfBounds ? "out of bounds" : "lost ball"} turnover`
				: undefined;
		case "stl": {
			const lost = name(result.pidTov);
			return who && lost
				? `${lost} ${result.outOfBounds ? "knocked out of bounds" : "lost ball"} turnover (${who} steals)`
				: undefined;
		}
		case "pfNonShooting":
			return who ? `${who} personal foul` : undefined;
		case "pfBonus": {
			const to = name(result.pidShooting);
			return who
				? `${who} personal foul${to ? ` (${to} to the line for 2)` : ""}`
				: undefined;
		}
		case "foulOut":
			return who ? `${who} fouls out` : undefined;
		case "injury":
			return who ? `${who} is hurt` : undefined;
		case "jumpBall": {
			const other = name(result.pid2);
			return who && other ? `${who} wins the tip over ${other}` : undefined;
		}
		case "sub": {
			const on = (result.pids ?? []).map(name).filter(Boolean);
			const off = (result.pidsOff ?? []).map(name).filter(Boolean);
			if (on.length === 0) {
				return undefined;
			}
			return `${on.join(", ")} ${on.length === 1 ? "enters" : "enter"}${off.length > 0 ? ` for ${off.join(", ")}` : ""}`;
		}
		case "timeout": {
			const team =
				typeof result.t === "number" ? teamName(result.t) : undefined;
			if (!team) {
				return undefined;
			}
			return `${team} timeout${typeof result.numLeft === "number" ? ` (${result.numLeft} left)` : ""}`;
		}
		default:
			return undefined;
	}
};
