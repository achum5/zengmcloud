// Every AI prompt in the game hands over names that collide with real people -
// that is the whole point of a real-players league - and without this the AI
// writes about the real person instead. It reaches for a college, a hometown, a
// signature move, a rivalry, a championship, none of which happened here, and
// the result is confidently wrong in a way that is worse than saying nothing.
//
// Career-length writeups are the most exposed, because a career is exactly the
// shape of thing the AI already "knows" about a famous name. So this is shared
// verbatim by every prompt rather than reworded per page.
export const FICTIONAL_LEAGUE_NOTICE = `THIS IS A FICTIONAL LEAGUE — USE ONLY THE DATA BELOW, NEVER REAL-WORLD KNOWLEDGE. Player and team names may coincide with real people and franchises, but they are NOT them and share no history. A player has no real-world team, hometown, college, draft position, championships, awards, signature moves, nicknames, rivalries, relationships, or reputation — only what the data below states. Do NOT reference or imply anything about a player or team from outside this data: e.g., do not associate Paul Pierce with the Celtics, assume a player's playing style or position, invoke a real-world rivalry, or call anyone the "real-life" anything. Every team a player has played for, every number, and every storyline must come solely from the data provided. Write as if these people and teams exist only within this league and nowhere else.`;
