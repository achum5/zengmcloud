// Match a leading "/l/{lid}" in a path, capturing the lid.
const LID_PREFIX = /^\/l\/(\d+)(?=\/|$)/;

// If `path` targets a league OTHER than the one currently loaded, retarget it to
// the current league.
//
// Content saved with an absolute /l/{lid}/... URL - game recaps ("X made a
// basket to force overtime…"), event/news links, feats - bakes in the lid from
// WHEN IT WAS WRITTEN. After an export + re-import the league gets a NEW lid, so
// those links point at a league that no longer exists ("League not found").
// While viewing league X, an in-app link to a DIFFERENT league Y is always one
// of these stale links (BBGM never cross-links between leagues from inside one),
// so rewrite Y → X. Legit league switching happens from the non-league
// dashboard, where there's no current lid, so this leaves it alone.
export const rewriteStaleLid = (
	path: string,
	currentPathname: string = typeof location !== "undefined"
		? location.pathname
		: "",
): string => {
	const target = LID_PREFIX.exec(path);
	if (!target) {
		return path;
	}
	const current = LID_PREFIX.exec(currentPathname);
	if (!current || current[1] === target[1]) {
		return path;
	}
	return path.replace(LID_PREFIX, `/l/${current[1]}`);
};
