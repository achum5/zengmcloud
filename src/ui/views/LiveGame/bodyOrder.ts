// The order the bodies on the floor (or the grass) are handed to React - and it
// is deliberately NOT the order the scene lists its actors in.
//
// Every body is keyed by pid, so a scene change never remounts one: that is what
// lets a man keep his element and GLIDE from where he was to where he now is.
// But a keyed list whose ORDER changes still makes React re-INSERT the children
// that moved, and re-inserting an element cancels the CSS transform transition it
// was about to start. That body SNAPS to its new spot while everyone React left
// in place runs there.
//
// It showed worst at the opening tip. The jump scene lists the two jumpers and
// then a ring of eight in roster order; the possession that follows lists both
// fives in court-position order. The two orders have nothing to do with each
// other - so on the single beat where all ten cross the floor, React re-inserted
// six or seven of them and most of the court teleported while three men ran.
// (Sampled through a simmed game, the set of players who snapped was EXACTLY the
// set React re-inserted, on every transition - and never anyone else.)
//
// Sorting by pid gives an order no scene can change, so nothing is ever
// re-inserted and every body keeps its transition. Painting is unaffected: each
// body sets its own zIndex.
export const bodyRenderOrder = <T extends { pid: number }>(
	actors: readonly T[],
): T[] => [...actors].sort((a, b) => a.pid - b.pid);
