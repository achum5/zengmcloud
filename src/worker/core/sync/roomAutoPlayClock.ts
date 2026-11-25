// When the room's auto play is next due to fire, as published by whichever
// device is running it - kept by connect.ts from the room's auto-play doc.
// Undefined when nobody is auto-playing.
//
// Its own module so the ready-up evaluator (draftReady.ts) can read it without
// importing connect.ts, which imports the evaluator.
let nextRunAt: number | undefined;

export const setRoomAutoPlayNextRunAt = (at: number | undefined) => {
	nextRunAt = at;
};

export const getRoomAutoPlayNextRunAt = () => nextRunAt;
