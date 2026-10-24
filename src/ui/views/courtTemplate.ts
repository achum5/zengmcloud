// THE TEMPLATE A WHOLE-FLOOR PICTURE IS PAINTED ON (see CourtStyle's
// floorImageURL): the floor seen from above, edge to edge as the court lays
// it down - the 94 x 50 ft court, five feet of apron past each baseline and
// two and a half past each sideline - at 20 px a foot, its lines where the
// game's are.

const FT = 20;
const END = 5;
const SIDE = 2.5;
const COURT_W = 94;
const COURT_H = 50;
const RIM_INSET = 5.25;
const THREE_R = 23.75;
const CORNER_LEN = RIM_INSET + Math.sqrt(THREE_R ** 2 - 22 ** 2);

export const FLOOR_TEMPLATE_W = (COURT_W + 2 * END) * FT;
export const FLOOR_TEMPLATE_H = (COURT_H + 2 * SIDE) * FT;

export const floorTemplate = (): HTMLCanvasElement => {
	const cv = document.createElement("canvas");
	cv.width = FLOOR_TEMPLATE_W;
	cv.height = FLOOR_TEMPLATE_H;
	const g = cv.getContext("2d")!;
	g.fillStyle = "#3a4a5a";
	g.fillRect(0, 0, cv.width, cv.height);
	// Court feet to pixels.
	g.translate(END * FT, SIDE * FT);
	g.scale(FT, FT);
	g.fillStyle = "#e2c497";
	g.fillRect(0, 0, COURT_W, COURT_H);
	g.strokeStyle = "#ffffff";
	g.lineWidth = 0.25;
	g.strokeRect(0, 0, COURT_W, COURT_H);
	g.beginPath();
	g.moveTo(COURT_W / 2, 0);
	g.lineTo(COURT_W / 2, COURT_H);
	g.stroke();
	for (const r of [6, 2]) {
		g.beginPath();
		g.arc(COURT_W / 2, 25, r, 0, Math.PI * 2);
		g.stroke();
	}
	for (const mirror of [false, true]) {
		g.save();
		if (mirror) {
			g.translate(COURT_W, 0);
			g.scale(-1, 1);
		}
		g.strokeRect(0, 17, 19, 16);
		g.beginPath();
		g.arc(19, 25, 6, 0, Math.PI * 2);
		g.stroke();
		g.beginPath();
		g.moveTo(4, 22);
		g.lineTo(4, 28);
		g.stroke();
		g.beginPath();
		g.arc(RIM_INSET, 25, 0.95, 0, Math.PI * 2);
		g.stroke();
		g.beginPath();
		g.arc(RIM_INSET, 25, 4, -Math.PI / 2, Math.PI / 2);
		g.stroke();
		g.beginPath();
		g.moveTo(0, 3);
		g.lineTo(CORNER_LEN, 3);
		g.moveTo(0, 47);
		g.lineTo(CORNER_LEN, 47);
		const a = Math.asin(22 / THREE_R);
		g.moveTo(CORNER_LEN, 3);
		g.arc(RIM_INSET, 25, THREE_R, -a, a);
		g.stroke();
		g.restore();
	}
	return cv;
};
