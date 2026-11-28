import { drawGlints, FLOOR_EDGE } from "../arena.ts";
import { type Camera, depthOf, project } from "../camera.ts";
import { bowlMatrix } from "../bowlData.ts";
import { paintBowlOn } from "../bowlGl.ts";
import { drawHeadAt, headKey, headOf } from "../figure.ts";
import { bodyPoint, type PlayerState } from "../evaluate.ts";
import type { Body, V3 } from "../poses.ts";
import { atTable } from "../crew.ts";
import {
	benchStates,
	boardAt,
	bowlLookOf,
	dressFloor,
	type Frame,
	hoopsAndBalls,
	overlays,
	warmupLook,
} from "../scene.ts";
import { FACE_LIFT } from "../sculpt.ts";
import { glForced } from "../sculptGl.ts";
import { CAST_ALPHA, CASTS, OUTLINE, SOLID } from "../sprite.ts";
import {
	floorDraw,
	type FloorGl,
	floorOnto,
	floorPainter,
	floorPools,
	itemsDraw,
	itemsPut,
	linesDraw,
	type PageSheet,
	type Put,
	sheetBegin,
	sheetCopy,
	sheetOnto,
	sheetPaint,
	sheetPut,
	sheetSend,
	sheetWay,
} from "./floor.ts";
import { attrib, buffer, compile, type Prog, rgb } from "./gl.ts";
import {
	drawAllHands,
	drawMan,
	type ManReady,
	menCamera,
	menOnto,
	type MenGl,
	MEN_ROWS,
	menRows,
	menUnknown,
	readyMan,
} from "./men.ts";

// THE GAME DRAWN ON THE GRAPHICS CARD, ALL OF IT IN ONE PICTURE (Global
// Settings, "3D Drawing": a test of it): the bowl (see bowlGl.ts), the floor
// and the court, the men on it - each one mesh bent and painted on the card
// (see men.ts) - his head as today's court draws it (see figure.ts), the
// ball. What stands in front of what, the card works out. None of today's
// pictures of the men (see sprite.ts), nor the floor cut into pieces (see
// planes.ts).

const FLAT_VERT = `#version 300 es
in vec3 aPos;
in vec2 aUv;
uniform mat4 uM;
out vec2 vUv;
void main() {
	gl_Position = uM * vec4(aPos, 1.0);
	vUv = aUv;
}`;
const FLAT_FRAG = `#version 300 es
precision highp float;
in vec2 vUv;
uniform sampler2D uPic;
uniform vec4 uColor;
uniform float uPicOn;
out vec4 o;
void main() {
	o = uPicOn > 0.5 ? texture(uPic, vUv) : uColor;
}`;

// A head: today's, drawn on a sheet of them (see heads), stood up where his
// head is - its front as near the camera as the front of his head, round
// to its edge (so his own arm up in front of his face is in front of it).
const HEAD_VERT = `#version 300 es
in vec2 aCorner;
// Where it is on the picture (px), and on the sheet (0 - 1); the picture's
// size; his head's middle on the picture and its radius (px); how near the
// camera its middle and its front are.
uniform vec4 uBox;
uniform vec4 uUv;
uniform vec2 uSize;
uniform vec2 uZ;
out vec2 vUv;
out vec2 vPx;
void main() {
	vPx = mix(uBox.xy, uBox.zw, aCorner);
	vUv = mix(uUv.xy, uUv.zw, aCorner);
	gl_Position = vec4(
		vPx.x / uSize.x * 2.0 - 1.0,
		1.0 - vPx.y / uSize.y * 2.0,
		uZ.x,
		1.0
	);
}`;
const HEAD_FRAG = `#version 300 es
precision highp float;
in vec2 vUv;
in vec2 vPx;
uniform sampler2D uPic;
uniform vec3 uC;
uniform vec2 uZ;
out vec4 o;
const float SOLID = ${SOLID / 255};
const vec3 RIM = vec3(${OUTLINE.map((c) => (c / 255).toFixed(4)).join(", ")});
void main() {
	o = texture(uPic, vUv);
	vec2 d = (vPx - uC.xy) / uC.z;
	if (o.a < SOLID) {
		// The dark rim round him (see sprite.ts rim): a pixel wide, under the
		// edge's own soft pixels - as far back as the back of his head, so his
		// own neck in front of it is not rimmed.
		vec2 t = 1.0 / vec2(textureSize(uPic, 0));
		float n = max(
			max(texture(uPic, vUv + vec2(t.x, 0.0)).a, texture(uPic, vUv - vec2(t.x, 0.0)).a),
			max(texture(uPic, vUv + vec2(0.0, t.y)).a, texture(uPic, vUv - vec2(0.0, t.y)).a)
		);
		if (n >= SOLID) {
			o = vec4(o.rgb + RIM * (1.0 - o.a), 1.0);
			gl_FragDepth = (2.0 * uZ.x - uZ.y) * 0.5 + 0.5;
			return;
		}
	}
	if (o.a < 0.004) {
		discard;
	}
	float z = mix(uZ.x, uZ.y, sqrt(max(0.0, 1.0 - dot(d, d))));
	gl_FragDepth = z * 0.5 + 0.5;
}`;

// Which of his faces are left out: his back ones (the mesh's faces go
// round clockwise seen from outside him, on the card's picture), his front
// ones for the dark edge round him, and his reflection's - upside down, so
// the other way round.
const BODY_CULL = WebGL2RenderingContext.BACK;
const EDGE_CULL = WebGL2RenderingContext.FRONT;
const MIRRORED_CULL = WebGL2RenderingContext.FRONT;
// The floor is polished: everybody on it shows in it this faintly (see
// scene.ts SHINE).
const SHINE = 0.24;
// (As many men as the card has rows for - see men.ts menRows.)
const MEN_MAX = MEN_ROWS;

type Scene = {
	gl: WebGL2RenderingContext;
	flat: Prog;
	flatVao: WebGLVertexArrayObject;
	flatBuf: WebGLBuffer;
	court: WebGLTexture;
	courtOf?: HTMLCanvasElement;
	men?: MenGl;
	// What is laid round the court (see floor.ts) - null if the card can't.
	floor?: FloorGl | null;
	head: Prog;
	headVao: WebGLVertexArrayObject;
	// What the page draws for the picture each frame (see floor.ts
	// PageSheet), and each man's head on the card (see headsPut), by pid.
	sheet: PageSheet;
	headPics: Map<number, HeadPic>;
	// Frames drawn.
	frame: number;
	// The shadows the men cast, drawn small (see softOf).
	soft?: Soft;
	ready: ManReady[];
	// The time the last frame spent on each part of it (ms - for the frame
	// rate meter: see meter.ts).
	ms: Record<string, number>;
	// How long the card took to finish a frame after it was all handed over
	// (ms), now and then - for the meter: how much of a frame goes on the
	// card itself, which the page's own time says nothing about.
	cardMs?: number;
	cardAt: number;
	// (This frame's wait for it, if it was waited for: not the page's own
	// time.)
	waited: number;
	lost: boolean;
};
// (Every so often, ms: a frame waited for - see cardMs.)
const CARD_EVERY = 2000;
export type GpuScene = Scene;

// The card, on the canvas the game is shown on - undefined if there is
// none to be had (today's drawing, then).
export const gpuScene = (canvas: HTMLCanvasElement): GpuScene | undefined => {
	const gl = canvas.getContext("webgl2", {
		alpha: false,
		antialias: true,
		depth: true,
		stencil: true,
		premultipliedAlpha: false,
		preserveDrawingBuffer: false,
		failIfMajorPerformanceCaveat: !glForced(),
	});
	if (!gl) {
		return undefined;
	}
	const flat = compile(gl, FLAT_VERT, FLAT_FRAG);
	const head = compile(gl, HEAD_VERT, HEAD_FRAG);
	if (!flat || !head) {
		return undefined;
	}
	const flatVao = gl.createVertexArray();
	gl.bindVertexArray(flatVao);
	const flatBuf = gl.createBuffer();
	gl.bindBuffer(gl.ARRAY_BUFFER, flatBuf);
	gl.bufferData(gl.ARRAY_BUFFER, 4 * 5 * 4, gl.DYNAMIC_DRAW);
	attrib(gl, flat, "aPos", flatBuf, 3, 20, 0);
	attrib(gl, flat, "aUv", flatBuf, 2, 20, 12);
	buffer(gl, new Uint16Array([0, 1, 2, 0, 2, 3]), gl.ELEMENT_ARRAY_BUFFER);
	const headVao = gl.createVertexArray();
	gl.bindVertexArray(headVao);
	attrib(
		gl,
		head,
		"aCorner",
		buffer(gl, new Float32Array([0, 0, 1, 0, 1, 1, 0, 1])),
		2,
		8,
		0,
	);
	buffer(gl, new Uint16Array([0, 1, 2, 0, 2, 3]), gl.ELEMENT_ARRAY_BUFFER);
	gl.bindVertexArray(null);
	const court = gl.createTexture();
	const sc: Scene = {
		gl,
		flat,
		flatVao,
		flatBuf,
		court,
		head,
		headVao,
		sheet: sheetOnto(gl),
		headPics: new Map(),
		frame: 0,
		ready: [],
		ms: {},
		cardAt: -Infinity,
		waited: 0,
		lost: false,
	};
	canvas.addEventListener("webglcontextlost", () => {
		sc.lost = true;
	});
	return sc;
};

const flatQuad = (
	sc: Scene,
	x0: number,
	y0: number,
	x1: number,
	y1: number,
	color?: [number, number, number],
	z = 0,
) => {
	const { gl, flat } = sc;
	const d = new Float32Array([
		x0,
		y0,
		z,
		0,
		0,
		x1,
		y0,
		z,
		1,
		0,
		x1,
		y1,
		z,
		1,
		1,
		x0,
		y1,
		z,
		0,
		1,
	]);
	gl.bindBuffer(gl.ARRAY_BUFFER, sc.flatBuf);
	gl.bufferSubData(gl.ARRAY_BUFFER, 0, d);
	gl.uniform1f(flat.u("uPicOn"), color ? 0 : 1);
	if (color) {
		gl.uniform4f(flat.u("uColor"), color[0], color[1], color[2], 1);
	}
	gl.drawElements(gl.TRIANGLES, 6, gl.UNSIGNED_SHORT, 0);
};

// The court's picture onto the card (again, if it is a new one).
const courtPic = (sc: Scene, pic: HTMLCanvasElement) => {
	const { gl } = sc;
	gl.activeTexture(gl.TEXTURE0);
	gl.bindTexture(gl.TEXTURE_2D, sc.court);
	if (sc.courtOf === pic) {
		return;
	}
	sc.courtOf = pic;
	gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, pic);
	gl.texParameteri(
		gl.TEXTURE_2D,
		gl.TEXTURE_MIN_FILTER,
		gl.LINEAR_MIPMAP_LINEAR,
	);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
	gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
	gl.generateMipmap(gl.TEXTURE_2D);
};

// THE FRAME, as scene.ts drawFrame has it - onto the card's canvas, `W` x
// `H`. False if it can't be (the card lost): today's drawing, then.
export const drawGpu = (
	sc: GpuScene,
	f: Frame,
	W: number,
	H: number,
): boolean => {
	const { gl } = sc;
	if (sc.lost || gl.isContextLost()) {
		return false;
	}
	const { cam } = f;
	const ms: Record<string, number> = {};
	sc.ms = ms;
	sc.waited = 0;
	let from = performance.now();
	const lap = (part: string) => {
		const now = performance.now();
		ms[part] = (ms[part] ?? 0) + now - from;
		from = now;
	};
	if (f.black) {
		gl.viewport(0, 0, W, H);
		gl.clearColor(0.027, 0.024, 0.039, 1);
		gl.clear(gl.COLOR_BUFFER_BIT | gl.DEPTH_BUFFER_BIT);
		return true;
	}
	const m = bowlMatrix(cam);
	if (sc.floor === undefined) {
		sc.floor = floorOnto(gl) ?? null;
	}
	const fl = sc.floor;
	// The men: each one worked out (the page's own work), then drawn,
	// nearest first.
	sc.men ??= menOnto(gl);
	const men: ManReady[] = [];
	// Who shows in the floor (his shadows, his reflection) - not a man sat
	// down, nor the four at the table - and who has a pool of shadow round
	// his feet (not those four).
	const shines = new Set<ManReady>();
	const pooled = new Set<ManReady>();
	if (sc.men) {
		const players = f.moment.players;
		const onFloor = new Set(players.map((p) => p.pid));
		const all = [
			...players.map((st) => {
				const look = f.lookFor(st.pid);
				return {
					st,
					body: f.bodyFor(st.pid),
					look: f.moment.warm ? warmupLook(look, f.warmups[st.team]) : look,
					table: false,
				};
			}),
			...(f.venue ? [] : benchStates(f, onFloor)).map(({ st, warm }) => {
				const look = f.lookFor(st.pid);
				return {
					st,
					body: f.bodyFor(st.pid),
					look: warm ? warmupLook(look, f.warmups[st.team]) : look,
					table: false,
				};
			}),
			...(f.crew ?? []).map((c) => ({ ...c, table: atTable(c.st.pid) })),
		];
		let i = 0;
		for (const one of all) {
			if (!inSight(cam, one.st, one.body)) {
				continue;
			}
			const r = readyMan(cam, one.st, one.body, one.look, sc.ready[i]);
			sc.ready[i] = r;
			i += 1;
			men.push(r);
			if (!one.table) {
				pooled.add(r);
				if (one.st.anim !== "sit") {
					shines.add(r);
				}
			}
		}
		sc.ready.length = i;
		men.sort((a, b) => a.depth - b.depth);
		men.length = Math.min(men.length, MEN_MAX);
	}
	lap("men");
	// What the page draws, all on its sheet, sent to the card once (see
	// floor.ts PageSheet): the baskets, and the ball where nobody has it (in
	// a man's hands, his: see drawHands); the heads wanting drawing again;
	// over all of it, the board over center court, the name tag, the lights
	// down for the lineups, the confetti... (Painted and sent before anything
	// is drawn this frame: sent after the drawing had begun, it took a phone
	// about 2.5 ms however little of it there was - waiting, it seems, on
	// what had been drawn so far.)
	const sheet = sc.sheet;
	const screen = boardAt(f.tl, f.moment.t);
	sheetBegin(sheet);
	const hoops = fl ? itemsPut(sheet, cam, hoopsAndBalls(f, screen)) : [];
	const hd = headsPut(sc, f, men);
	const over = fl ? itemsPut(sheet, cam, overlays(f, screen)) : [];
	sheetPaint(sheet);
	lap("paint");
	sheetSend(gl, sheet);
	headsKept(sc, hd);
	// (Which way it went - see floor.ts sheetWay.)
	lap(["send", "sendC", "sendP"][sheetWay(sheet)]!);
	const look = bowlLookOf(f);
	const bowl =
		look !== undefined && paintBowlOn(gl, cam, f.arena.bowl!.data, look, W, H);
	if (!bowl) {
		gl.viewport(0, 0, W, H);
		gl.clearColor(0.027, 0.024, 0.039, 1);
		gl.clear(gl.COLOR_BUFFER_BIT | gl.DEPTH_BUFFER_BIT);
	}
	gl.disable(gl.BLEND);
	gl.disable(gl.CULL_FACE);
	gl.enable(gl.DEPTH_TEST);
	gl.depthMask(true);

	// The floor round the court and the court: over the bowl's own floor,
	// whatever it is (as the page's drawing lays them over it).
	gl.depthFunc(gl.ALWAYS);
	gl.useProgram(sc.flat.p);
	gl.uniformMatrix4fv(sc.flat.u("uM"), false, m);
	gl.uniform1i(sc.flat.u("uPic"), 0);
	gl.bindVertexArray(sc.flatVao);
	const E = FLOOR_EDGE;
	flatQuad(sc, E.x0, E.y0, E.x1, E.y1, rgb(f.apron ?? f.padColor, "#3a2a1a"));
	const court = f.arena.court;
	if (court) {
		courtPic(sc, court.canvas);
		flatQuad(sc, court.x, court.y, court.x + court.w, court.y + court.h);
	}
	gl.depthFunc(gl.LEQUAL);
	// (All of that behind everything on it, as the page lays it down first:
	// what is in front of what is only among the men, the baskets and the
	// table they stand in front of - not, say, the floor in front of the
	// basket's stand, nearer than its rim, hiding the foot of it.)
	gl.clear(gl.DEPTH_BUFFER_BIT);
	lap("bowl");
	// Round it: the table, the seats on the floor, the benches, the boards'
	// shine (as the page's drawing has them - see scene.ts dressFloor); the
	// court's lines; the lights' glints in the floor.
	if (fl && !f.venue) {
		const painter = floorPainter(gl, fl, cam);
		dressFloor(f, painter, boardAt(f.tl, f.moment.t), () => {});
		floorDraw(gl, fl, m);
		if (f.lineColor) {
			linesDraw(gl, fl, m, f.lineColor);
		}
		drawGlints(painter, cam);
		floorDraw(gl, fl, m);
	}
	lap("floor");

	if (sc.men) {
		const g = sc.men;
		// (The bowl and the floor have had the card's texture units.)
		menUnknown(g, true);
		menRows(gl, g, men);
		menCamera(gl, g, m);
		// What is laid over the floor under them - their reflections in it,
		// soft pools of shadow round their feet, the shadows they cast - each
		// over each pixel once (see once), blended in, behind all of them.
		gl.enable(gl.STENCIL_TEST);
		gl.clearStencil(0);
		gl.clear(gl.STENCIL_BUFFER_BIT);
		let ref = 0;
		const once = () => {
			ref += 1;
			if (ref > 255) {
				gl.clear(gl.STENCIL_BUFFER_BIT);
				ref = 1;
			}
			gl.stencilFunc(gl.NOTEQUAL, ref, 0xff);
			gl.stencilOp(gl.KEEP, gl.KEEP, gl.REPLACE);
		};
		gl.enable(gl.BLEND);
		gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
		gl.depthMask(false);
		gl.depthFunc(gl.ALWAYS);
		gl.enable(gl.CULL_FACE);
		gl.cullFace(MIRRORED_CULL);
		for (let i = men.length - 1; i >= 0; i--) {
			const man = men[i]!;
			if (!shines.has(man)) {
				continue;
			}
			once();
			drawMan(gl, g, man, i, { mode: 3, shine: SHINE }, W, H);
		}
		gl.disable(gl.CULL_FACE);
		gl.depthFunc(gl.LEQUAL);
		gl.disable(gl.STENCIL_TEST);
		if (fl) {
			pools(
				sc,
				fl,
				f,
				men.filter((man) => pooled.has(man)),
				m,
			);
		}
		gl.enable(gl.BLEND);
		gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
		gl.depthMask(false);
		gl.useProgram(g.prog.p);
		menUnknown(g, false);
		// (Drawn a third the size and laid over the floor drawn back up,
		// soft-edged, as a shadow is - see softOf. Sharp, if that can't be.)
		const soft = softOf(sc, W, H);
		if (soft) {
			gl.bindFramebuffer(gl.FRAMEBUFFER, soft.fb);
			gl.viewport(0, 0, soft.w, soft.h);
			gl.clearColor(0, 0, 0, 0);
			gl.clear(gl.COLOR_BUFFER_BIT | gl.STENCIL_BUFFER_BIT);
			ref = 0;
			gl.disable(gl.DEPTH_TEST);
		}
		gl.enable(gl.STENCIL_TEST);
		for (const [i, man] of men.entries()) {
			const lift = Math.max(0, man.st.z) / (man.body.H * 1.05);
			if (lift >= 1 || !shines.has(man)) {
				continue;
			}
			for (const [lean, run] of CASTS) {
				once();
				drawMan(
					gl,
					g,
					man,
					i,
					{
						mode: 2,
						cast: castOf(cam, man.st, lean, run),
						alpha: CAST_ALPHA * (1 - lift),
					},
					W,
					H,
				);
			}
		}
		gl.disable(gl.STENCIL_TEST);
		if (soft) {
			gl.bindFramebuffer(gl.FRAMEBUFFER, null);
			gl.viewport(0, 0, W, H);
			gl.enable(gl.DEPTH_TEST);
			gl.depthFunc(gl.ALWAYS);
			gl.useProgram(sc.flat.p);
			gl.uniformMatrix4fv(sc.flat.u("uM"), false, WHOLE);
			gl.bindVertexArray(sc.flatVao);
			gl.activeTexture(gl.TEXTURE0);
			gl.bindTexture(gl.TEXTURE_2D, soft.tex);
			flatQuad(sc, -1, -1, 1, 1);
			gl.depthFunc(gl.LEQUAL);
			gl.useProgram(g.prog.p);
			menUnknown(g, false);
		}
		gl.disable(gl.BLEND);
		gl.depthMask(true);
		lap("under");
		// Each man: the dark edge round him, then him.
		gl.enable(gl.CULL_FACE);
		for (const [i, man] of men.entries()) {
			gl.cullFace(EDGE_CULL);
			drawMan(gl, g, man, i, { mode: 1 }, W, H);
			gl.cullFace(BODY_CULL);
			drawMan(gl, g, man, i, { mode: 0 }, W, H);
		}
		gl.disable(gl.CULL_FACE);
		// Their hands, over them (blended in at their soft edges).
		gl.enable(gl.BLEND);
		gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
		drawAllHands(gl, g, men, W, H);
		gl.disable(gl.BLEND);
		lap("bodies");
	}
	if (fl) {
		itemsDraw(gl, fl, m, cam, sheet, hoops);
	}
	headsDraw(sc, f, hd.cells, m, W, H);
	if (fl) {
		itemsDraw(gl, fl, m, cam, sheet, over);
	}
	gl.bindVertexArray(null);
	lap("items");
	if (from - sc.cardAt > CARD_EVERY) {
		// (One pixel read back: only once the card has drawn all of it.)
		gl.readPixels(0, 0, 1, 1, gl.RGBA, gl.UNSIGNED_BYTE, ONE_PIXEL);
		sc.cardAt = performance.now();
		sc.cardMs = sc.waited = sc.cardAt - from;
	}
	return true;
};
const ONE_PIXEL = new Uint8Array(4);

// The shadows the men cast, drawn a third the size of the picture (on a
// sheet of their own) and laid over the floor drawn back up, so soft-edged
// (as sprite.ts darkOf has them today) - undefined if the card can't.
const SOFT = 3;
type Soft = {
	fb: WebGLFramebuffer;
	tex: WebGLTexture;
	stencil: WebGLRenderbuffer;
	w: number;
	h: number;
	ok: boolean;
};
const softOf = (sc: Scene, W: number, H: number): Soft | undefined => {
	const { gl } = sc;
	const w = Math.max(1, Math.ceil(W / SOFT));
	const h = Math.max(1, Math.ceil(H / SOFT));
	sc.soft ??= {
		fb: gl.createFramebuffer(),
		tex: gl.createTexture(),
		stencil: gl.createRenderbuffer(),
		w: 0,
		h: 0,
		ok: false,
	};
	const s = sc.soft;
	if (s.w !== w || s.h !== h) {
		s.w = w;
		s.h = h;
		gl.activeTexture(gl.TEXTURE0);
		gl.bindTexture(gl.TEXTURE_2D, s.tex);
		gl.texImage2D(
			gl.TEXTURE_2D,
			0,
			gl.RGBA8,
			w,
			h,
			0,
			gl.RGBA,
			gl.UNSIGNED_BYTE,
			null,
		);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
		gl.bindRenderbuffer(gl.RENDERBUFFER, s.stencil);
		gl.renderbufferStorage(gl.RENDERBUFFER, gl.DEPTH24_STENCIL8, w, h);
		gl.bindFramebuffer(gl.FRAMEBUFFER, s.fb);
		gl.framebufferTexture2D(
			gl.FRAMEBUFFER,
			gl.COLOR_ATTACHMENT0,
			gl.TEXTURE_2D,
			s.tex,
			0,
		);
		gl.framebufferRenderbuffer(
			gl.FRAMEBUFFER,
			gl.DEPTH_STENCIL_ATTACHMENT,
			gl.RENDERBUFFER,
			s.stencil,
		);
		s.ok =
			gl.checkFramebufferStatus(gl.FRAMEBUFFER) === gl.FRAMEBUFFER_COMPLETE;
		gl.bindFramebuffer(gl.FRAMEBUFFER, null);
	}
	return s.ok ? s : undefined;
};
// (The whole picture, corner to corner.)
const WHOLE = new Float32Array([
	1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1,
]);

// Whether any of a man can be in the picture - all of him, arms out,
// his reflection under him, his shadows either side - or he is right up
// against the camera or behind it (as today's drawing skips him: see
// sprite.ts NEAR_FT). Worked out before anything of him is.
const inSight = (cam: Camera, st: PlayerState, body: Body): boolean => {
	if (depthOf(cam, { x: st.x, y: st.y, z: 0 }) < 3) {
		return false;
	}
	const foot = project(cam, { x: st.x, y: st.y, z: 0 });
	const top = project(cam, {
		x: st.x,
		y: st.y,
		z: Math.max(0, st.z) + body.H + 1,
	});
	const up = foot.y - top.y;
	const half = 4 * foot.k + up;
	return (
		foot.x + half > 0 &&
		foot.x - half < cam.viewW &&
		foot.y + up > 0 &&
		top.y < cam.viewH
	);
};

// A shadow cast by the lights (see sprite.ts CASTS): today laid on the
// picture from his feet - for each pixel he is up, `lean` across it and
// `run` down it - here on the floor: so far across it (x) and toward the
// camera (y) for each foot up he is, to come out the same on the picture.
const castOf = (
	cam: Frame["cam"],
	st: { x: number; y: number },
	lean: number,
	run: number,
): [number, number] => {
	const o = project(cam, { x: st.x, y: st.y, z: 0 });
	const up = project(cam, { x: st.x, y: st.y, z: 1 });
	const ax = project(cam, { x: st.x + 1, y: st.y, z: 0 });
	const ay = project(cam, { x: st.x, y: st.y + 1, z: 0 });
	const h = o.y - up.y;
	// Across the picture and down it, for a foot each way over the floor.
	const a = ax.x - o.x;
	const b = ay.x - o.x;
	const c = ax.y - o.y;
	const d = ay.y - o.y;
	const det = a * d - b * c;
	if (Math.abs(det) < 1e-9) {
		return [0, 0];
	}
	const px = lean * h;
	const py = run * h;
	return [(d * px - b * py) / det, (a * py - c * px) / det];
};

// Soft pools of shadow round their feet, shrinking as they leave the floor
// (as scene.ts lays them), and under the ball.
const pools = (
	sc: Scene,
	fl: FloorGl,
	f: Frame,
	men: readonly ManReady[],
	m: Float32Array,
) => {
	const list = men.map(({ st }) => {
		const lift = Math.min(1, Math.max(0, st.z) / 4);
		return {
			x: st.x,
			y: st.y,
			r: 1.25 * (1 - lift * 0.35),
			dark: 1 - lift * 0.6,
		};
	});
	const ball = f.moment.ball;
	list.push({
		x: ball.x,
		y: ball.y,
		r: 0.45,
		dark: Math.max(0, 1 - ball.z / 14) * 0.9,
	});
	floorPools(sc.gl, fl, list);
	floorDraw(sc.gl, fl, m, true);
};

// Where a point is in the card's picture: its x, y (-1 to 1) and how near.
const clip = (m: Float32Array, p: { x: number; y: number; z: number }) => {
	const c = [0, 1, 2, 3].map(
		(i) => m[i]! * p.x + m[4 + i]! * p.y + m[8 + i]! * p.z + m[12 + i]!,
	);
	return { x: c[0]! / c[3]!, y: c[1]! / c[3]!, z: c[2]! / c[3]!, w: c[3]! };
};

// THE HEADS, as today's court draws them (see figure.ts drawHeadAt): each
// man's on a picture of its own on the card, drawn again only when it would
// come out different (see headKey) - a man sat still on the bench, the
// camera going by, kept as it was, moved over; each stood up where his head
// is, furthest first, over what is behind it.
type HeadPic = {
	tex: WebGLTexture;
	key: number[];
	look: ManReady["look"];
	// Its corner from the middle of his face (px), its size, and his head's
	// radius on it: stretched to his size now (see figure.ts headKey).
	dx: number;
	dy: number;
	w: number;
	h: number;
	r: number;
	// The frame it was last wanted, and drawn.
	seen: number;
	drawn: number;
};
// (One not wanted for so many frames is let go.)
const HEAD_KEPT = 120;
// (So many drawn again in a frame, at most, of those only a little off their
// pictures - the one kept waiting longest first, the rest a frame or two
// later. One with no picture yet, or far off it - a cut to another camera -
// drawn whatever: further off than this, a quarter of a px each.)
const HEAD_REDRAWS = 5;
const HEAD_FAR = 4;
const keyOff = (a: readonly number[], b: readonly number[]) => {
	if (a.length !== b.length) {
		return Infinity;
	}
	// (Its size a step at a time - see HEAD_SIZES - each a few %.)
	let off = Math.abs(a[0]! - b[0]!) * 4;
	for (let i = 1; i < a.length; i++) {
		off = Math.max(off, Math.abs(a[i]! - b[i]!));
	}
	return off;
};
// Which heads want drawing again (see keyOff), on the page's sheet; after it
// is on the card, each copied into his own picture (headsKept); then all of
// them drawn (headsDraw).
type HeadCell = {
	man: ManReady;
	head: V3;
	mid: { x: number; y: number; z: number };
	x: number;
	y: number;
	r: number;
	key: number[];
	pic: HeadPic | undefined;
	off: number;
};
type HeadsPut = {
	cells: HeadCell[];
	redrawn: { cell: HeadCell; put: Put }[];
};
const headsPut = (sc: Scene, f: Frame, men: readonly ManReady[]): HeadsPut => {
	const { cam } = f;
	sc.frame += 1;
	const cells = men.map((man): HeadCell => {
		const { st, body } = man;
		const { q } = man.b;
		// (Where today's drawing has his head - see figure.ts headOf - and
		// the middle of his face on the picture, as the sculptor has it.)
		const head = headOf(st, body, q);
		const mid = bodyPoint(st, head);
		const { key, r } = headKey(cam, st, body, q, head);
		const p = project(cam, mid);
		const pic = sc.headPics.get(st.pid);
		const off = pic && pic.look === man.look ? keyOff(pic.key, key) : Infinity;
		return { man, head, mid, x: p.x, y: p.y - r * FACE_LIFT, r, key, pic, off };
	});
	const due = cells
		.filter((c) => c.off > 0)
		.sort(
			(a, b) =>
				Number(b.off > HEAD_FAR) - Number(a.off > HEAD_FAR) ||
				(a.pic?.drawn ?? 0) - (b.pic?.drawn ?? 0),
		);
	const redrawn: HeadsPut["redrawn"] = [];
	for (const [n, c] of due.entries()) {
		if (n >= HEAD_REDRAWS && c.off <= HEAD_FAR) {
			break;
		}
		const { st, body, look } = c.man;
		const { x, y, r } = c;
		const x0 = Math.floor(x - r * 1.9) - 2;
		const y0 = Math.floor(y - r * 2) - 2;
		const w = Math.ceil(x + r * 1.9) + 2 - x0;
		const h = Math.ceil(y + r * 1.6) + 2 - y0;
		const q = c.man.b.q;
		const put = sheetPut(sc.sheet, x0, y0, w, h, (ctx) =>
			drawHeadAt(ctx, cam, st, body, look, c.head, q),
		);
		if (put) {
			redrawn.push({ cell: c, put });
		}
	}
	for (const c of cells) {
		if (c.pic) {
			c.pic.seen = sc.frame;
		}
	}
	return { cells, redrawn };
};
const headsKept = (sc: Scene, hd: HeadsPut) => {
	const { gl } = sc;
	// (Not one there was no room for on the sheet: his old picture kept.)
	const into = hd.redrawn
		.filter(({ put }) => put.sx >= 0)
		.map(({ cell: c, put }) => {
			const made = !!c.pic && c.pic.w === put.w && c.pic.h === put.h;
			const pic = c.pic ?? {
				tex: gl.createTexture(),
				key: c.key,
				look: c.man.look,
				dx: 0,
				dy: 0,
				w: 0,
				h: 0,
				r: 1,
				seen: sc.frame,
				drawn: 0,
			};
			pic.key = c.key;
			pic.look = c.man.look;
			pic.dx = put.x0 - c.x;
			pic.dy = put.y0 - c.y;
			pic.w = put.w;
			pic.h = put.h;
			pic.r = c.r;
			pic.drawn = sc.frame;
			sc.headPics.set(c.man.st.pid, pic);
			c.pic = pic;
			return { put, tex: pic.tex, made, filter: gl.LINEAR };
		});
	sheetCopy(gl, sc.sheet, into);
	for (const [pid, pic] of sc.headPics) {
		if (sc.frame - pic.seen > HEAD_KEPT) {
			gl.deleteTexture(pic.tex);
			sc.headPics.delete(pid);
		}
	}
};
const headsDraw = (
	sc: Scene,
	f: Frame,
	cells: readonly HeadCell[],
	m: Float32Array,
	W: number,
	H: number,
) => {
	const { gl } = sc;
	const { cam } = f;
	if (cells.length === 0) {
		return;
	}
	const { head } = sc;
	gl.useProgram(head.p);
	gl.bindVertexArray(sc.headVao);
	gl.activeTexture(gl.TEXTURE0);
	gl.uniform1i(head.u("uPic"), 0);
	gl.uniform2f(head.u("uSize"), W, H);
	gl.enable(gl.BLEND);
	gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
	gl.depthMask(false);
	const toCam = (p: { x: number; y: number; z: number }, d: number) => {
		const dx = cam.pos.x - p.x;
		const dy = cam.pos.y - p.y;
		const dz = cam.pos.z - p.z;
		const l = Math.hypot(dx, dy, dz) || 1;
		return {
			x: p.x + (dx / l) * d,
			y: p.y + (dy / l) * d,
			z: p.z + (dz / l) * d,
		};
	};
	for (let i = cells.length - 1; i >= 0; i--) {
		const c = cells[i]!;
		const { mid, pic } = c;
		if (!pic) {
			continue;
		}
		const zc = clip(m, mid).z;
		const zf = clip(m, toCam(mid, c.man.body.headR)).z;
		// (Stretched about the middle of his face, from his size on it.)
		const k = c.r / pic.r;
		const x0 = c.x + pic.dx * k;
		const y0 = c.y + pic.dy * k;
		gl.bindTexture(gl.TEXTURE_2D, pic.tex);
		gl.uniform4f(head.u("uBox"), x0, y0, x0 + pic.w * k, y0 + pic.h * k);
		gl.uniform4f(head.u("uUv"), 0, 0, 1, 1);
		gl.uniform3f(head.u("uC"), c.x, c.y, c.r);
		gl.uniform2f(head.u("uZ"), zc, zf);
		gl.drawElements(gl.TRIANGLES, 6, gl.UNSIGNED_SHORT, 0);
	}
	gl.depthMask(true);
	gl.disable(gl.BLEND);
};
