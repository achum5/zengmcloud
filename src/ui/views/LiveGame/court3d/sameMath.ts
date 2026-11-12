// THE SAME GAME ON EVERY DEVICE.
//
// A game is staged on each device that shows it (see stageCourt), and the
// shot chart one device saved is the one everybody else's box score shows -
// so every device has to stage the very same game: every man where he is,
// every shot from where the chart has it. Staged by the arithmetic the
// browser has, it was not. Sines, cosines, arctangents, logs, powers and
// hypotenuses are each browser's own (Chrome's are not Safari's, in the
// last bit or two); one bit tipped one choice early in a game one way on a
// phone and the other way on a computer, and from there on the two games
// went their own ways - two shots in three a foot and more apart, some of
// them different shots altogether.
//
// So while a game is staged, those are worked out here instead: V8's own
// sums as node has them (fdlibm's, and its hypot) - so the tests' games,
// staged on node, are staged as they always were - in nothing but adding,
// multiplying, dividing, square roots and the bits of a number, which
// every browser does to the bit: the same everywhere.
// Only while staging (see sameEverywhere): drawing the picture, the
// browser's own are quicker, and a bit either way makes no odds to it.

const f64 = new Float64Array(1);
const u32 = new Uint32Array(f64.buffer);
// (Which of the two words is the high one, on this machine.)
const HI = new Uint8Array(new Uint16Array([1]).buffer)[0] === 1 ? 1 : 0;
const LO = 1 - HI;
const highOf = (x: number) => {
	f64[0] = x;
	return u32[HI]! | 0;
};
// 2^k, exactly.
const twoTo = (k: number): number => {
	if (k > 1023) {
		return twoTo(1023) * twoTo(k - 1023);
	}
	if (k < -1022) {
		return twoTo(-1022) * twoTo(k + 1022);
	}
	u32[LO] = 0;
	u32[HI] = ((k + 1023) << 20) >>> 0;
	return f64[0]!;
};

// ---- sin, cos, tan: fdlibm's, as V8 has them ---------------------------------

const lowOf = (x: number) => {
	f64[0] = x;
	return u32[LO]!;
};
const withLow0 = (x: number) => {
	f64[0] = x;
	u32[LO] = 0;
	return f64[0]!;
};
const withHigh = (x: number, hi: number) => {
	f64[0] = x;
	u32[HI] = hi >>> 0;
	return f64[0]!;
};
const S1 = -0.16666666666666632;
const S2 = 0.00833333333332249;
const S3 = -0.0001984126982985795;
const S4 = 2.7557313707070068e-6;
const S5 = -2.5050760253406863e-8;
const S6 = 1.58969099521155e-10;
// The sine of x + y, x within a right angle's half; y, all of it there is
// past x (iy 0: no y).
const kSin = (x: number, y: number, iy: number) => {
	if ((highOf(x) & 0x7fffffff) < 0x3e400000) {
		return x;
	}
	const z = x * x;
	const v = z * x;
	const r = S2 + z * (S3 + z * (S4 + z * (S5 + z * S6)));
	return iy === 0
		? x + v * (S1 + z * r)
		: x - (z * (0.5 * y - v * r) - y - v * S1);
};
const C1 = 0.0416666666666666;
const C2 = -0.001388888888887411;
const C3 = 2.480158728947673e-5;
const C4 = -2.7557314351390663e-7;
const C5 = 2.087572321298175e-9;
const C6 = -1.1359647557788195e-11;
const kCos = (x: number, y: number) => {
	const ix = highOf(x) & 0x7fffffff;
	if (ix < 0x3e400000) {
		return 1;
	}
	const z = x * x;
	const r = z * (C1 + z * (C2 + z * (C3 + z * (C4 + z * (C5 + z * C6)))));
	if (ix < 0x3fd33333) {
		return 1 - (0.5 * z - (z * r - x * y));
	}
	const qx = ix > 0x3fe90000 ? 0.28125 : withHigh(0, ix - 0x00200000);
	const iz = 0.5 * z - qx;
	const a = 1 - qx;
	return a - (iz - (z * r - x * y));
};
const TT = [
	0.3333333333333341, 0.13333333333320124, 0.05396825397622605,
	0.021869488294859542, 0.0088632398235993, 0.0035920791075913124,
	0.0014562094543252903, 0.0005880412408202641, 0.0002464631348184699,
	7.817944429395571e-5, 7.140724913826082e-5, -1.8558637485527546e-5,
	2.590730518636337e-5,
];
const PIO4 = 0.7853981633974483;
const PIO4LO = 3.061616997868383e-17;
const kTan = (x0: number, y0: number, iy: number): number => {
	let x = x0;
	let y = y0;
	const hx = highOf(x);
	const ix = hx & 0x7fffffff;
	if (ix < 0x3e300000) {
		if ((ix | lowOf(x) | (iy + 1)) === 0) {
			return 1 / Math.abs(x);
		}
		if (iy === 1) {
			return x;
		}
		const w = x + y;
		const z = withLow0(w);
		const v = y - (z - x);
		const a = -1 / w;
		const t = withLow0(a);
		const s = 1 + t * z;
		return t + a * (s + t * v);
	}
	if (ix >= 0x3fe59428) {
		if (hx < 0) {
			x = -x;
			y = -y;
		}
		const z = PIO4 - x;
		const w = PIO4LO - y;
		x = z + w;
		y = 0;
	}
	const z = x * x;
	let w = z * z;
	let r =
		TT[1]! +
		w * (TT[3]! + w * (TT[5]! + w * (TT[7]! + w * (TT[9]! + w * TT[11]!))));
	let v =
		z *
		(TT[2]! +
			w * (TT[4]! + w * (TT[6]! + w * (TT[8]! + w * (TT[10]! + w * TT[12]!)))));
	const s = z * x;
	r = y + z * (s * (r + v) + y);
	r += TT[0]! * s;
	w = x + r;
	if (ix >= 0x3fe59428) {
		v = iy;
		return (1 - ((hx >> 30) & 2)) * (v - 2 * (x - ((w * w) / (w + v) - r)));
	}
	if (iy === 1) {
		return w;
	}
	const zz = withLow0(w);
	v = r - (zz - x);
	const a = -1 / w;
	const t = withLow0(a);
	const s2 = 1 + t * zz;
	return t + a * (s2 + t * v);
};

// x less a whole number n of right angles: n, and the rest in two parts
// (red.y0, red.y1) - fdlibm's own way of it, to the bit.
const red = { y0: 0, y1: 0 };
const INV_PIO2 = 0.6366197723675814;
const PIO2_1 = 1.5707963267341256;
const PIO2_1T = 6.077100506506192e-11;
const PIO2_2 = 6.077100506303966e-11;
const PIO2_2T = 2.0222662487959506e-21;
const PIO2_3 = 2.0222662487111665e-21;
const PIO2_3T = 8.4784276603689e-32;
const NPIO2_HW = [
	0x3ff921fb, 0x400921fb, 0x4012d97c, 0x401921fb, 0x401f6a7a, 0x4022d97c,
	0x4025fdbb, 0x402921fb, 0x402c463a, 0x402f6a7a, 0x4031475c, 0x4032d97c,
	0x40346b9c, 0x4035fdbb, 0x40378fdb, 0x403921fb, 0x403ab41b, 0x403c463a,
	0x403dd85a, 0x403f6a7a, 0x40407e4c, 0x4041475c, 0x4042106c, 0x4042d97c,
	0x4043a28c, 0x40446b9c, 0x404534ac, 0x4045fdbb, 0x4046c6cb, 0x40478fdb,
	0x404858eb, 0x404921fb,
];
const remPio2 = (x: number): number => {
	const hx = highOf(x);
	const ix = hx & 0x7fffffff;
	if (ix <= 0x3fe921fb) {
		red.y0 = x;
		red.y1 = 0;
		return 0;
	}
	if (ix < 0x4002d97c) {
		if (hx > 0) {
			let z = x - PIO2_1;
			if (ix !== 0x3ff921fb) {
				red.y0 = z - PIO2_1T;
				red.y1 = z - red.y0 - PIO2_1T;
			} else {
				z -= PIO2_2;
				red.y0 = z - PIO2_2T;
				red.y1 = z - red.y0 - PIO2_2T;
			}
			return 1;
		}
		let z = x + PIO2_1;
		if (ix !== 0x3ff921fb) {
			red.y0 = z + PIO2_1T;
			red.y1 = z - red.y0 + PIO2_1T;
		} else {
			z += PIO2_2;
			red.y0 = z + PIO2_2T;
			red.y1 = z - red.y0 + PIO2_2T;
		}
		return -1;
	}
	if (ix > 0x413921fb) {
		return remPio2Far(x, hx, ix);
	}
	const t0 = Math.abs(x);
	const n = Math.trunc(t0 * INV_PIO2 + 0.5);
	let r = t0 - n * PIO2_1;
	let w = n * PIO2_1T;
	red.y0 = r - w;
	if (n >= 32 || ix === NPIO2_HW[n - 1]) {
		const j = ix >> 20;
		let i = j - ((highOf(red.y0) >> 20) & 0x7ff);
		if (i > 16) {
			let t = r;
			w = n * PIO2_2;
			r = t - w;
			w = n * PIO2_2T - (t - r - w);
			red.y0 = r - w;
			i = j - ((highOf(red.y0) >> 20) & 0x7ff);
			if (i > 49) {
				t = r;
				w = n * PIO2_3;
				r = t - w;
				w = n * PIO2_3T - (t - r - w);
				red.y0 = r - w;
			}
		}
	}
	red.y1 = r - red.y0 - w;
	if (hx < 0) {
		red.y0 = -red.y0;
		red.y1 = -red.y1;
		return -n;
	}
	return n;
};
// (Further round than 2^19 right angles - any number of turns of a
// stride's phase in a whole game: 2/pi to 1584 bits.)
const TWO_OVER_PI = [
	0xa2f983, 0x6e4e44, 0x1529fc, 0x2757d1, 0xf534dd, 0xc0db62, 0x95993c,
	0x439041, 0xfe5163, 0xabdebb, 0xc561b7, 0x246e3a, 0x424dd2, 0xe00649,
	0x2eea09, 0xd1921c, 0xfe1deb, 0x1cb129, 0xa73ee8, 0x8235f5, 0x2ebb44,
	0x84e99c, 0x7026b4, 0x5f7e41, 0x3991d6, 0x398353, 0x39f49c, 0x845f8b,
	0xbdf928, 0x3b1ff8, 0x97ffde, 0x05980f, 0xef2f11, 0x8b5a0a, 0x6d1f6d,
	0x367ecf, 0x27cb09, 0xb74f46, 0x3f669e, 0x5fea2d, 0x7527ba, 0xc7ebe5,
	0xf17b3d, 0x0739f7, 0x8a5292, 0xea6bfb, 0x5fb11f, 0x8d5d08, 0x560330,
	0x46fc7b, 0x6babf0, 0xcfbc20, 0x9af436, 0x1da9e3, 0x91615e, 0xe61b08,
	0x659985, 0x5f14a0, 0x68408d, 0xffd880, 0x4d7327, 0x310606, 0x1556ca,
	0x73a8c9, 0x60e27b, 0xc08c6b,
];
const PIO2_PARTS = [
	1.570796251296997, 7.549789415861596e-8, 5.390302529957765e-15,
	3.282003415807913e-22, 1.270655753080676e-29, 1.2293330898111133e-36,
	2.7337005381646456e-44, 2.1674168387780482e-51,
];
const TWO24 = 16777216;
const TWON24 = 5.960464477539063e-8;
const remPio2Far = (x: number, hx: number, ix: number): number => {
	if (ix >= 0x7ff00000) {
		red.y0 = Number.NaN;
		red.y1 = Number.NaN;
		return 0;
	}
	// (|x| scaled to 2^23 and more, in three 24-bit pieces.)
	const e0 = (ix >> 20) - 1046;
	f64[0] = 0;
	u32[LO] = lowOf(x);
	u32[HI] = (ix - (e0 << 20)) >>> 0;
	let z = f64[0]!;
	const tx = [0, 0, 0];
	for (let i = 0; i < 2; i++) {
		tx[i] = Math.trunc(z);
		z = (z - tx[i]!) * TWO24;
	}
	tx[2] = z;
	let nx = 3;
	while (tx[nx - 1] === 0) {
		nx--;
	}
	const n = kRemPio2(tx, e0, nx);
	if (hx < 0) {
		red.y0 = -red.y0;
		red.y1 = -red.y1;
		return -n;
	}
	return n;
};
const kRemPio2 = (x: number[], e0: number, nx: number): number => {
	const jk = 4;
	const jx = nx - 1;
	const jv = Math.max(0, Math.trunc((e0 - 3) / 24));
	let q0 = e0 - 24 * (jv + 1);
	const f: number[] = new Array<number>(20).fill(0);
	const q: number[] = new Array<number>(20).fill(0);
	const fq: number[] = new Array<number>(20).fill(0);
	const iq = new Int32Array(20);
	for (let i = 0, j = jv - jx; i <= jx + jk; i++, j++) {
		f[i] = j < 0 ? 0 : TWO_OVER_PI[j]!;
	}
	for (let i = 0; i <= jk; i++) {
		let fw = 0;
		for (let j = 0; j <= jx; j++) {
			fw += x[j]! * f[jx + i - j]!;
		}
		q[i] = fw;
	}
	let jz = jk;
	let z = 0;
	let n = 0;
	let ih = 0;
	for (;;) {
		z = q[jz]!;
		for (let i = 0, j = jz; j > 0; i++, j--) {
			const fw = Math.trunc(TWON24 * z);
			iq[i] = Math.trunc(z - TWO24 * fw);
			z = q[j - 1]! + fw;
		}
		z *= twoTo(q0);
		z -= 8 * Math.floor(z * 0.125);
		n = Math.trunc(z);
		z -= n;
		ih = 0;
		if (q0 > 0) {
			const i = iq[jz - 1]! >> (24 - q0);
			n += i;
			iq[jz - 1]! -= i << (24 - q0);
			ih = iq[jz - 1]! >> (23 - q0);
		} else if (q0 === 0) {
			ih = iq[jz - 1]! >> 23;
		} else if (z >= 0.5) {
			ih = 2;
		}
		if (ih > 0) {
			n += 1;
			let carry = 0;
			for (let i = 0; i < jz; i++) {
				const j = iq[i]!;
				if (carry === 0) {
					if (j !== 0) {
						carry = 1;
						iq[i] = 0x1000000 - j;
					}
				} else {
					iq[i] = 0xffffff - j;
				}
			}
			if (q0 === 1) {
				iq[jz - 1]! &= 0x7fffff;
			} else if (q0 === 2) {
				iq[jz - 1]! &= 0x3fffff;
			}
			if (ih === 2) {
				z = 1 - z;
				if (carry !== 0) {
					z -= twoTo(q0);
				}
			}
		}
		if (z !== 0) {
			break;
		}
		let j = 0;
		for (let i = jz - 1; i >= jk; i--) {
			j |= iq[i]!;
		}
		if (j !== 0) {
			break;
		}
		// (Not enough of 2/pi yet: more of it.)
		let k = 1;
		while (jk >= k && iq[jk - k] === 0) {
			k++;
		}
		for (let i = jz + 1; i <= jz + k; i++) {
			f[jx + i] = TWO_OVER_PI[jv + i]!;
			let fw = 0;
			for (let jj = 0; jj <= jx; jj++) {
				fw += x[jj]! * f[jx + i - jj]!;
			}
			q[i] = fw;
		}
		jz += k;
	}
	if (z === 0) {
		jz -= 1;
		q0 -= 24;
		while (iq[jz] === 0) {
			jz--;
			q0 -= 24;
		}
	} else {
		z *= twoTo(-q0);
		if (z >= TWO24) {
			const fw = Math.trunc(TWON24 * z);
			iq[jz] = Math.trunc(z - TWO24 * fw);
			jz += 1;
			q0 += 24;
			iq[jz] = fw;
		} else {
			iq[jz] = Math.trunc(z);
		}
	}
	let fw = twoTo(q0);
	for (let i = jz; i >= 0; i--) {
		q[i] = fw * iq[i]!;
		fw *= TWON24;
	}
	for (let i = jz; i >= 0; i--) {
		fw = 0;
		for (let k = 0; k <= jk && k <= jz - i; k++) {
			fw += PIO2_PARTS[k]! * q[i + k]!;
		}
		fq[jz - i] = fw;
	}
	fw = 0;
	for (let i = jz; i >= 0; i--) {
		fw += fq[i]!;
	}
	red.y0 = ih === 0 ? fw : -fw;
	fw = fq[0]! - fw;
	for (let i = 1; i <= jz; i++) {
		fw += fq[i]!;
	}
	red.y1 = ih === 0 ? fw : -fw;
	return n & 7;
};
export const sameSin = (x: number): number => {
	const ix = highOf(x) & 0x7fffffff;
	if (ix <= 0x3fe921fb) {
		return kSin(x, 0, 0);
	}
	if (ix >= 0x7ff00000) {
		return Number.NaN;
	}
	switch (remPio2(x) & 3) {
		case 0:
			return kSin(red.y0, red.y1, 1);
		case 1:
			return kCos(red.y0, red.y1);
		case 2:
			return -kSin(red.y0, red.y1, 1);
		default:
			return -kCos(red.y0, red.y1);
	}
};
export const sameCos = (x: number): number => {
	const ix = highOf(x) & 0x7fffffff;
	if (ix <= 0x3fe921fb) {
		return kCos(x, 0);
	}
	if (ix >= 0x7ff00000) {
		return Number.NaN;
	}
	switch (remPio2(x) & 3) {
		case 0:
			return kCos(red.y0, red.y1);
		case 1:
			return -kSin(red.y0, red.y1, 1);
		case 2:
			return -kCos(red.y0, red.y1);
		default:
			return kSin(red.y0, red.y1, 1);
	}
};
export const sameTan = (x: number): number => {
	const ix = highOf(x) & 0x7fffffff;
	if (ix <= 0x3fe921fb) {
		return kTan(x, 0, 1);
	}
	if (ix >= 0x7ff00000) {
		return Number.NaN;
	}
	const n = remPio2(x);
	return kTan(red.y0, red.y1, 1 - ((n & 1) << 1));
};

// ---- atan, atan2, asin, acos -----------------------------------------------

const ATAN_HI = [
	0.4636476090008061, 0.7853981633974483, 0.982793723247329, 1.5707963267948966,
];
const ATAN_LO = [
	2.2698777452961687e-17, 3.061616997868383e-17, 1.3903311031230998e-17,
	6.123233995736766e-17,
];
const AT = [
	0.3333333333333293, -0.19999999999876483, 0.14285714272503466,
	-0.11111110405462356, 0.09090887133436507, -0.0769187620504483,
	0.06661073137387531, -0.058335701337905735, 0.049768779946159324,
	-0.036531572744216916, 0.016285820115365782,
];
export const sameAtan = (x0: number): number => {
	if (Number.isNaN(x0)) {
		return Number.NaN;
	}
	const hx = highOf(x0);
	const ix = hx & 0x7fffffff;
	let x = x0;
	let id: number;
	if (ix >= 0x44100000) {
		return hx < 0 ? -ATAN_HI[3]! - ATAN_LO[3]! : ATAN_HI[3]! + ATAN_LO[3]!;
	}
	if (ix < 0x3fdc0000) {
		if (ix < 0x3e400000) {
			return x;
		}
		id = -1;
	} else {
		x = Math.abs(x);
		if (ix < 0x3ff30000) {
			if (ix < 0x3fe60000) {
				id = 0;
				x = (2 * x - 1) / (2 + x);
			} else {
				id = 1;
				x = (x - 1) / (x + 1);
			}
		} else if (ix < 0x40038000) {
			id = 2;
			x = (x - 1.5) / (1 + 1.5 * x);
		} else {
			id = 3;
			x = -1 / x;
		}
	}
	const z = x * x;
	const w = z * z;
	const s1 =
		z *
		(AT[0]! +
			w * (AT[2]! + w * (AT[4]! + w * (AT[6]! + w * (AT[8]! + w * AT[10]!)))));
	const s2 =
		w * (AT[1]! + w * (AT[3]! + w * (AT[5]! + w * (AT[7]! + w * AT[9]!))));
	if (id < 0) {
		return x - x * (s1 + s2);
	}
	const r = ATAN_HI[id]! - (x * (s1 + s2) - ATAN_LO[id]! - x);
	return hx < 0 ? -r : r;
};
const PI = 3.141592653589793;
const PI_LO = 1.2246467991473532e-16;
const PI_O_2 = 1.5707963267948966;
const PI_O_4 = 0.7853981633974483;
export const sameAtan2 = (y: number, x: number): number => {
	// (Neither given - as the browser's own has it - is not a number.)
	if (x === undefined || Number.isNaN(x) || Number.isNaN(y)) {
		return Number.NaN;
	}
	f64[0] = x;
	const hx = u32[HI]! | 0;
	const lx = u32[LO]!;
	f64[0] = y;
	const hy = u32[HI]! | 0;
	const ly = u32[LO]!;
	const ix = hx & 0x7fffffff;
	const iy = hy & 0x7fffffff;
	if (((hx - 0x3ff00000) | lx) === 0) {
		return sameAtan(y);
	}
	// (2 for x below 0, and 1 for y.)
	let m = ((hy >> 31) & 1) | ((hx >> 30) & 2);
	if ((iy | ly) === 0) {
		return m < 2 ? y : m === 2 ? PI : -PI;
	}
	if ((ix | lx) === 0) {
		return hy < 0 ? -PI_O_2 : PI_O_2;
	}
	if (ix === 0x7ff00000) {
		if (iy === 0x7ff00000) {
			return [PI_O_4, -PI_O_4, 3 * PI_O_4, -3 * PI_O_4][m]!;
		}
		return [0, -0, PI, -PI][m]!;
	}
	if (iy === 0x7ff00000) {
		return hy < 0 ? -PI_O_2 : PI_O_2;
	}
	const k = (iy - ix) >> 20;
	let z: number;
	if (k > 60) {
		z = PI_O_2 + 0.5 * PI_LO;
		m &= 1;
	} else if (hx < 0 && k < -60) {
		z = 0;
	} else {
		z = sameAtan(Math.abs(y / x));
	}
	switch (m) {
		case 0:
			return z;
		case 1:
			return -z;
		case 2:
			return PI - (z - PI_LO);
		default:
			return z - PI_LO - PI;
	}
};
const PIO2_HI = 1.5707963267948966;
const PIO2_LO = 6.123233995736766e-17;
const PIO4_HI = 0.7853981633974483;
const PS0 = 0.16666666666666666;
const PS1 = -0.3255658186224009;
const PS2 = 0.20121253213486293;
const PS3 = -0.04005553450067941;
const PS4 = 0.0007915349942898145;
const PS5 = 3.479331075960212e-5;
const QS1 = -2.403394911734414;
const QS2 = 2.0209457602335057;
const QS3 = -0.6882839716054533;
const QS4 = 0.07703815055590194;
const pOf = (t: number) =>
	t * (PS0 + t * (PS1 + t * (PS2 + t * (PS3 + t * (PS4 + t * PS5)))));
const qOf = (t: number) => 1 + t * (QS1 + t * (QS2 + t * (QS3 + t * QS4)));
export const sameAsin = (x: number): number => {
	const hx = highOf(x);
	const ix = hx & 0x7fffffff;
	if (ix >= 0x3ff00000) {
		if (((ix - 0x3ff00000) | lowOf(x)) === 0) {
			return x * PIO2_HI + x * PIO2_LO;
		}
		return Number.NaN;
	}
	if (ix < 0x3fe00000) {
		if (ix < 0x3e400000) {
			return x;
		}
		const t = x * x;
		const w = pOf(t) / qOf(t);
		return x + x * w;
	}
	let w = 1 - Math.abs(x);
	let t = w * 0.5;
	let p = pOf(t);
	let q = qOf(t);
	const s = Math.sqrt(t);
	if (ix >= 0x3fef3333) {
		w = p / q;
		t = PIO2_HI - (2 * (s + s * w) - PIO2_LO);
	} else {
		w = withLow0(s);
		const c = (t - w * w) / (s + w);
		const r = p / q;
		p = 2 * s * r - (PIO2_LO - 2 * c);
		q = PIO4_HI - 2 * w;
		t = PIO4_HI - (p - q);
	}
	return hx > 0 ? t : -t;
};
export const sameAcos = (x: number): number => {
	const hx = highOf(x);
	const ix = hx & 0x7fffffff;
	if (ix >= 0x3ff00000) {
		if (((ix - 0x3ff00000) | lowOf(x)) === 0) {
			return hx > 0 ? 0 : PI + 2 * PIO2_LO;
		}
		return Number.NaN;
	}
	if (ix < 0x3fe00000) {
		if (ix <= 0x3c600000) {
			return PIO2_HI + PIO2_LO;
		}
		const z = x * x;
		const r = pOf(z) / qOf(z);
		return PIO2_HI - (x - (PIO2_LO - x * r));
	}
	if (hx < 0) {
		const z = (1 + x) * 0.5;
		const s = Math.sqrt(z);
		const r = pOf(z) / qOf(z);
		const w = r * s - PIO2_LO;
		return PI - 2 * (s + w);
	}
	const z = (1 - x) * 0.5;
	const s = Math.sqrt(z);
	const df = withLow0(s);
	const cc = (z - df * df) / (s + df);
	const r = pOf(z) / qOf(z);
	const w = r * s + cc;
	return 2 * (df + w);
};

// ---- exp, log -------------------------------------------------------------

const LN2_HI = 0.6931471803691238;
const LN2_LO = 1.9082149292705877e-10;
const INV_LN2 = 1.4426950408889634;
const HALF_LN2 = 3.4657359027997264e-1;
const P1 = 0.16666666666666602;
const P2 = -0.0027777777777015593;
const P3 = 6.613756321437934e-5;
const P4 = -1.6533902205465252e-6;
const P5 = 4.1381367970572385e-8;
export const sameExp = (x0: number): number => {
	if (Number.isNaN(x0)) {
		return Number.NaN;
	}
	if (x0 > 709.782712893384) {
		return Infinity;
	}
	if (x0 < -745.1332191019411) {
		return 0;
	}
	// (V8's has e itself for e^1, a bit nearer than the sums give.)
	if (x0 === 1) {
		return Math.E;
	}
	let x = x0;
	let k = 0;
	let hi = 0;
	let lo = 0;
	const ax = Math.abs(x);
	if (ax > HALF_LN2) {
		k = Math.trunc(INV_LN2 * x + (x < 0 ? -0.5 : 0.5));
		hi = x - k * LN2_HI;
		lo = k * LN2_LO;
		x = hi - lo;
	} else if (ax < 3.725290298461914e-9) {
		return 1 + x;
	}
	const t = x * x;
	const c = x - t * (P1 + t * (P2 + t * (P3 + t * (P4 + t * P5))));
	if (k === 0) {
		return 1 - ((x * c) / (c - 2) - x);
	}
	const y = 1 - (lo - (x * c) / (2 - c) - hi);
	return y * twoTo(k);
};
const LG1 = 0.6666666666666735;
const LG2 = 0.3999999999940942;
const LG3 = 0.2857142874366239;
const LG4 = 0.22222198432149784;
const LG5 = 0.1818357216161805;
const LG6 = 0.15313837699209373;
const LG7 = 0.14798198605116586;
export const sameLog = (x0: number): number => {
	if (!(x0 > 0)) {
		return x0 === 0 ? -Infinity : Number.NaN;
	}
	if (x0 === Infinity) {
		return Infinity;
	}
	// (Its bits read and written straight off the one number - this is asked
	// a great deal: every run's shape.)
	let k = 0;
	f64[0] = x0;
	let hx = u32[HI]! | 0;
	if (hx < 0x00100000) {
		k = -54;
		f64[0] = x0 * 18014398509481984;
		hx = u32[HI]! | 0;
	}
	k += (hx >> 20) - 1023;
	hx &= 0x000fffff;
	const i0 = (hx + 0x95f64) & 0x100000;
	u32[HI] = (hx | (i0 ^ 0x3ff00000)) >>> 0;
	k += i0 >> 20;
	const f = f64[0]! - 1;
	if ((0x000fffff & (2 + hx)) < 3) {
		if (f === 0) {
			return k === 0 ? 0 : k * LN2_HI + k * LN2_LO;
		}
		const R = f * f * (0.5 - 0.3333333333333333 * f);
		return k === 0 ? f - R : k * LN2_HI - (R - k * LN2_LO - f);
	}
	const s = f / (2 + f);
	const z = s * s;
	const w = z * z;
	const t1 = w * (LG2 + w * (LG4 + w * LG6));
	const t2 = z * (LG1 + w * (LG3 + w * (LG5 + w * LG7)));
	const i = (hx - 0x6147a) | (0x6b851 - hx);
	const R = t2 + t1;
	if (i > 0) {
		const hfsq = 0.5 * f * f;
		return k === 0
			? f - (hfsq - s * (hfsq + R))
			: k * LN2_HI - (hfsq - (s * (hfsq + R) + k * LN2_LO) - f);
	}
	return k === 0
		? f - s * (f - R)
		: k * LN2_HI - (s * (f - R) - k * LN2_LO - f);
};
// fdlibm's log2 (FreeBSD's, as V8 has it).
const IVLN2HI = 1.4426950407214463;
const IVLN2LO = 1.6751713164886512e-10;
export const sameLog2 = (x0: number): number => {
	let x = x0;
	f64[0] = x;
	let hx = u32[HI]! | 0;
	const lx = u32[LO]!;
	let k = 0;
	if (hx < 0x00100000) {
		if (((hx & 0x7fffffff) | lx) === 0) {
			return -Infinity;
		}
		if (hx < 0) {
			return Number.NaN;
		}
		k -= 54;
		x *= 18014398509481984;
		hx = highOf(x);
	}
	if (hx >= 0x7ff00000) {
		return x + x;
	}
	if (hx === 0x3ff00000 && lx === 0) {
		return 0;
	}
	k += (hx >> 20) - 1023;
	hx &= 0x000fffff;
	const i = (hx + 0x95f64) & 0x100000;
	x = withHigh(x, hx | (i ^ 0x3ff00000));
	k += i >> 20;
	const y = k;
	const f = x - 1;
	const hfsq = 0.5 * f * f;
	const s = f / (2 + f);
	const z = s * s;
	const w = z * z;
	const t1 = w * (LG2 + w * (LG4 + w * LG6));
	const t2 = z * (LG1 + w * (LG3 + w * (LG5 + w * LG7)));
	const r = s * (hfsq + (t2 + t1));
	const hi = withLow0(f - hfsq);
	const lo = f - hi - hfsq + r;
	let valHi = hi * IVLN2HI;
	let valLo = (lo + hi) * IVLN2LO + lo * IVLN2HI;
	const ww = y + valHi;
	valLo += y - ww + valHi;
	valHi = ww;
	return valLo + valHi;
};

// ---- pow: fdlibm's ------------------------------------------------------------

const BP = [1, 1.5];
const DP_H = [0, 0.5849624872207642];
const DP_L = [0, 1.350039202129749e-8];
const TWO53 = 9007199254740992;
const HUGE = 1e300;
const TINY = 1e-300;
const PL1 = 0.5999999999999946;
const PL2 = 0.4285714285785502;
const PL3 = 0.33333332981837743;
const PL4 = 0.272728123808534;
const PL5 = 0.23066074577556175;
const PL6 = 0.20697501780033842;
const PP1 = 0.16666666666666602;
const PP2 = -0.0027777777777015593;
const PPP3 = 6.613756321437934e-5;
const PPP4 = -1.6533902205465252e-6;
const PP5 = 4.1381367970572385e-8;
const LG2_ = 0.6931471805599453;
const LG2_H = 0.6931471824645996;
const LG2_L = -1.904654299957768e-9;
const OVT = 8.008566259537294e-17;
const CP = 0.9617966939259756;
const CP_H = 0.9617967009544373;
const CP_L = -7.028461650952758e-9;
const IVLN2 = 1.4426950408889634;
const IVLN2_H = 1.4426950216293335;
const IVLN2_L = 1.9259629911266175e-8;
export const samePow = (x: number, y: number): number => {
	f64[0] = x;
	const hx = u32[HI]! | 0;
	const lx = u32[LO]!;
	f64[0] = y;
	const hy = u32[HI]! | 0;
	const ly = u32[LO]!;
	let ix = hx & 0x7fffffff;
	const iy = hy & 0x7fffffff;
	if ((iy | ly) === 0) {
		return 1;
	}
	if (
		ix > 0x7ff00000 ||
		(ix === 0x7ff00000 && lx !== 0) ||
		iy > 0x7ff00000 ||
		(iy === 0x7ff00000 && ly !== 0)
	) {
		return x + y;
	}
	// (Whether y is a whole number - odd, 1; even, 2 - for x below 0.)
	let yisint = 0;
	if (hx < 0) {
		if (iy >= 0x43400000) {
			yisint = 2;
		} else if (iy >= 0x3ff00000) {
			const k = (iy >> 20) - 0x3ff;
			if (k > 20) {
				const j = ly >>> (52 - k);
				if (j << (52 - k) === (ly | 0)) {
					yisint = 2 - (j & 1);
				}
			} else if (ly === 0) {
				const j = iy >> (20 - k);
				if (j << (20 - k) === iy) {
					yisint = 2 - (j & 1);
				}
			}
		}
	}
	if (ly === 0) {
		if (iy === 0x7ff00000) {
			if (((ix - 0x3ff00000) | lx) === 0) {
				return y - y;
			}
			if (ix >= 0x3ff00000) {
				return hy >= 0 ? y : 0;
			}
			return hy < 0 ? -y : 0;
		}
		if (iy === 0x3ff00000) {
			return hy < 0 ? 1 / x : x;
		}
		if (hy === 0x40000000) {
			return x * x;
		}
		if (hy === 0x3fe00000 && hx >= 0) {
			return Math.sqrt(x);
		}
	}
	let ax = Math.abs(x);
	if (lx === 0 && (ix === 0x7ff00000 || ix === 0 || ix === 0x3ff00000)) {
		let z = ax;
		if (hy < 0) {
			z = 1 / z;
		}
		if (hx < 0) {
			if (((ix - 0x3ff00000) | yisint) === 0) {
				z = Number.NaN;
			} else if (yisint === 1) {
				z = -z;
			}
		}
		return z;
	}
	let n = (hx >> 31) + 1;
	if ((n | yisint) === 0) {
		return Number.NaN;
	}
	let s = 1;
	if ((n | (yisint - 1)) === 0) {
		s = -1;
	}
	let t1: number;
	let t2: number;
	if (iy > 0x41e00000) {
		if (iy > 0x43f00000) {
			if (ix <= 0x3fefffff) {
				return hy < 0 ? HUGE * HUGE : TINY * TINY;
			}
			if (ix >= 0x3ff00000) {
				return hy > 0 ? HUGE * HUGE : TINY * TINY;
			}
		}
		if (ix < 0x3fefffff) {
			return hy < 0 ? s * HUGE * HUGE : s * TINY * TINY;
		}
		if (ix > 0x3ff00000) {
			return hy > 0 ? s * HUGE * HUGE : s * TINY * TINY;
		}
		const t = ax - 1;
		const w = t * t * (0.5 - t * (0.3333333333333333 - t * 0.25));
		const u = IVLN2_H * t;
		const v = t * IVLN2_L - w * IVLN2;
		t1 = withLow0(u + v);
		t2 = v - (t1 - u);
	} else {
		n = 0;
		if (ix < 0x00100000) {
			ax *= TWO53;
			n -= 53;
			ix = highOf(ax);
		}
		n += (ix >> 20) - 0x3ff;
		const j = ix & 0x000fffff;
		ix = j | 0x3ff00000;
		let k: number;
		if (j <= 0x3988e) {
			k = 0;
		} else if (j < 0xbb67a) {
			k = 1;
		} else {
			k = 0;
			n += 1;
			ix -= 0x00100000;
		}
		ax = withHigh(ax, ix);
		let u = ax - BP[k]!;
		let v = 1 / (ax + BP[k]!);
		const ss = u * v;
		const sH = withLow0(ss);
		let tH = withHigh(0, ((ix >> 1) | 0x20000000) + 0x00080000 + (k << 18));
		let tL = ax - (tH - BP[k]!);
		const sL = v * (u - sH * tH - sH * tL);
		let s2 = ss * ss;
		let r =
			s2 *
			s2 *
			(PL1 + s2 * (PL2 + s2 * (PL3 + s2 * (PL4 + s2 * (PL5 + s2 * PL6)))));
		r += sL * (sH + ss);
		s2 = sH * sH;
		tH = withLow0(3 + s2 + r);
		tL = r - (tH - 3 - s2);
		u = sH * tH;
		v = sL * tH + tL * ss;
		const pH = withLow0(u + v);
		const pL = v - (pH - u);
		const zH = CP_H * pH;
		const zL = CP_L * pH + pL * CP + DP_L[k]!;
		const t = n;
		t1 = withLow0(zH + zL + DP_H[k]! + t);
		t2 = zL - (t1 - t - DP_H[k]! - zH);
	}
	const y1 = withLow0(y);
	const pL = (y - y1) * t1 + y * t2;
	let pH = y1 * t1;
	let z = pL + pH;
	f64[0] = z;
	let j = u32[HI]! | 0;
	let i = u32[LO]!;
	if (j >= 0x40900000) {
		if (((j - 0x40900000) | i) !== 0) {
			return s * HUGE * HUGE;
		}
		if (pL + OVT > z - pH) {
			return s * HUGE * HUGE;
		}
	} else if ((j & 0x7fffffff) >= 0x4090cc00) {
		if (((j - 0xc090cc00) | i) !== 0) {
			return s * TINY * TINY;
		}
		if (pL <= z - pH) {
			return s * TINY * TINY;
		}
	}
	i = j & 0x7fffffff;
	let k = (i >> 20) - 0x3ff;
	n = 0;
	if (i > 0x3fe00000) {
		n = j + (0x00100000 >> (k + 1));
		k = ((n & 0x7fffffff) >> 20) - 0x3ff;
		const t = withHigh(0, n & ~(0x000fffff >> k));
		n = ((n & 0x000fffff) | 0x00100000) >> (20 - k);
		if (j < 0) {
			n = -n;
		}
		pH -= t;
	}
	const t = withLow0(pL + pH);
	const u = t * LG2_H;
	const v = (pL - (t - pH)) * LG2_ + t * LG2_L;
	z = u + v;
	const w = v - (z - u);
	const tt = z * z;
	const tt1 =
		z - tt * (PP1 + tt * (PP2 + tt * (PPP3 + tt * (PPP4 + tt * PP5))));
	const r = (z * tt1) / (tt1 - 2 - (w + z * w));
	z = 1 - (r - z);
	j = (highOf(z) + (n << 20)) | 0;
	if (j >> 20 <= 0) {
		// (Below the least a number holds in full: rare as anything.)
		return s * z * twoTo(n);
	}
	return s * withHigh(z, j);
};

// ---- hypot: V8's ---------------------------------------------------------------

// V8's: each over the largest, their squares summed (Kahan's way), the
// root of that times the largest.
export const sameHypot = (...xs: number[]): number => {
	let max = 0;
	let nan = false;
	for (const x of xs) {
		const a = Math.abs(x);
		if (Number.isNaN(a)) {
			nan = true;
		} else if (a > max) {
			max = a;
		}
	}
	if (max === Infinity) {
		return Infinity;
	}
	if (nan) {
		return Number.NaN;
	}
	if (max === 0) {
		return 0;
	}
	let sum = 0;
	let comp = 0;
	for (const x of xs) {
		const n = Math.abs(x) / max;
		const summand = n * n - comp;
		const pre = sum + summand;
		comp = pre - sum - summand;
		sum = pre;
	}
	return Math.sqrt(sum) * max;
};

// While `f` runs - a game being staged - the browser's own sums of these
// are these instead: to the bit the same in any browser. (Nothing else runs
// meanwhile: staging is all of a piece, nothing awaited in it.)
const SAME = {
	sin: sameSin,
	cos: sameCos,
	tan: sameTan,
	atan: sameAtan,
	atan2: sameAtan2,
	asin: sameAsin,
	acos: sameAcos,
	exp: sameExp,
	log: sameLog,
	log2: sameLog2,
	pow: samePow,
	hypot: sameHypot,
} as const;
type Name = keyof typeof SAME;
let depth = 0;
const own = new Map<Name, (typeof Math)[Name]>();
export const sameEverywhere = <T>(f: () => T): T => {
	if (depth === 0) {
		for (const name of Object.keys(SAME) as Name[]) {
			own.set(name, Math[name]);
			(Math as any)[name] = SAME[name];
		}
	}
	depth += 1;
	try {
		return f();
	} finally {
		depth -= 1;
		if (depth === 0) {
			for (const [name, g] of own) {
				(Math as any)[name] = g;
			}
		}
	}
};
