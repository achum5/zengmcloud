import clsx from "clsx";
import { useEffect, useRef, useState, type PointerEvent } from "react";
import useTitleBar from "../hooks/useTitleBar.tsx";
import { helpers } from "../util/helpers.ts";
import { toWorker } from "../util/toWorker.ts";
import { MoreLinks } from "../components/MoreLinks.tsx";
import { InjuryIcon } from "../components/InjuryIcon.tsx";
import type { View } from "../../common/types.ts";
import {
	gridToRotation,
	playersPerMinute,
	rotationToGrid,
	type RotationGrid,
	type RotationStint,
} from "../../common/rotation.ts";

// THE ROTATION TIMELINE.
//
// One row per player, one cell per minute of each period, painted where he is
// planned to be on the floor. Tap a cell to flip it; drag across cells to
// paint a stint. The count under each minute says how many are planned for
// it, and turns red when it is not the number the game puts on the floor,
// because that is the one thing a plan can get wrong that the sim cannot
// quietly fix the way the planner meant.
//
// The plan is a guide. The sim follows it at every dead ball it can, and
// hands the floor back to the coach in a blowout, for foul trouble, and in a
// close finish.

type Cell = { pid: number; period: number; minute: number };

const cellFromElement = (element: Element | null): Cell | undefined => {
	const el = element?.closest("[data-pid]");
	if (!(el instanceof HTMLElement)) {
		return undefined;
	}
	return {
		pid: Number(el.dataset.pid),
		period: Number(el.dataset.period),
		minute: Number(el.dataset.minute),
	};
};

const cloneGrid = (grid: RotationGrid): RotationGrid =>
	new Map(
		[...grid].map(([pid, periods]) => [
			pid,
			periods.map((minutes) => [...minutes]),
		]),
	);

const Rotation = ({
	abbrev,
	auto,
	editable,
	enabled,
	generated,
	numPeriods,
	numPlayersOnCourt,
	periodLength,
	players,
	stints,
	tid,
}: View<"rotation">) => {
	useTitleBar({
		title: "Rotation",
		dropdownView: "rotation",
		dropdownFields: { teams: abbrev },
		moreInfoAbbrev: abbrev,
		moreInfoTid: tid,
	});

	const pids = players.map((p) => p.pid);
	const [grid, setGrid] = useState<RotationGrid>(() =>
		rotationToGrid(stints, pids, numPeriods, periodLength),
	);

	// What the worker hands over is the truth; local state only exists so a
	// drag paints without a round trip per cell.
	const stintsKey = JSON.stringify(stints);
	useEffect(() => {
		setGrid(rotationToGrid(stints, pids, numPeriods, periodLength));
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [tid, stintsKey, numPeriods, periodLength]);

	const canEdit = editable && !auto;

	const save = async (next: RotationGrid, nextAuto: boolean) => {
		const rotation = {
			auto: nextAuto,
			stints: gridToRotation(next, periodLength),
		};
		await toWorker("main", "updateRotation", { tid, rotation });
	};

	const saveStints = async (nextStints: RotationStint[]) => {
		const next = rotationToGrid(nextStints, pids, numPeriods, periodLength);
		setGrid(next);
		await save(next, auto);
	};

	// Painting: the first cell touched decides whether the drag turns cells on
	// or off, and every cell crossed after that gets the same.
	const painting = useRef<{ on: boolean; grid: RotationGrid } | undefined>(
		undefined,
	);

	const paint = (cell: Cell | undefined) => {
		const state = painting.current;
		if (!state || !cell) {
			return;
		}
		const row = state.grid.get(cell.pid);
		if (!row || row[cell.period]![cell.minute] === state.on) {
			return;
		}
		row[cell.period]![cell.minute] = state.on;
		setGrid(cloneGrid(state.grid));
	};

	const onPointerDown = (event: PointerEvent<HTMLTableElement>) => {
		if (!canEdit) {
			return;
		}
		const cell = cellFromElement(event.target as Element);
		if (!cell) {
			return;
		}
		const current = grid.get(cell.pid)?.[cell.period]?.[cell.minute] ?? false;
		painting.current = { on: !current, grid: cloneGrid(grid) };
		paint(cell);
	};

	const onPointerMove = (event: PointerEvent<HTMLTableElement>) => {
		if (!painting.current) {
			return;
		}
		paint(
			cellFromElement(document.elementFromPoint(event.clientX, event.clientY)),
		);
	};

	const onPointerEnd = async () => {
		const state = painting.current;
		painting.current = undefined;
		if (state) {
			await save(state.grid, auto);
		}
	};

	if (!enabled) {
		return (
			<>
				<MoreLinks type="team" page="rotation" abbrev={abbrev} tid={tid} />
				<p>
					Rotation plans are off for this league. Turn them on in{" "}
					<a href={helpers.leagueUrl(["settings"])}>League Settings</a>.
				</p>
			</>
		);
	}

	const counts = playersPerMinute(grid, numPeriods, periodLength);
	const gameMinutes = numPeriods * periodLength;
	const heavy = gameMinutes - 4;

	return (
		<>
			<MoreLinks type="team" page="rotation" abbrev={abbrev} tid={tid} />

			{editable ? (
				<div className="d-flex flex-wrap align-items-center gap-3 mb-3">
					<div className="form-check mb-0">
						<input
							className="form-check-input"
							type="checkbox"
							checked={auto}
							id="rotation-auto"
							onChange={async () => {
								await save(grid, !auto);
							}}
						/>
						<label className="form-check-label" htmlFor="rotation-auto">
							Let the coach handle the rotation
						</label>
					</div>
					{!auto ? (
						<div className="btn-group">
							<button
								className="btn btn-light-bordered btn-sm"
								onClick={() => saveStints(generated)}
							>
								Reset to coach's rotation
							</button>
							<button
								className="btn btn-light-bordered btn-sm"
								onClick={() => saveStints([])}
							>
								Clear
							</button>
						</div>
					) : null}
				</div>
			) : null}

			<div className="table-responsive">
				<table
					className={clsx("table table-sm table-borderless rotation-grid", {
						"rotation-grid-editable": canEdit,
						"opacity-75": auto,
					})}
					onPointerDown={onPointerDown}
					onPointerMove={onPointerMove}
					onPointerUp={onPointerEnd}
					onPointerCancel={onPointerEnd}
					onPointerLeave={onPointerEnd}
				>
					<thead>
						<tr>
							<th className="rotation-name" />
							{Array.from({ length: numPeriods }, (_, period) => (
								<th
									key={period}
									colSpan={periodLength}
									className="text-center border-start"
								>
									{helpers.ordinal(period + 1)}
								</th>
							))}
							<th className="rotation-total text-end">MIN</th>
						</tr>
						<tr>
							<th className="rotation-name" />
							{Array.from({ length: numPeriods }, (_, period) =>
								Array.from({ length: periodLength }, (_, minute) => (
									<th
										key={`${period}-${minute}`}
										className={clsx(
											"rotation-minute text-body-secondary fw-normal",
											{ "border-start": minute === 0 },
										)}
									>
										{minute + 1}
									</th>
								)),
							)}
							<th className="rotation-total" />
						</tr>
					</thead>
					<tbody>
						{players.map((p) => {
							const row = grid.get(p.pid);
							let total = 0;
							for (const minutes of row ?? []) {
								for (const on of minutes) {
									if (on) {
										total += 1;
									}
								}
							}
							return (
								<tr key={p.pid}>
									<td className="rotation-name text-nowrap">
										<a href={helpers.leagueUrl(["player", p.pid])}>
											{p.firstNameShort} {p.lastName}
										</a>
										<InjuryIcon injury={p.injury} />
										<span className="text-body-secondary ms-1">
											{p.ratings.pos} {p.ratings.ovr}
										</span>
									</td>
									{Array.from({ length: numPeriods }, (_, period) =>
										Array.from({ length: periodLength }, (_, minute) => {
											const on = row?.[period]?.[minute] ?? false;
											return (
												<td
													key={`${period}-${minute}`}
													className={clsx("rotation-cell", {
														"rotation-on": on,
														"border-start": minute === 0,
													})}
													data-pid={p.pid}
													data-period={period}
													data-minute={minute}
												/>
											);
										}),
									)}
									<td
										className={clsx("rotation-total text-end", {
											"text-danger fw-bold": total > heavy,
										})}
									>
										{total}
									</td>
								</tr>
							);
						})}
					</tbody>
					<tfoot>
						<tr>
							<th className="rotation-name text-body-secondary fw-normal">
								On floor
							</th>
							{counts.map((period, i) =>
								period.map((count, minute) => (
									<td
										key={`${i}-${minute}`}
										className={clsx("rotation-minute text-center", {
											"border-start": minute === 0,
											"text-danger fw-bold": count !== numPlayersOnCourt,
											"text-body-secondary": count === numPlayersOnCourt,
										})}
									>
										{count}
									</td>
								)),
							)}
							<td className="rotation-total" />
						</tr>
					</tfoot>
				</table>
			</div>
		</>
	);
};

export default Rotation;
