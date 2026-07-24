import { useMemo, useState } from "react";
import {
	DndContext,
	DragOverlay,
	KeyboardSensor,
	PointerSensor,
	TouchSensor,
	closestCorners,
	useDroppable,
	useSensor,
	useSensors,
	type DragEndEvent,
	type DragStartEvent,
	type UniqueIdentifier,
} from "@dnd-kit/core";
import {
	SortableContext,
	arrayMove,
	useSortable,
	verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import type { View } from "../../../common/types.ts";
import useTitleBar from "../../hooks/useTitleBar.tsx";
import { toWorker } from "../../util/toWorker.ts";
import { useLocal } from "../../util/local.ts";
import { PlayerNameLabels } from "../../components/PlayerNameLabels.tsx";

type IntrasquadPlayer = View<"intrasquad">["players"][number];

type SquadKey = "primary" | "secondary";
const SQUADS: SquadKey[] = ["primary", "secondary"];

// Snake-draft the roster (already sorted best-first) into two balanced squads:
// picks go Primary, Secondary, Secondary, Primary, ... so neither squad stacks
// all the top players. The user can then drag to rebalance however they like.
const snakeSplit = (players: IntrasquadPlayer[]) => {
	const primary: number[] = [];
	const secondary: number[] = [];
	players.forEach((p, i) => {
		const toPrimary = i % 4 === 0 || i % 4 === 3;
		(toPrimary ? primary : secondary).push(p.pid);
	});
	return { primary, secondary };
};

const squadLabel: Record<SquadKey, string> = {
	primary: "Primary",
	secondary: "Secondary",
};

// One draggable player card, shown in whichever squad column it currently sits.
const PlayerCard = ({
	p,
	season,
	color,
	challengeNoRatings,
	onMove,
}: {
	p: IntrasquadPlayer;
	season: number;
	color: string;
	challengeNoRatings: boolean;
	onMove: () => void;
}) => {
	const {
		attributes,
		listeners,
		setNodeRef,
		transform,
		transition,
		isDragging,
	} = useSortable({ id: p.pid });

	return (
		<div
			ref={setNodeRef}
			className="d-flex align-items-center gap-2 border rounded p-2 mb-2 bg-body"
			style={{
				transform: transform
					? `translate3d(${transform.x}px, ${transform.y}px, 0)`
					: undefined,
				transition,
				opacity: isDragging ? 0.4 : 1,
				borderLeft: `4px solid ${color}`,
				cursor: "grab",
			}}
			{...attributes}
			{...listeners}
		>
			<div className="flex-grow-1 lh-sm">
				<PlayerNameLabels
					pid={p.pid}
					injury={p.injury}
					skills={p.ratings.skills}
					defaultWatch={p.watch}
					firstName={p.firstName}
					lastName={p.lastName}
					season={season}
				/>
				<div className="text-body-secondary small">
					{p.ratings.pos} · {p.age} yo
					{challengeNoRatings ? "" : ` · ${p.ratings.ovr} ovr`}
				</div>
			</div>
			{/* A click-to-move fallback so the squads can be built without dragging
			    (and on touch devices where a drag can be finicky). */}
			<button
				type="button"
				className="btn btn-light-bordered btn-sm"
				title="Move to the other squad"
				onPointerDown={(event) => {
					// Don't let the drag sensor swallow the click.
					event.stopPropagation();
				}}
				onClick={onMove}
			>
				<span className="glyphicon glyphicon-transfer" />
			</button>
		</div>
	);
};

const SquadColumn = ({
	squad,
	pids,
	players,
	season,
	color,
	challengeNoRatings,
	onMovePlayer,
}: {
	squad: SquadKey;
	pids: number[];
	players: Map<number, IntrasquadPlayer>;
	season: number;
	color: string;
	challengeNoRatings: boolean;
	onMovePlayer: (pid: number) => void;
}) => {
	const { setNodeRef } = useDroppable({ id: squad });
	const enough = pids.length >= 5;

	return (
		<div className="col-12 col-md-6 mb-3">
			<div className="d-flex align-items-center justify-content-between mb-2">
				<h3 className="mb-0 d-flex align-items-center gap-2">
					<span
						className="d-inline-block rounded"
						style={{ width: 16, height: 16, background: color }}
					/>
					{squadLabel[squad]}
				</h3>
				<span className={enough ? "text-success" : "text-danger"}>
					{pids.length} player{pids.length === 1 ? "" : "s"}
				</span>
			</div>
			<div
				ref={setNodeRef}
				className="p-2 rounded"
				style={{ minHeight: 80, background: "var(--bs-secondary-bg)" }}
			>
				<SortableContext items={pids} strategy={verticalListSortingStrategy}>
					{pids.length === 0 ? (
						<div className="text-body-secondary text-center py-3">
							Drag players here
						</div>
					) : (
						pids.map((pid) => {
							const p = players.get(pid);
							if (!p) {
								return null;
							}
							return (
								<PlayerCard
									key={pid}
									p={p}
									season={season}
									color={color}
									challengeNoRatings={challengeNoRatings}
									onMove={() => onMovePlayer(pid)}
								/>
							);
						})
					)}
				</SortableContext>
			</div>
		</div>
	);
};

const Intrasquad = ({
	tid,
	season,
	region,
	name,
	colors,
	players,
}: View<"intrasquad">) => {
	useTitleBar({
		title: "Intrasquad Scrimmage",
		titleLong: `Intrasquad Scrimmage » ${region} ${name}`,
	});

	const { challengeNoRatings } = useLocal(["challengeNoRatings"]);

	const playersByPid = useMemo(
		() => new Map(players.map((p) => [p.pid, p])),
		[players],
	);

	const [squads, setSquads] = useState(() => snakeSplit(players));
	const [simming, setSimming] = useState(false);
	const [error, setError] = useState<string | undefined>();

	const sensors = useSensors(
		useSensor(PointerSensor, { activationConstraint: { distance: 5 } }),
		useSensor(TouchSensor, {
			activationConstraint: { delay: 150, tolerance: 8 },
		}),
		useSensor(KeyboardSensor),
	);
	const [activeId, setActiveId] = useState<number | undefined>();

	const containerOf = (id: UniqueIdentifier): SquadKey | undefined => {
		if (id === "primary" || id === "secondary") {
			return id;
		}
		const pid = Number(id);
		if (squads.primary.includes(pid)) {
			return "primary";
		}
		if (squads.secondary.includes(pid)) {
			return "secondary";
		}
		return undefined;
	};

	const onDragStart = (event: DragStartEvent) => {
		setActiveId(Number(event.active.id));
	};

	const onDragEnd = (event: DragEndEvent) => {
		setActiveId(undefined);
		const { active, over } = event;
		if (!over) {
			return;
		}
		const from = containerOf(active.id);
		const to = containerOf(over.id);
		if (!from || !to) {
			return;
		}
		const pid = Number(active.id);

		setSquads((prev) => {
			if (from === to) {
				// Reorder within the same squad.
				const list = prev[from];
				const oldIndex = list.indexOf(pid);
				const overIndex =
					over.id === to ? list.length - 1 : list.indexOf(Number(over.id));
				if (oldIndex === overIndex || overIndex < 0) {
					return prev;
				}
				return { ...prev, [from]: arrayMove(list, oldIndex, overIndex) };
			}

			// Move across squads, inserting where it was dropped.
			const fromList = prev[from].filter((x) => x !== pid);
			const toList = [...prev[to]];
			const overIndex =
				over.id === to ? toList.length : toList.indexOf(Number(over.id));
			toList.splice(overIndex < 0 ? toList.length : overIndex, 0, pid);
			return { ...prev, [from]: fromList, [to]: toList };
		});
	};

	// The click-to-move fallback: send a player to the other squad (appended).
	const movePlayer = (pid: number) => {
		setSquads((prev) => {
			const from: SquadKey = prev.primary.includes(pid)
				? "primary"
				: "secondary";
			const to: SquadKey = from === "primary" ? "secondary" : "primary";
			return {
				...prev,
				[from]: prev[from].filter((x) => x !== pid),
				[to]: [...prev[to], pid],
			};
		});
	};

	const colorFor = (squad: SquadKey) =>
		squad === "primary" ? colors[0] : colors[1];

	const canRun = squads.primary.length >= 5 && squads.secondary.length >= 5;

	const run = async () => {
		setError(undefined);
		setSimming(true);
		try {
			await toWorker("main", "simIntrasquadGame", {
				tid,
				squads: [squads.primary, squads.secondary],
			});
		} catch (error) {
			setSimming(false);
			setError((error as Error).message);
		}
	};

	const activePlayer =
		activeId !== undefined ? playersByPid.get(activeId) : undefined;

	return (
		<>
			<p>
				Split the {region} {name} into two squads and run a scrimmage. Drag
				players between the squads (or use the{" "}
				<span className="glyphicon glyphicon-transfer" /> button); each squad
				needs at least five.
			</p>

			<div className="d-flex flex-wrap gap-2 mb-3">
				<button
					type="button"
					className="btn btn-primary"
					disabled={!canRun || simming}
					onClick={run}
				>
					{simming ? "Simming…" : "Start scrimmage"}
				</button>
				<button
					type="button"
					className="btn btn-light-bordered"
					disabled={simming}
					onClick={() => setSquads(snakeSplit(players))}
				>
					Auto-balance
				</button>
			</div>

			{!canRun ? (
				<p className="text-danger">
					Each squad needs at least five players to run the scrimmage.
				</p>
			) : null}
			{error ? <p className="text-danger">{error}</p> : null}

			<DndContext
				sensors={sensors}
				collisionDetection={closestCorners}
				onDragStart={onDragStart}
				onDragEnd={onDragEnd}
				onDragCancel={() => setActiveId(undefined)}
			>
				<div className="row">
					{SQUADS.map((squad) => (
						<SquadColumn
							key={squad}
							squad={squad}
							pids={squads[squad]}
							players={playersByPid}
							season={season}
							color={colorFor(squad)}
							challengeNoRatings={challengeNoRatings}
							onMovePlayer={movePlayer}
						/>
					))}
				</div>
				<DragOverlay>
					{activePlayer ? (
						<div
							className="border rounded p-2 bg-body"
							style={{
								borderLeft: `4px solid ${
									squads.primary.includes(activePlayer.pid)
										? colors[0]
										: colors[1]
								}`,
							}}
						>
							{activePlayer.firstName} {activePlayer.lastName}
						</div>
					) : null}
				</DragOverlay>
			</DndContext>
		</>
	);
};

export default Intrasquad;
