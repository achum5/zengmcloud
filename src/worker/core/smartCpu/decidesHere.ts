import { advancesHere } from "../sync/advanceLease.ts";

// Whether the smart CPU makes CPU teams' moves on this device: single player,
// the device in charge of simming, or a device running a step of the league's
// advance it claimed (a ready-up step). A league mate's own last draft pick,
// or own game that ends a day, runs on their phone and is no advance: the
// CPU's trades and signings then wait for the device that decides
// (docs/smart-cpu.md, shared leagues).
export const smartDecidesHere = advancesHere;
