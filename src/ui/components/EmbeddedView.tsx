import { createContext } from "react";

// A PAGE'S VIEW DRAWN INSIDE ANOTHER PAGE (the refreshed look's team page:
// its schedule, finances, picks, transactions as sections): it sets no title
// bar and draws no team header of its own - the page it is in has those.
export const EmbeddedView = createContext(false);
