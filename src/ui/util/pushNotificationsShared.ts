// Lightweight localStorage keys for push notifications, split out so the
// bootstrap keep-fresh hook can read the "enabled" flag WITHOUT importing
// pushNotifications.ts (which statically pulls in the large firebase/messaging
// dependency). That dep must only load for devices that actually turned push on.
export const PUSH_ENABLED_KEY = "pushNotificationsEnabled";
export const PUSH_NAME_KEY = "pushNotificationsName";
