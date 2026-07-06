// Firebase web config for the shared-league sync + push-notification backend.
//
// These values are NOT secret - Firebase web config is designed to ship in
// client code. Access is controlled by Firestore security rules, not by hiding
// this object. Lives in common/ because BOTH threads need it: the worker (for
// Firestore sync) and the UI thread (for Firebase Cloud Messaging, which can
// only run in a window context).
export const firebaseConfig = {
	apiKey: "AIzaSyCUvEh1yMuJ1aq-LfZHVI_ty7MOb64CXuE",
	authDomain: "zengmcloud-4a454.firebaseapp.com",
	projectId: "zengmcloud-4a454",
	storageBucket: "zengmcloud-4a454.firebasestorage.app",
	messagingSenderId: "446695548992",
	appId: "1:446695548992:web:3be29f048e582ebd6457a5",
};

// Web Push public VAPID key, from:
//   Firebase console -> Project settings -> Cloud Messaging -> Web configuration
//   -> "Web Push certificates" -> Key pair.
// Paste the "Key pair" string here. Until it's set, the "Enable phone
// notifications" button explains that push isn't configured yet - everything
// else (league sync) works without it. This key is public and safe to commit.
export const vapidKey: string =
	"BDP84L_2qs_IMuq7IHkJIRAUM_Z4yx_HD-HmHaMcI0YzkYyJgF5wmDK2VJ6266v5bdjBj94Hf52dJKe7CbZkIoU";
