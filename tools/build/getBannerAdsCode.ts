// Ads are disabled entirely in this build. This used to return the InMobi
// consent-manager, Freestar loader, and Ad-Shield scripts that get injected into
// <head> (via the BANNER_ADS_CODE placeholder in index.html). Returning an empty
// string means none of those third-party ad/consent scripts are ever loaded.
export const getBannerAdsCode = () => "";
