// What vega-interpreter imports from `vega-util`, served from the
// `vega` global the vendored UMD build already defines — so the
// interpreter, an ES module, loads beside the classic scripts with no
// import map and no second copy of vega-util.
const { ascending, isString, DisallowedObjectProperties } = window.vega;
export { ascending, isString, DisallowedObjectProperties };
