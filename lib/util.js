/* eslint-env es6 */
/**
 * Reduce two sets of consumed capacity metrics into a single object
 *
 * @param {object} existing capacity. This object will be updated.
 * @param {object | array} new capacity object(s) to be added to the existing object.
 */
function reduceCapacity(existing, incoming) {
  if (!existing) {
    return;
  }

  if (Array.isArray(incoming)) {
    for (const item of incoming) {
      reduceCapacity(existing, item);
    }
    return;
  }

  function mergeCapacityUnits(dst, src) {
    if (!src.CapacityUnits) {
      return;
    }
    dst.CapacityUnits = (dst.CapacityUnits || 0) + src.CapacityUnits;
  }

  function mergeCapacityParents(dst, src, k) {
    if (!src[k]) {
      return;
    }
    dst[k] = dst[k] || {};
    mergeCapacityUnits(dst[k], src[k]);
  }

  existing.TableName = existing.TableName || incoming.TableName;

  mergeCapacityUnits(existing, incoming);
  mergeCapacityParents(existing, incoming, 'Table');
  mergeCapacityParents(existing, incoming, 'LocalSecondaryIndexes');
  mergeCapacityParents(existing, incoming, 'GlobalSecondaryIndexes');

  for (const indexGroup of [
    'LocalSecondaryIndexes',
    'GlobalSecondaryIndexes',
  ]) {
    const dst = existing[indexGroup];
    const src = incoming[indexGroup];
    for (const index of Object.keys(src || {})) {
      mergeCapacityParents(dst, src, index);
    }
  }
}

module.exports.reduceCapacity = reduceCapacity;
