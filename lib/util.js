/**
 * Reduce two sets of consumed capacity metrics into a single object
 *
 * @param {object} existing capacity. This object will be updated.
 * @param {object} new capacity object to be added to the existing object.
 */
module.exports.reduceCapacity = function(existing, incoming) {
  existing = existing || {};
  existing.TableName = existing.TableName || incoming.TableName;

  if (incoming.CapacityUnits) {
    existing.CapacityUnits = (existing.CapacityUnits || 0) + incoming.CapacityUnits;
  }

  if (incoming.Table) {
    existing.Table = existing.Table ?
      { CapacityUnits: existing.Table.CapacityUnits + incoming.Table.CapacityUnits } :
      { CapacityUnits: incoming.Table.CapacityUnits };
  }

  if (incoming.LocalSecondaryIndexes) {
    existing.LocalSecondaryIndexes = existing.LocalSecondaryIndexes ?
      { CapacityUnits: existing.LocalSecondaryIndexes.CapacityUnits + incoming.LocalSecondaryIndexes.CapacityUnits } :
      { CapacityUnits: incoming.LocalSecondaryIndexes.CapacityUnits };
  }

  if (incoming.GlobalSecondaryIndexes) {
    existing.GlobalSecondaryIndexes = existing.GlobalSecondaryIndexes ?
      { CapacityUnits: existing.GlobalSecondaryIndexes.CapacityUnits + incoming.GlobalSecondaryIndexes.CapacityUnits } :
      { CapacityUnits: incoming.GlobalSecondaryIndexes.CapacityUnits };
  }
};
