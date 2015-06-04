## upcoming

- Add support for [document types (List and Map)][1], [Boolean][2], and [Null][3]
- Native JavaScript arrays will now transform into lists instead of sets
- Add `dyno.createSet()`, which constructs sets (of number, string, or binary
  type) that transform to the DynamoDB wire format correctly.
- Drop support for using a wire-formatted object as input.

[1]:http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModel.DataTypes.Document
[2]:http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModel.DataTypes.Boolean
[3]:http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModel.DataTypes.Null
