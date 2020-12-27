package an004005.join

sealed trait JoinType

final case class BroadcastJoinType() extends JoinType

final case class SortMergeJoinType() extends JoinType

final case class AQEJoinType() extends JoinType

final case class PartialBroadcastJoinType() extends JoinType
