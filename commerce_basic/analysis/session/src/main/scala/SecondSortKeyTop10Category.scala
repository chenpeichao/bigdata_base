case class SecondSortKeyTop10Category(var categoryId: Long, var clickCount: Long, var orderCount: Long, var payCount: Long) extends Comparable[SecondSortKeyTop10Category] {
  override def compareTo(that: SecondSortKeyTop10Category): Int = {
    var compare = 0;
    if (!this.clickCount.equals(that.clickCount)) {
      compare = this.clickCount.compare(that.clickCount)
    } else if (!this.orderCount.equals(that.orderCount)) {
      compare = this.orderCount.compare(that.orderCount)
    } else if (!this.payCount.equals(that.payCount)) {
      compare = this.payCount.compare(that.payCount)
    } else {
      compare = this.categoryId.compare(that.categoryId)
    }
    compare
  }
}
