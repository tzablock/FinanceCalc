case class A(a: Int)
object A {
  def apply(a: String): A = {
    print(a)
    A(2)
  }
}

A("sss")