import cats.free.Free
import cats.free.Free.liftF
import cats.arrow.FunctionK
import cats.Id

object GetPutFree {
    trait GetPutF[A]
    case class Put(key: String, value: String) extends GetPutF[Unit]
    case class Get(key: String) extends GetPutF[Option[String]]

    type GetPut[A] = Free[GetPutF, A]

    def put(key: String, value: String): GetPut[Unit] = liftF(Put(key, value))
    def get(key: String): GetPut[Option[String]] = liftF(Get(key))

    def double(key: String): GetPut[Option[String]] = for {
        v <- get(key)
        v2 = v.map(vv => vv + vv)
        _ <- v2.map(vv2 => put(key, vv2)).getOrElse(Free.pure())
    } yield v2

    def interpret: FunctionK[GetPutF, Id] = new FunctionK[GetPutF, Id] {
        val m = scala.collection.mutable.Map.empty[String, String]
        m.put("foo", "123")
        def apply[A](fa: GetPutF[A]): cats.Id[A] = {
            fa match {
                case Get(key) => m.get(key)
                case Put(key, value) => {
                    m.put(key, value)
                    ()
                }
            }
        }
    }

    val result: Option[String] = Free.foldMap(interpret).apply(double("foo"))

    def main(args: Array[String]): Unit = {
        println(result.toString)
    }
}