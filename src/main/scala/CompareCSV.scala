import cats.free.Free
import cats.Monad

object CompareCSV {

    trait FieldType

    case object StringType extends FieldType
    case object IntType extends FieldType
    case object FloatType extends FieldType
    case object BooleanType extends FieldType
    case object DateTimeType extends FieldType

    case class Field(name: String, fieldType: FieldType)

    case class Schema(fields: List[Field])

    trait Path

    trait Row

    trait Table

    trait DFrame[A]

    trait CompareF[A]

    case class Read(path: Path, fields: Schema) extends CompareF[DFrame[Row]]
    case class Join(df1: DFrame[Row], df2: DFrame[Row], key: Schema) extends CompareF[DFrame[Row]]

    type Compare[A] = Free[CompareF, A]

    val f1 = Field("id", IntType)
    val schema = new Schema(List(f1, Field("name", StringType), Field("payed", BooleanType)))

}

object Store {

    sealed trait KVStoreA[A]
    case class Put[T](key: String, value: T) extends KVStoreA[Unit]
    case class Get[T](key: String) extends KVStoreA[Option[T]]
    case class Delete(key: String) extends KVStoreA[Unit]

    import cats.free.Free

    type KVStore[A] = Free[KVStoreA, A]

    import cats.free.Free.liftF

    // Put returns nothing (i.e. Unit).
    def put[T](key: String, value: T): KVStore[Unit] =
      liftF[KVStoreA, Unit](Put[T](key, value))

    // Get returns a T value.
    def get[T](key: String): KVStore[Option[T]] =
      liftF[KVStoreA, Option[T]](Get[T](key))

    // Delete returns nothing (i.e. Unit).
    def delete(key: String): KVStore[Unit] =
      liftF(Delete(key))

    // Update composes get and set, and returns nothing.
    def update[T](key: String, f: T => T): KVStore[Unit] =
      for {
        vMaybe <- get[T](key)
        _ <- vMaybe.map(v => put[T](key, f(v))).getOrElse(Free.pure(()))
      } yield ()

    def program: KVStore[Option[Int]] =
        for {
            _ <- put("wild-cats", 2)
            _ <- update[Int]("wild-cats", (_ + 12))
            _ <- put("tame-cats", 5)
            n <- get[Int]("wild-cats")
            _ <- delete("tame-cats")
        } yield n


    import cats.arrow.FunctionK
    import cats.{Id, ~>}
    import scala.collection.mutable

    // the program will crash if a key is not found,
    // or if a type is incorrectly specified.
    def impureCompiler: FunctionK[KVStoreA, Id]  =

        new FunctionK[KVStoreA, Id] {

            // a very simple (and imprecise) key-value store
            val kvs = mutable.Map.empty[String, Any]

            def apply[A](fa: KVStoreA[A]): A =
                fa match {
                    case Put(key, value) =>
                        println(s"put($key, $value)")
                        kvs(key) = value
                        ()
                    case Get(key) =>
                        println(s"get($key)")
                        kvs.get(key).map(_.asInstanceOf[A])
                    case Delete(key) =>
                        println(s"delete($key)")
                        kvs.remove(key)
                        ()
                }
    }

    def program2: KVStore[Option[Int]] = put("wild-cats", 2).flatMap(_ => update[Int]("wild-cats", (x => x + 12))
    .flatMap(_ => put("tame-cats", 5).flatMap(_ => get[Int]("wild-cats").flatMap(n => delete("tame-cats").flatMap(_ => Free.pure(n))))))

    trait KVStore2 {
        def put[T](key: String, value: T): Unit
        def get[T](key: String): Option[T]
        def delete(key: String): Unit
        def update[T](key: String, f: T => T): Unit = {
            get(key).map(v => put(key, f(v)))
        }
    }


    def program3(store: KVStore2): Option[Int] = {
        store.put("wc", 2)
        store.update("wc", ((x: Int) => x + 12))
        store.put("tc", 5)
        val n = store.get("wc")
        store.delete("tc")
        return n
    }

    def p4(store: KVStore2): Option[Int] = {
        for {
            x <- program3(store)
            y <- program3(store)
        } yield x + y
    }

    def crossProduct(xs: List[Int], ys: List[Int]): List[(Int, Int)] = {
        for {
            x <- xs
            y <- ys if x < y
        } yield (x, y)
    }
}
