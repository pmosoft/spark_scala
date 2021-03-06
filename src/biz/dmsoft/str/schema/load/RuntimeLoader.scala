package biz.dmsoft.str.schema.load

import org.apache.spark.sql.types.StructType
import scala.reflect.runtime.universe

object RuntimeLoader {

  val aa = "";

  def main(args: Array[String])   {
    var objNm = "TSTRTRN001"
  }

  def execute(objNm: String): StructType = {
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader);
    val module = runtimeMirror.staticModule(s"biz.dmsoft.str.schema.${objNm}")
    val im = runtimeMirror.reflectModule(module)
    val method = im.symbol.info.decl(universe.TermName("schema")).asMethod
    val objMirror = runtimeMirror.reflect(im.instance)
    val schema = objMirror.reflectMethod(method)().asInstanceOf[StructType]
    return schema
  }

}