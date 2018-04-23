package innopolis

import org.scalatra._
import scala.collection.mutable.HashMap
import org.json4s._
import org.scalatra.json._

case class Message(id: String, text: String)

class WebServer extends ScalatraServlet with MethodOverride{

    val messages = HashMap.empty[String,Message]
    
    before() {
        //contentType = formats("json")
    }
    
    get("/") {
        "Hello, world!"
    }
  
    notFound {
        "Sorry"
    }
    
    protected implicit val jsonFormats: Formats = DefaultFormats
}
