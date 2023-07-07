import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

case class TvShows(rowID: Int,id: Int,title: String,year:Int,age:String,IMDb:String,RottenTomatoes:String,isNetflix:Int,isHulu:Int,isPrimeVideo:Int)
object TvShows {
  implicit val decoder: Decoder[TvShows] = deriveDecoder[TvShows]
}