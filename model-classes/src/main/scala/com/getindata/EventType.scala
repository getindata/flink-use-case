package com.getindata

object EventType extends Enumeration {
  type EventType = Value
  val EndSong = Value("EndSong")
  val NextSong = Value("NextSong")
  val SearchSong = Value("SearchSong")
}
