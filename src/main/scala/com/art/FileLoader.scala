package com.art

object FileLoader {

  def pathToFile = {
    val resource = this.getClass.getClassLoader.getResource("latin.txt")
    if (resource == null) {
      sys.error("No file in resources")
    }
    resource.getPath
  }

}
