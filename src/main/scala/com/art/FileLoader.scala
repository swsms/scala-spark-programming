package com.art

object FileLoader {

  def pathToFile(fileName: String) = {
    val resource = this.getClass.getClassLoader.getResource(fileName)
    if (resource == null) {
      sys.error("No file in resources")
    }
    resource.getPath
  }
}
