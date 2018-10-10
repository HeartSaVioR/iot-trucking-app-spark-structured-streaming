package com.hortonworks.spark.benchmark.streaming.sessionwindow

object TestSentences {

  val SENTENCES: Array[String] =
    """Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla posuere neque eu lacus porta ultrices. Vivamus a ipsum lacus. In eu dolor eu nunc suscipit tristique eu sed felis. Suspendisse a lectus orci. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Aliquam erat dolor, pretium ac porta ac, rhoncus at diam. Etiam ut luctus felis.
      |Aenean porttitor mollis odio, in malesuada mi suscipit commodo. Vestibulum posuere, neque et mattis laoreet, diam justo euismod quam, non aliquam tortor elit eget diam. Proin ipsum tortor, tincidunt vel augue at, egestas aliquet nibh. Suspendisse fringilla cursus volutpat. Duis eget scelerisque nisi. Aliquam vitae metus nec erat pellentesque tincidunt luctus et orci. Nam vel nisl vulputate, efficitur tellus ut, commodo ante.
      |Morbi venenatis velit non metus finibus lobortis. Vestibulum dapibus lectus ac odio tristique, at auctor arcu congue. Vestibulum a odio vitae lectus mollis accumsan. Donec blandit turpis purus, sit amet porttitor tortor ultrices quis. Duis varius egestas justo eu imperdiet. Maecenas euismod augue et velit ornare ullamcorper. Proin lacinia a enim a elementum. Praesent volutpat egestas turpis, ac consequat nunc luctus vel. Curabitur ullamcorper viverra enim at egestas. Pellentesque dictum interdum arcu a euismod.
      |Interdum et malesuada fames ac ante ipsum primis in faucibus. Fusce lobortis enim sem, vitae semper neque posuere eu. Duis tempus luctus ante id porta. Ut varius lectus ipsum, a facilisis libero ultrices eget. Fusce ut nisi tortor. Cras at mi sed magna ultrices lacinia. Vestibulum porttitor, enim quis accumsan aliquam, diam sapien interdum massa, id aliquet mi risus ac neque. Suspendisse odio quam, posuere pellentesque urna sed, vehicula efficitur ex. Interdum et malesuada fames ac ante ipsum primis in faucibus.
      |Ut elementum sit amet dui sed pharetra. Aliquam libero eros, imperdiet ut dictum at, interdum et metus. Vivamus sit amet dolor sit amet lacus elementum vehicula ut non tellus. Aliquam erat volutpat. Morbi scelerisque bibendum commodo. Nullam rhoncus risus velit, at dictum nulla eleifend id. Phasellus ultrices, enim sit amet vestibulum congue, magna orci venenatis ex, quis tristique risus elit sit amet erat. Fusce sollicitudin felis accumsan, tincidunt nisi a, tempor felis. In hac habitasse platea dictumst. Vivamus vehicula nibh eu finibus finibus. Maecenas nec pharetra nisi, ut bibendum sapien.
      |Vestibulum sed metus metus. Donec non blandit sapien. Ut tempus dui nec metus sodales lobortis nec id sem. Morbi sodales mi sed nisl auctor luctus. Mauris bibendum finibus felis at bibendum. Curabitur euismod justo sed lorem dictum, sed ullamcorper lacus aliquam. Nunc sagittis sem ut rhoncus aliquet.
      |Curabitur suscipit blandit eros in lobortis. Donec sit amet pulvinar erat. Donec scelerisque nisl lorem, quis imperdiet ipsum laoreet ac. Sed ut velit pharetra ex convallis lobortis eget eget augue. Donec felis metus, elementum ut sagittis sit amet, imperdiet sit amet ipsum. Sed porttitor ex id faucibus faucibus. Aliquam vitae diam ut nisl pharetra dignissim. Curabitur aliquet, arcu ac luctus aliquet, leo justo consequat risus, at feugiat felis dolor ut mauris. Integer vel nulla libero. Nunc nec elit quis tortor pulvinar efficitur eget in quam. Sed venenatis sodales finibus. Integer ultrices, sem at pretium facilisis, lacus neque sodales elit, ut commodo elit nulla congue turpis. Vestibulum a venenatis massa.
      |Mauris vel facilisis erat, in imperdiet urna. Pellentesque at sem vel magna tristique consequat vitae sed justo. In aliquet nisl malesuada lectus porttitor feugiat. Phasellus fringilla hendrerit mollis. Duis eu purus id odio scelerisque mollis non quis libero. Nulla placerat nisl sed felis suscipit, nec placerat diam pulvinar. Nullam ac ultrices metus. Nam et consectetur purus, sed pulvinar dui. Vestibulum a massa sit amet elit fringilla sollicitudin et sagittis tellus. Aenean vitae faucibus neque.
      |Morbi elit elit, dignissim ac consectetur ac, elementum aliquam eros. Maecenas magna lacus, lobortis sit amet eros ac, dapibus egestas libero. In sit amet mollis justo. Phasellus imperdiet pulvinar libero. Maecenas rutrum risus quis tellus rhoncus, ac luctus lectus semper. Praesent consequat congue lacinia. Cras auctor blandit est, sit amet maximus risus faucibus molestie.
      |Suspendisse ut nibh nec neque accumsan malesuada id quis lorem. Proin aliquet ultricies malesuada. Donec purus ex, faucibus hendrerit tristique vitae, semper sed nulla. Fusce in condimentum diam. Aliquam elementum ligula arcu, bibendum sagittis lectus porttitor eget. Nunc et orci eu velit imperdiet luctus varius eget magna. In commodo felis odio, eget imperdiet nisi iaculis id. Fusce elit nisi, vehicula ut neque eu, consequat aliquam nunc. Fusce quis vestibulum enim. Nullam at risus non quam imperdiet sollicitudin at sed lorem. Aliquam non felis quis leo faucibus lobortis sit amet consequat felis. Pellentesque rutrum sed leo id pellentesque. Quisque eget hendrerit dui, sit amet ullamcorper lectus. Proin ut eleifend nunc. Duis mollis, eros eget maximus cursus, est mi accumsan lacus, id fringilla est nisi vitae sapien. Nulla non ex erat.
    """.stripMargin.split("\\n")


  def createCaseExprStr(valExpr: String, count: Int): String = {
    require(SENTENCES.length >= count)

    var caseExpr = s"CASE $valExpr "
    SENTENCES
      .take(count - 1)
      .zipWithIndex.foreach {
      case (word, idx) => caseExpr += s"WHEN $idx THEN '$word' "
    }

    caseExpr += SENTENCES
      .drop(count - 1)
      .take(1)
      .map(word => s"ELSE '$word' END ")
      .last

    caseExpr
  }
}