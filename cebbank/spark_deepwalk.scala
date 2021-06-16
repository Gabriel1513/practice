

def processItemSequence(sparkSession: SparkSession): RDD[Seq[String]] ={
  //设定rating数据的路径并用spark载入数据
  val ratingsResourcesPath = this.getClass.getResource("/webroot/sampledata/ratings.csv")
  val ratingSamples = sparkSession.read.format("csv").option("header", "true").load(ratingsResourcesPath.getPath)


  //实现一个用户定义的操作函数(UDF)，用于之后的排序
  val sortUdf: UserDefinedFunction = udf((rows: Seq[Row]) => {
    rows.map { case Row(movieId: String, timestamp: String) => (movieId, timestamp) }
      .sortBy { case (movieId, timestamp) => timestamp }
      .map { case (movieId, timestamp) => movieId }
  })


  //把原始的rating数据处理成序列数据
  val userSeq = ratingSamples
    .where(col("rating") >= 3.5)  //过滤掉评分在3.5一下的评分记录
    .groupBy("userId")            //按照用户id分组
    .agg(sortUdf(collect_list(struct("movieId", "timestamp"))) as "movieIds")     //每个用户生成一个序列并用刚才定义好的udf函数按照timestamp排序
    .withColumn("movieIdStr", array_join(col("movieIds"), " "))
                //把所有id连接成一个String，方便后续word2vec模型处理


  //把序列数据筛选出来，丢掉其他过程数据
  userSeq.select("movieIdStr").rdd.map(r => r.getAs[String]("movieIdStr").split(" ").toSeq)


def trainItem2vec(samples : RDD[Seq[String]]): Unit ={
    //设置模型参数
    val word2vec = new Word2Vec()
    .setVectorSize(10)
    .setWindowSize(5)
    .setNumIterations(10)


  //训练模型
  val model = word2vec.fit(samples)


  //训练结束，用模型查找与item"592"最相似的20个item
  val synonyms = model.findSynonyms("592", 20)
  for((synonym, cosineSimilarity) <- synonyms) {
    println(s"$synonym $cosineSimilarity")
  }

  //保存模型
  val embFolderPath = this.getClass.getResource("/webroot/sampledata/")
  val file = new File(embFolderPath.getPath + "embedding.txt")
  val bw = new BufferedWriter(new FileWriter(file))
  var id = 0
  //用model.getVectors获取所有Embedding向量
  for (movieId <- model.getVectors.keys){
    id+=1
    bw.write( movieId + ":" + model.getVectors(movieId).mkString(" ") + "\n")
  }
  bw.close()


  //samples 输入的观影序列样本集
  def graphEmb(samples : RDD[Seq[String]], sparkSession: SparkSession): Unit ={
    //通过flatMap操作把观影序列打碎成一个个影片对
    val pairSamples = samples.flatMap[String]( sample => {
      var pairSeq = Seq[String]()
      var previousItem:String = null
      sample.foreach((element:String) => {
        if(previousItem != null){
          pairSeq = pairSeq :+ (previousItem + ":" + element)
        }
        previousItem = element
      })
      pairSeq
    })
    //统计影片对的数量
    val pairCount = pairSamples.countByValue()
    //转移概率矩阵的双层Map数据结构
    val transferMatrix = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Long]]()
    val itemCount = scala.collection.mutable.Map[String, Long]()


    //求取转移概率矩阵
    pairCount.foreach( pair => {
      val pairItems = pair._1.split(":")
      val count = pair._2
      lognumber = lognumber + 1
      println(lognumber, pair._1)


      if (pairItems.length == 2){
        val item1 = pairItems.apply(0)
        val item2 = pairItems.apply(1)
        if(!transferMatrix.contains(pairItems.apply(0))){
          transferMatrix(item1) = scala.collection.mutable.Map[String, Long]()
        }


        transferMatrix(item1)(item2) = count
        itemCount(item1) = itemCount.getOrElse[Long](item1, 0) + count
      }


      //随机游走采样函数
      //transferMatrix 转移概率矩阵
      //itemCount 物品出现次数的分布
      def randomWalk(transferMatrix : scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Long]], itemCount : scala.collection.mutable.Map[String, Long]): Seq[Seq[String]] ={
        //样本的数量
        val sampleCount = 20000
        //每个样本的长度
        val sampleLength = 10
        val samples = scala.collection.mutable.ListBuffer[Seq[String]]()

        //物品出现的总次数
        var itemTotalCount:Long = 0
        for ((k,v) <- itemCount) itemTotalCount += v


        //随机游走sampleCount次，生成sampleCount个序列样本
        for( w <- 1 to sampleCount) {
          samples.append(oneRandomWalk(transferMatrix, itemCount, itemTotalCount, sampleLength))
        }


        Seq(samples.toList : _*)
      }


      //通过随机游走产生一个样本的过程
      //transferMatrix 转移概率矩阵
      //itemCount 物品出现次数的分布
      //itemTotalCount 物品出现总次数
      //sampleLength 每个样本的长度
      def oneRandomWalk(transferMatrix : scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Long]], itemCount : scala.collection.mutable.Map[String, Long], itemTotalCount:Long, sampleLength:Int): Seq[String] ={
        val sample = scala.collection.mutable.ListBuffer[String]()


        //决定起始点
        val randomDouble = Random.nextDouble()
        var firstElement = ""
        var culCount:Long = 0
        //根据物品出现的概率，随机决定起始点
        breakable { for ((item, count) <- itemCount) {
          culCount += count
          if (culCount >= randomDouble * itemTotalCount){
            firstElement = item
            break
          }
        }}


        sample.append(firstElement)
        var curElement = firstElement
        //通过随机游走产生长度为sampleLength的样本
        breakable { for( w <- 1 until sampleLength) {
          if (!itemCount.contains(curElement) || !transferMatrix.contains(curElement)){
            break
          }
          //从curElement到下一个跳的转移概率向量
          val probDistribution = transferMatrix(curElement)
          val curCount = itemCount(curElement)
          val randomDouble = Random.nextDouble()
          var culCount:Long = 0
          //根据转移概率向量随机决定下一跳的物品
          breakable { for ((item, count) <- probDistribution) {
            culCount += count
            if (culCount >= randomDouble * curCount){
              curElement = item
              break
            }
          }}
          sample.append(curElement)
        }}
        Seq(sample.toList : _



        //随机游走采样函数
        //transferMatrix 转移概率矩阵
        //itemCount 物品出现次数的分布
        def randomWalk(transferMatrix : scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Long]], itemCount : scala.collection.mutable.Map[String, Long]): Seq[Seq[String]] ={
          //样本的数量
          val sampleCount = 20000
          //每个样本的长度
          val sampleLength = 10
          val samples = scala.collection.mutable.ListBuffer[Seq[String]]()

          //物品出现的总次数
          var itemTotalCount:Long = 0
          for ((k,v) <- itemCount) itemTotalCount += v


          //随机游走sampleCount次，生成sampleCount个序列样本
          for( w <- 1 to sampleCount) {
            samples.append(oneRandomWalk(transferMatrix, itemCount, itemTotalCount, sampleLength))
          }


          Seq(samples.toList : _*)
        }


        //通过随机游走产生一个样本的过程
        //transferMatrix 转移概率矩阵
        //itemCount 物品出现次数的分布
        //itemTotalCount 物品出现总次数
        //sampleLength 每个样本的长度
        def oneRandomWalk(transferMatrix : scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Long]], itemCount : scala.collection.mutable.Map[String, Long], itemTotalCount:Long, sampleLength:Int): Seq[String] ={
          val sample = scala.collection.mutable.ListBuffer[String]()


          //决定起始点
          val randomDouble = Random.nextDouble()
          var firstElement = ""
          var culCount:Long = 0
          //根据物品出现的概率，随机决定起始点
          breakable { for ((item, count) <- itemCount) {
            culCount += count
            if (culCount >= randomDouble * itemTotalCount){
              firstElement = item
              break
            }
          }}


          sample.append(firstElement)
          var curElement = firstElement
          //通过随机游走产生长度为sampleLength的样本
          breakable { for( w <- 1 until sampleLength) {
            if (!itemCount.contains(curElement) || !transferMatrix.contains(curElement)){
              break
            }
            //从curElement到下一个跳的转移概率向量
            val probDistribution = transferMatrix(curElement)
            val curCount = itemCount(curElement)
            val randomDouble = Random.nextDouble()
            var culCount:Long = 0
            //根据转移概率向量随机决定下一跳的物品
            breakable { for ((item, count) <- probDistribution) {
              culCount += count
              if (culCount >= randomDouble * curCount){
                curElement = item
                break
              }
            }}
            sample.append(curElement)
          }}
          Seq(sample.toList : _
