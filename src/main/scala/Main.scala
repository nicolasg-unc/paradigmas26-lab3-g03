import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {
    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt prints error messages
    }

    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    val feedsSuccessAcc = sc.longAccumulator("feedsSuccess")
    val feedsFailedAcc = sc.longAccumulator("feedsFailed")
    val postsDownloadedAcc = sc.longAccumulator("postsDownloaded")
    val postsDiscardedAcc = sc.longAccumulator("postsDiscarded")
    sc.setLogLevel("ERROR")

    // Load subscriptions
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile)

    // Filter out malformed subscriptions (None values)
    val subscriptions = subscriptionOpts.flatten

    val subsRDD = sc.parallelize(subscriptions)

    val t0 = System.currentTimeMillis()

    // Download feeds and parse posts, tracking success/failure
    val downloadResults = subsRDD.map { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url, subscription.name)

      val posts = feedOpt.fold(List[Post]()) { content =>
        JsonParser.parsePosts(content, subscription.name, subscription.url)
      }

      if (feedOpt.isDefined) feedsSuccessAcc.add(1)
      else feedsFailedAcc.add(1)

      postsDownloadedAcc.add(posts.size)
      (feedOpt.isDefined, posts)
     }

    // Flatten all posts
    val allPosts = downloadResults.flatMap(_._2)

    val t1 = System.currentTimeMillis()

    // Filter empty posts and count how many were discarded
    val filteredPosts = Analyzer.filterEmptyPosts(allPosts, postsDiscardedAcc)
    val filteredCount = filteredPosts.count()

    val t2 = System.currentTimeMillis()

    // Calculate average characters in filtered posts
    val totalChars = filteredPosts.map(post => post.title.length + post.selftext.length).sum
    val avgChars = (if (filteredCount > 0) totalChars / filteredCount else 0).toInt

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> feedsSuccessAcc.value.toInt,
      "feedsFailed" -> feedsFailedAcc.value.toInt,
      "postsSuccess" -> postsDownloadedAcc.value.toInt,
      "postsFiltered" -> postsDiscardedAcc.value.toInt,
      "avgChars" -> avgChars
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Check if we have any posts to process
    if (filteredCount == 0) {
      println("Error: No valid posts downloaded after filtering")
      spark.stop()
      return
    }

    // Load dictionaries
    val dictionaryOpt = Dictionary.loadAll(cmdArgs.entitiesDir)

    if (dictionaryOpt.isEmpty) {
      spark.stop()
      return
    }

    val dictionary = dictionaryOpt.get
    val dictBroadcast = sc.broadcast(dictionary)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = filteredPosts.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictBroadcast.value)
    }

    // Count entities
    val entityCounts = Analyzer.countEntities(allEntities)
    val typeStats = Analyzer.countByType(allEntities)

    val t3 = System.currentTimeMillis()

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))

    println(s"\n============ ACCUMULATORS ============")
    println(s"Feeds exitosos:     ${feedsSuccessAcc.value}")
    println(s"Feeds fallidos:     ${feedsFailedAcc.value}")
    println(s"Posts descargados:  ${postsDownloadedAcc.value}")
    println(s"Posts descartados:  ${postsDiscardedAcc.value}")

    println(s"Tiempo etapa de descarga: ${(t1 - t0) / 1000.0} segundos")
    println(s"Tiempo etapa de filtrado: ${(t2 - t1) / 1000.0} segundos")
    println(s"Tiempo etapa de entidades: ${(t3 - t2) / 1000.0} segundos")
    println(s"Tiempo total del pipeline: ${(t3 - t0) / 1000.0} segundos")

    spark.stop()
  }
}
