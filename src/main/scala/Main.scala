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
    sc.setLogLevel("ERROR")

    // Load subscriptions
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile)

    // Filter out malformed subscriptions (None values)
    val subscriptions = subscriptionOpts.flatten

    val subsRDD = sc.parallelize(subscriptions)

    // Download feeds and parse posts, tracking success/failure
    val downloadResults = subsRDD.map { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url, subscription.name)
      val posts = feedOpt.fold(List[Post]())(JsonParser.parsePosts(_, subscription.name, subscription.url))
      (feedOpt.isDefined, posts)
    }

    // Count feed successes/failures
    val feedsSuccess = downloadResults.filter(_._1).count()
    val feedsFailed = downloadResults.count() - feedsSuccess

    // Flatten all posts and count JSON parse failures
    val allPosts = downloadResults.flatMap(_._2)
    val postsSuccess = allPosts.count()
    val postsFailed = downloadResults.filter(_._2.isEmpty).count()

    // Filter empty posts
    val filteredPosts = Analyzer.filterEmptyPosts(allPosts)
    val postsFiltered = (allPosts.count() - filteredPosts.count()).toInt

    // Calculate average characters in filtered posts
    val totalChars = filteredPosts.map(post => post.title.length + post.selftext.length).sum
    val avgChars = (if (filteredPosts.count() > 0) totalChars / filteredPosts.count() else 0).toInt

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> feedsSuccess.toInt,
      "feedsFailed" -> feedsFailed.toInt,
      "postsSuccess" -> postsSuccess.toInt,
      "postsFailed" -> postsFailed.toInt,
      "postsFiltered" -> postsFiltered.toInt,
      "avgChars" -> avgChars
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Check if we have any posts to process
    if (filteredPosts.count() == 0) {
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

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))

    spark.stop()
  }
}
