from import_statements import *

def task_3(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    asin_column = 'asin'
    price_column = 'price'
    attribute = 'also_viewed'
    related_column = 'related'
    # Outputs:
    meanPriceAlsoViewed_column = 'meanPriceAlsoViewed'
    countAlsoViewed_column = 'countAlsoViewed'
    # -------------------------------------------------------------------------

    prod = product_data
    rel = prod.select("asin", "related", "price")
    exploded = rel.withColumn("also_viewed",rel.related.getItem("also_viewed"))

    prices = prod.select("asin", "price")
    prices_only = prices.withColumnRenamed("price","also_reviewed_price") 

    #meanPriceAlsoViewed
    exploded_expanded = exploded.select("asin", "price" , F.explode_outer("also_viewed"))
    exploded_expand = exploded_expanded.withColumnRenamed("asin","original_asin") \
        .withColumnRenamed("col","exploded_also_reviewed")

    y = exploded_expand.join(prices_only,exploded_expand.exploded_also_reviewed ==  prices_only.asin,"fullouter")
    merged = y.select("original_asin","exploded_also_reviewed", "also_reviewed_price") 

    final = merged.groupBy("original_asin").agg(avg("also_reviewed_price").alias("meanPriceAlsoViewed"))

    #countAlsoViewed
    exploded_expanded2 = exploded.select("asin", "price" , explode("also_viewed"))
    exploded_expand2 = exploded_expanded2.withColumnRenamed("asin","original_asin") \
        .withColumnRenamed("col","exploded_also_reviewed")

    y2 = exploded_expand2.join(prices_only,exploded_expand2.exploded_also_reviewed ==  prices_only.asin,"fullouter")
    merged2 = y2.select("original_asin","exploded_also_reviewed", "also_reviewed_price") 

    final2 = merged2.groupBy("original_asin").agg(count("exploded_also_reviewed").alias("countAlsoViewed"))
    
    #summary stats 
    count_total = prod.count()
    mean_meanPriceAlsoViewed = final.select(F.avg(F.col("meanPriceAlsoViewed"))).head()[0]
    variance_meanPriceAlsoViewed = final.select(F.variance(F.col("meanPriceAlsoViewed"))).head()[0]
    numNulls_meanPriceAlsoViewed = final.select([F.count(F.when(F.col('meanPriceAlsoViewed').isNull(),
                                                                     'meanPriceAlsoViewed'))]).head()[0]
    mean_countAlsoViewed = final2.select(F.avg(F.col("countAlsoViewed"))).head()[0]
    variance_countAlsoViewed = final2.select(F.variance(F.col("countAlsoViewed"))).head()[0]
    numNulls_countAlsoViewed = exploded_expanded.select([F.count(F.when(F.col('col').isNull(),'col'))]).head()[0]
 
    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'mean_meanPriceAlsoViewed': None,
        'variance_meanPriceAlsoViewed': None,
        'numNulls_meanPriceAlsoViewed': None,
        'mean_countAlsoViewed': None,
        'variance_countAlsoViewed': None,
        'numNulls_countAlsoViewed': None
    }
    # Modify res:
    res['count_total'] = count_total
    res['mean_meanPriceAlsoViewed'] = mean_meanPriceAlsoViewed
    res['variance_meanPriceAlsoViewed'] = variance_meanPriceAlsoViewed
    res['numNulls_meanPriceAlsoViewed'] = numNulls_meanPriceAlsoViewed
    res['mean_countAlsoViewed'] = mean_countAlsoViewed
    res['variance_countAlsoViewed'] = variance_countAlsoViewed
    res['numNulls_countAlsoViewed'] = numNulls_countAlsoViewed

    # -------------------------------------------------------------------------
    data_io.save(res, 'task_3')
    return res
    
