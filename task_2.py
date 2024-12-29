from import_statements import *

def task_2(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    salesRank_column = 'salesRank'
    categories_column = 'categories'
    asin_column = 'asin'
    # Outputs:
    category_column = 'category'
    bestSalesCategory_column = 'bestSalesCategory'
    bestSalesRank_column = 'bestSalesRank'
    # -------------------------------------------------------------------------
    #make new column 'category' with a[0][0] unless null, then fill in 'null' as the value
    category = product_data.select(F.col("*"), F.when(product_data.categories.isNull() ,None)
                  .when(product_data.categories[0][0] == "",None)
                  .otherwise(product_data.categories[0][0]).alias(category_column))

    #put key in new column 'bestSalesCategory' and value in new column 'bestSalesRank'
    #fill 'null' if original entry was null or empty
    newSales = product_data.select( F.col(asin_column),
    F.map_keys(salesRank_column)[0].alias(bestSalesCategory_column),
    F.map_values(salesRank_column)[0].alias(bestSalesRank_column)
    )
   
    count_total = category[[asin_column]].count()

    mean_bestSalesRank = newSales.select(F.avg(F.col(bestSalesRank_column))).head()[0]

    variance_bestSalesRank = newSales.select(F.variance(F.col(bestSalesRank_column))).head()[0]
    numNulls_category = category.select([F.count(F.when(F.col(category_column).isNull(), category_column))
                                               ]).head()[0]
    countDistinct_category = category.select(F.countDistinct(F.col(category_column))).head()[0]

    numNulls_bestSalesCategory = newSales.select([F.count(F.when(F.col(bestSalesCategory_column).isNull(),
                                                                 bestSalesCategory_column))]).head()[0]
    countDistinct_bestSalesCategory = newSales.select(F.countDistinct(F.col(bestSalesCategory_column))).head()[0]
    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'mean_bestSalesRank': None,
        'variance_bestSalesRank': None,
        'numNulls_category': None,
        'countDistinct_category': None,
        'numNulls_bestSalesCategory': None,
        'countDistinct_bestSalesCategory': None
    }
    # Modify res:
    res['count_total'] = count_total
    res['mean_bestSalesRank'] = mean_bestSalesRank
    res['variance_bestSalesRank'] = variance_bestSalesRank
    res['numNulls_category'] = numNulls_category
    res['countDistinct_category'] = countDistinct_category
    res['numNulls_bestSalesCategory'] = numNulls_bestSalesCategory
    res['countDistinct_bestSalesCategory'] = countDistinct_bestSalesCategory
    # -------------------------------------------------------------------------
    data_io.save(res, 'task_2')
    return res
