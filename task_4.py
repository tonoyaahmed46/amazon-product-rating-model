from import_statements import *

def task_4(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    price_column = 'price'
    title_column = 'title'
    # Outputs:
    meanImputedPrice_column = 'meanImputedPrice'
    medianImputedPrice_column = 'medianImputedPrice'
    unknownImputedTitle_column = 'unknownImputedTitle'
    # -------------------------------------------------------------------------

    #1.in products table: cast every entry of 'price' column to float type.
    product_data = product_data.withColumn(price_column,product_data.price.cast(T.FloatType()))
   
    #2.Impute nulls with mean of all the non-null values. Store in a new column meanImputedPrice.
    #3.Impute with median value. Store the imputed data in a new column medianImputedPrice.
    imputer = M.feature.Imputer(
    inputCols=[price_column],
    outputCols=[medianImputedPrice_column])
    median_imputed = imputer.setStrategy("median").fit(product_data[[price_column]]).transform(product_data[[price_column]])

    imputer2 = M.feature.Imputer(
    inputCols=[price_column],
    outputCols=[meanImputedPrice_column])
    mean_imputed = imputer2.setStrategy("mean").fit(product_data[[price_column]]).transform(product_data[[price_column]])
   
    #4.'title' col: impute nulls/empty strings with string ‘unknown’.Store in new column unknownImputedTitle.
    imputed_title = product_data.select(F.col(title_column)).fillna('unknown')

    count_total = median_imputed[[medianImputedPrice_column]].count()
    mean_meanImputedPrice = mean_imputed.select(F.avg(F.col(meanImputedPrice_column))).head()[0]
    variance_meanImputedPrice = mean_imputed.select(F.variance(F.col(meanImputedPrice_column))).head()[0]
    numNulls_meanImputedPrice = mean_imputed.select([F.count(F.when(F.col(meanImputedPrice_column).isNull(),
                                                                 meanImputedPrice_column))]).head()[0]
    mean_medianImputedPrice = median_imputed.select(F.avg(F.col(medianImputedPrice_column))).head()[0]
    variance_medianImputedPrice = median_imputed.select(F.variance(F.col(medianImputedPrice_column))).head()[0]
    numNulls_medianImputedPrice = median_imputed.select([F.count(F.when(F.col(medianImputedPrice_column).isNull(),
                                                                 medianImputedPrice_column))]).head()[0]
    numUnknowns_unknownImputedTitle = imputed_title.filter(F.col(title_column) == 'unknown').count()
    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'mean_meanImputedPrice': None,
        'variance_meanImputedPrice': None,
        'numNulls_meanImputedPrice': None,
        'mean_medianImputedPrice': None,
        'variance_medianImputedPrice': None,
        'numNulls_medianImputedPrice': None,
        'numUnknowns_unknownImputedTitle': None
    }
    # Modify res:
    res['count_total'] = count_total
    res['mean_meanImputedPrice'] = mean_meanImputedPrice
    res['variance_meanImputedPrice'] = variance_meanImputedPrice
    res['numNulls_meanImputedPrice'] = numNulls_meanImputedPrice
    res['mean_medianImputedPrice'] = mean_medianImputedPrice
    res['variance_medianImputedPrice'] = variance_medianImputedPrice
    res['numNulls_medianImputedPrice'] = numNulls_medianImputedPrice
    res['numUnknowns_unknownImputedTitle'] = numUnknowns_unknownImputedTitle

    # -------------------------------------------------------------------------
    data_io.save(res, 'task_4')
    return res